import calendar
import os
import pandas as pd
import json
import threading
import plotly.express as px
from datetime import date, timedelta
from pathlib import Path
from shiny import App, ui, reactive, render
from shinywidgets import output_widget, render_widget
import pyarrow.parquet as pq
from runtime_mode import DEMO_MODE

if not DEMO_MODE:
    raise RuntimeError("This public dashboard supports demo mode only. Use APP_MODE=demo.")

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "demo_data"
from demo_data import ensure_demo_data

ensure_demo_data(DATA_DIR)

ORDERS_PARQUET = DATA_DIR / "orders.parquet"
ORDERS_BY_YEAR_DIR = DATA_DIR / "orders_by_year"
ORDER_METADATA_PATH = ORDERS_BY_YEAR_DIR / "_metadata.json"
TRIPS_PARQUET = DATA_DIR / "trips.parquet"
ALLOW_ORDER_PARTITION_BOOTSTRAP = os.getenv("ALLOW_ORDER_PARTITION_BOOTSTRAP", "0") == "1"

ORDER_BASE_COLUMNS = [
    "date",
    "city",
    "total_orders",
    "cancelled_orders",
    "delivered_orders",
    "not_delivered_orders",
    "total_orders_amount",
    "delivered_paid_amount",
    "discount_funded",
    "sum_delivery_fee",
    "sum_driver_earning",
    "service_commission",
    "channel",
    "firstlevel",
    "tiptop_share_delivery_discount",
    "direct_distance",
    "account_manager",
    "coupon_on_delivery_tiptop_share",
    "coupon_on_item_discount_tiptop_share",
    "voided_orders",
    "canceled_orders_by_branch",
]
ORDER_CATEGORY_COLUMNS = ["city", "merchant_city", "channel", "account_manager"]
TAXI_COLUMNS = [
    "request_id",
    "created_date",
    "city",
    "user_id",
    "driver_id",
    "tiptop_commission",
    "price",
    "offer_discount_amount",
    "coupon_discount_amount",
    "status",
    "distance",
    "duration",
]
TAXI_CATEGORY_COLUMNS = ["city", "status"]

_DATA_LOCK = threading.Lock()
_DATA_CACHE = {
    "taxi": {"mtime": None, "df": None, "choices": {}, "error": None},
}
_ORDER_BUILD_LOCK = threading.Lock()
_ORDER_YEAR_CACHE = {}
_ORDER_META_CACHE = {"signature": None, "choices": {}, "latest": None, "years": [], "error": None}


def _average_per_order(df, amount_column, *count_columns):
    """Divide aggregate amounts by order counts, including mixed-outcome rows."""
    count = sum(df[column].sum() for column in count_columns)
    return df[amount_column].sum() / count if count > 0 else 0


def _file_mtime(path: Path):
    try:
        return path.stat().st_mtime_ns
    except FileNotFoundError:
        return None


def _available_parquet_columns(path: Path) -> set[str]:
    return set(pq.ParquetFile(path).schema.names)


def _read_existing_columns(path: Path, desired_columns: list[str]) -> pd.DataFrame:
    available = _available_parquet_columns(path)
    columns = [col for col in desired_columns if col in available]
    return pd.read_parquet(path, columns=columns)


def _downcast_numeric_columns(df: pd.DataFrame) -> None:
    for col in df.select_dtypes(include="number").columns:
        if pd.api.types.is_float_dtype(df[col]):
            df[col] = pd.to_numeric(df[col], downcast="float")
        else:
            df[col] = pd.to_numeric(df[col], downcast="integer")


def _as_categories(df: pd.DataFrame, columns: list[str]) -> None:
    for col in columns:
        if col in df.columns:
            df[col] = df[col].astype("category")


def _normal_date(value):
    return pd.Timestamp(value).normalize()


def _safe_number(value):
    if value is None or pd.isna(value):
        return 0
    return value


def _selectize_choices(df: pd.DataFrame, column: str) -> list[str]:
    if df is None or df.empty or column not in df.columns:
        return ["All"]
    values = df[column].dropna()
    return ["All"] + sorted(map(str, values.unique().tolist()))


def _order_partition_path(year: int) -> Path:
    return ORDERS_BY_YEAR_DIR / f"orders_{year}.parquet"


def _order_partition_year(path: Path) -> int | None:
    try:
        return int(path.stem.rsplit("_", 1)[1])
    except (IndexError, ValueError):
        return None


def _order_partition_paths() -> dict[int, Path]:
    if not ORDERS_BY_YEAR_DIR.exists():
        return {}

    paths = {}
    for path in ORDERS_BY_YEAR_DIR.glob("orders_*.parquet"):
        year = _order_partition_year(path)
        if year is not None:
            paths[year] = path
    return dict(sorted(paths.items()))


def _order_partition_signature():
    return tuple(
        (year, _file_mtime(path), path.stat().st_size)
        for year, path in _order_partition_paths().items()
    )


def _orders_partitions_current() -> bool:
    paths = _order_partition_paths()
    if not paths:
        return False

    monolith_mtime = _file_mtime(ORDERS_PARQUET)
    if monolith_mtime is None:
        return True

    partition_mtimes = [_file_mtime(path) or 0 for path in paths.values()]
    return min(partition_mtimes) >= monolith_mtime


def _empty_orders_df() -> pd.DataFrame:
    columns = ORDER_BASE_COLUMNS + ["merchant_city", "l2_l1", "date_without_time"]
    return pd.DataFrame(columns=list(dict.fromkeys(columns)))


def _write_order_metadata(df: pd.DataFrame, years: list[int]) -> None:
    latest = df["date"].max() if "date" in df.columns and not df.empty else None
    metadata = {
        "years": years,
        "latest": latest.isoformat() if latest is not None and pd.notna(latest) else None,
        "choices": {
            "city": _selectize_choices(df, "city"),
            "merchant": _selectize_choices(df, "merchant_city"),
            "channel": _selectize_choices(df, "channel"),
            "account_manager": _selectize_choices(df, "account_manager"),
        },
    }

    ORDER_METADATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = ORDER_METADATA_PATH.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(metadata), encoding="utf-8")
    tmp_path.replace(ORDER_METADATA_PATH)


def build_order_year_partitions(force: bool = False):
    if not force and _orders_partitions_current():
        return None

    with _ORDER_BUILD_LOCK:
        if not force and _orders_partitions_current():
            return None

        if not ALLOW_ORDER_PARTITION_BOOTSTRAP:
            if _order_partition_paths():
                return None
            return FileNotFoundError(
                "orders_by_year/ is missing. Create it with "
                "`python sync_orders.py --bootstrap-from-monolith` on a machine with enough memory, "
                "then upload the folder to the server."
            )

        if not ORDERS_PARQUET.exists():
            return FileNotFoundError(f"{ORDERS_PARQUET.name} was not found")

        df = _prepare_orders_df(ORDERS_PARQUET)
        if df.empty:
            _write_order_metadata(df, [])
            return None

        ORDERS_BY_YEAR_DIR.mkdir(parents=True, exist_ok=True)
        years = []
        for year, year_df in df.groupby(df["date"].dt.year, observed=True):
            if pd.isna(year):
                continue

            year = int(year)
            years.append(year)
            partition_path = _order_partition_path(year)
            tmp_path = partition_path.with_name(f"{partition_path.name}.tmp")
            year_df.to_parquet(tmp_path, engine="pyarrow", compression="snappy", index=False)
            tmp_path.replace(partition_path)

        _write_order_metadata(df, sorted(years))

    with _DATA_LOCK:
        _ORDER_YEAR_CACHE.clear()
        _ORDER_META_CACHE.update({"signature": None, "choices": {}, "latest": None, "years": [], "error": None})

    return None


def _read_order_metadata():
    try:
        if not ORDER_METADATA_PATH.exists():
            return None
        return json.loads(ORDER_METADATA_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None


def load_order_choices(force: bool = False):
    error = build_order_year_partitions(force=False)
    if error is not None:
        return {}, None, [], error

    signature = _order_partition_signature()
    with _DATA_LOCK:
        if not force and _ORDER_META_CACHE["signature"] == signature:
            return (
                _ORDER_META_CACHE["choices"],
                _ORDER_META_CACHE["latest"],
                _ORDER_META_CACHE["years"],
                _ORDER_META_CACHE["error"],
            )

    metadata = _read_order_metadata()
    if metadata is not None:
        choices = metadata.get("choices", {})
        latest = metadata.get("latest")
        years = metadata.get("years", [])
        latest = pd.Timestamp(latest) if latest else None
        error = None
    else:
        frames = []
        latest_values = []
        for path in _order_partition_paths().values():
            available = _available_parquet_columns(path)
            choice_columns = [col for col in ORDER_CATEGORY_COLUMNS if col in available]
            if "date" in available:
                choice_columns.append("date")
            if not choice_columns:
                continue

            frame = pd.read_parquet(path, columns=choice_columns)
            if "date" in frame.columns:
                latest_values.append(pd.to_datetime(frame["date"], errors="coerce").max())
                frame = frame.drop(columns=["date"])
            frames.append(frame)

        choices_df = pd.concat(frames, ignore_index=True) if frames else _empty_orders_df()
        choices = {
            "city": _selectize_choices(choices_df, "city"),
            "merchant": _selectize_choices(choices_df, "merchant_city"),
            "channel": _selectize_choices(choices_df, "channel"),
            "account_manager": _selectize_choices(choices_df, "account_manager"),
        }
        latest = max(latest_values) if latest_values else None
        years = sorted(_order_partition_paths().keys())
        error = None

    with _DATA_LOCK:
        _ORDER_META_CACHE.update(
            {"signature": signature, "choices": choices, "latest": latest, "years": years, "error": error}
        )

    return choices, latest, years, error


def _years_for_date_ranges(*ranges) -> list[int]:
    years = set()
    for start, end in ranges:
        if start is None or end is None:
            continue

        start = _normal_date(start)
        end = _normal_date(end)
        if pd.isna(start) or pd.isna(end):
            continue
        if start > end:
            start, end = end, start

        years.update(range(start.year, end.year + 1))

    return sorted(years) or [date.today().year]


def load_orders(years: list[int] | None = None, force: bool = False):
    error = build_order_year_partitions(force=False)
    if error is not None:
        return _empty_orders_df(), {}, error

    choices, _, available_years, metadata_error = load_order_choices(force=force)
    if metadata_error is not None:
        return _empty_orders_df(), choices, metadata_error

    if years is None:
        years = [max(available_years)] if available_years else [date.today().year]

    frames = []
    errors = []
    for year in sorted(set(years)):
        path = _order_partition_path(year)
        if not path.exists():
            continue

        mtime = _file_mtime(path)
        with _DATA_LOCK:
            cached = _ORDER_YEAR_CACHE.get(year)
            if not force and cached is not None and cached["mtime"] == mtime:
                if cached["error"] is None and cached["df"] is not None:
                    frames.append(cached["df"])
                elif cached["error"] is not None:
                    errors.append(cached["error"])
                continue

        try:
            df = _prepare_orders_df(path)
            load_error = None
        except Exception as exc:
            df = None
            load_error = exc

        with _DATA_LOCK:
            _ORDER_YEAR_CACHE[year] = {"mtime": mtime, "df": df, "error": load_error}

        if load_error is None and df is not None:
            frames.append(df)
        elif load_error is not None:
            errors.append(load_error)

    if not frames:
        return _empty_orders_df(), choices, errors[0] if errors else None

    df = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
    return df, choices, errors[0] if errors else None


def _prepare_orders_df(path: Path) -> pd.DataFrame:
    available = _available_parquet_columns(path)
    desired_columns = ORDER_BASE_COLUMNS.copy()

    if "merchant_city" in available:
        desired_columns.append("merchant_city")
    elif "name" in available:
        desired_columns.append("name")

    if "l2_l1" in available:
        desired_columns.append("l2_l1")
    elif "secondlevel" in available:
        desired_columns.append("secondlevel")

    df = _read_existing_columns(path, desired_columns)
    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["date_without_time"] = df["date"].dt.normalize()

    if "merchant_city" not in df.columns:
        if "name" in df.columns and "city" in df.columns:
            df["merchant_city"] = df["name"].fillna("").astype(str) + " - " + df["city"].fillna("").astype(str)
            df = df.drop(columns=["name"])
        elif "city" in df.columns:
            df["merchant_city"] = df["city"]

    if "l2_l1" not in df.columns:
        if "firstlevel" in df.columns and "secondlevel" in df.columns:
            df["l2_l1"] = df["firstlevel"].fillna(0) - df["secondlevel"].fillna(0)
        else:
            df["l2_l1"] = 0

    if "account_manager" not in df.columns:
        df["account_manager"] = "Unknown"

    _downcast_numeric_columns(df)
    _as_categories(df, ORDER_CATEGORY_COLUMNS)
    return df


def _prepare_taxi_df(path: Path) -> pd.DataFrame:
    df = _read_existing_columns(path, TAXI_COLUMNS)
    if df.empty:
        return df

    df["created_date"] = pd.to_datetime(df["created_date"], errors="coerce")
    df["date_without_time"] = df["created_date"].dt.normalize()
    _downcast_numeric_columns(df)
    _as_categories(df, TAXI_CATEGORY_COLUMNS)
    return df


def _load_cached(kind: str, path: Path, prepare_func, force: bool = False):
    mtime = _file_mtime(path)
    with _DATA_LOCK:
        cached = _DATA_CACHE[kind]
        if not force and cached["df"] is not None and cached["mtime"] == mtime:
            return cached["df"], cached["choices"], cached["error"]

    try:
        df = prepare_func(path)
        choices = {"city": _selectize_choices(df, "city")}
        error = None
    except Exception as exc:
        df = None
        choices = {}
        error = exc

    with _DATA_LOCK:
        _DATA_CACHE[kind] = {"mtime": mtime, "df": df, "choices": choices, "error": error}
    return df, choices, error


def load_taxi(force: bool = False):
    return _load_cached("taxi", TRIPS_PARQUET, _prepare_taxi_df, force)

def normalize_selectize_selection(value) -> list[str]:
    if not value:
        return []
    if isinstance(value, str):
        value = [value]
    return [item for item in value if item != "All"]


def normalize_selectize_value(value) -> list[str]:
    if not value:
        return []
    if isinstance(value, str):
        value = [value]
    return [str(item) for item in value]


def get_yesterday():
    return date.today() - timedelta(days=1)

def get_day_before():
    return date.today() - timedelta(days=2)

DATE_PRESET_CHOICES = {
    "today": "Today",
    "yesterday": "Yesterday",
    "month_to_date": "Month to Date",
    "custom": "Custom Range",
}

def shift_months(source_date, months):
    month_index = source_date.month - 1 + months
    year = source_date.year + month_index // 12
    month = month_index % 12 + 1
    last_day = calendar.monthrange(year, month)[1]
    day = min(source_date.day, last_day)
    return date(year, month, day)

def resolve_preset_range(preset_key):
    today = date.today()
    yesterday = today - timedelta(days=1)
    day_before = today - timedelta(days=2)
    last_week_same_day = today - timedelta(days=7)
    last_month_same_day = shift_months(today, -1)
    last_month_today = shift_months(today, -1)
    last_month_start = last_month_today.replace(day=1)

    if preset_key == "today":
        return today, today
    if preset_key == "day_before_yesterday":
        return day_before, day_before
    if preset_key == "last_week_same_day":
        return last_week_same_day, last_week_same_day
    if preset_key == "last_month_same_day":
        return last_month_same_day, last_month_same_day
    if preset_key == "last_month_to_date":
        return last_month_start, last_month_today
    if preset_key == "month_to_date":
        return today.replace(day=1), today

    return yesterday, yesterday

def get_default_date_ranges():
    current_start, current_end = resolve_preset_range("yesterday")
    previous_start, previous_end = resolve_preset_range("day_before_yesterday")
    return {
        "date_range": (current_start, current_end),
        "prev_date_range": (previous_start, previous_end),
        "taxi_date_range": (current_start, current_end),
        "taxi_prev_date_range": (previous_start, previous_end),
    }

CURRENT_PRESET_CHOICES = DATE_PRESET_CHOICES
PREVIOUS_PRESET_CHOICES = {
    "yesterday": "Yesterday",
    "day_before_yesterday": "Day Before Yesterday",
    "last_week_same_day": "Last Week Same Day",
    "last_month_same_day": "Last Month Same Day",
    "last_month_to_date": "Last Month to Date",
    "custom": "Custom Range",
}

# ---------------------
# UI
# ---------------------

app_ui = ui.page_navbar(
    ui.nav_panel("Delivery Dashboard", 
        ui.page_sidebar(
            ui.sidebar(
                ui.input_select("date_preset", "Current Preset", CURRENT_PRESET_CHOICES, selected="yesterday"),
                ui.input_date_range("date_range", "Current Period", 
                                    start=get_yesterday(), end=get_yesterday()),
                ui.input_select("prev_date_preset", "Previous Preset", PREVIOUS_PRESET_CHOICES, selected="day_before_yesterday"),
                ui.input_date_range("prev_date_range", "Previous Period", 
                                    start=get_day_before(), end=get_day_before()),
                ui.hr(),
                ui.h6("Filters"),
                ui.input_selectize("filter_city", "City", choices=["All"], selected="All", multiple=True),
                ui.input_selectize("filter_merchant", "Merchant", choices=["All"], selected="All", multiple=True),
                ui.input_selectize("filter_channel", "Vertical", choices=["All"], selected="All", multiple=True),
                ui.input_selectize("filter_account_manager", "Account Manager", choices=["All"], selected="All", multiple=True),
                ui.hr(),
                ui.output_text("last_update"),
                title="Controls", width=350, open="desktop"
            ),

            ui.tags.head(
                ui.tags.style("""
                    .bslib-value-box { 
                        background-color: white !important; 
                        color: #2c3e50 !important;
                        border-radius: 12px !important;
                        box-shadow: 0 4px 12px rgba(0,0,0,0.05) !important;
                        border: 1px solid #eef0f2 !important;
                        min-height: 100px !important;
                    }
                    .bslib-grid { gap: 15px !important; margin-bottom: 25px !important; }
                    .bslib-value-box .value-box-title { 
                        font-size: 0.75rem !important; 
                        text-transform: uppercase; 
                        color: #6c757d !important; 
                        font-weight: 600; 
                    }
                    body { background-color: #f8f9fa; }
                    h4 { color: #2c3e50; font-weight: 700; margin: 30px 0 15px 5px; border-left: 5px solid #007bff; padding-left: 12px; font-size: 1.1rem; }
                    .date-header { font-size: 1.0rem; color: #6c757d; font-weight: 500; margin-bottom: 20px; }
                """)
            ),

            ui.h2("Order Performance Dashboard", class_="ps-2 pt-2"),
            ui.div(ui.output_text("date_range_text"), class_="ps-2 date-header"),

            ui.h4("Operations Volume"),
            ui.layout_columns(
                ui.value_box("All Orders", ui.output_ui("all_orders_compared")),
                ui.value_box("Delivered", ui.output_ui("delivered_compared")),
                ui.value_box("Canceled", ui.output_ui("canceled_compared")),
                ui.value_box("Merchant Canceled", ui.output_ui("merchant_canceled_compared")),
                ui.value_box("Not Delivered", ui.output_ui("not_delivered_compared")),
                ui.value_box("Voided", ui.output_ui("voided_compared")),
                col_widths=[2,2,2, 2, 2, 2]
            ),

            ui.h4("Quality & Performance"),
            ui.layout_columns(
                ui.value_box("Delivered Rate %", ui.output_ui("delivered_prct_compared")),
                ui.value_box("Cancellation Rate %", ui.output_ui("cancelation_prct_compared")),
                ui.value_box("Merchant Cancellation Rate %", ui.output_ui("merchant_cancellation_prct_compared")),
                ui.value_box("Rejection Rate %", ui.output_ui("rejection_prct_compared")),
                ui.value_box("Voided Rate %", ui.output_ui("voided_prct_compared")),
                col_widths=[4, 2, 2, 2, 2]
            ),

            ui.h4("Revenue & Growth"),
            ui.layout_columns(
                ui.value_box("GMV", ui.output_ui("gmv_compared")),
                ui.value_box("NMV", ui.output_ui("nmv_compared")),
                ui.value_box("Total Order Revenue", ui.output_ui("total_order_revenue")),
                ui.value_box("Delivery Fee", ui.output_ui("delivery_fee_compared")),
                col_widths=[3, 3, 3, 3]
            ),

            ui.h4("Logistics Costs"),
            ui.layout_columns(
                ui.value_box("Total Order Cost", ui.output_ui("total_order_cost")),
                ui.value_box("Discount Funded", ui.output_ui("discount_funded_compared")),
                ui.value_box("Delivery Share", ui.output_ui("service_delivery_share_compared")),
                col_widths=[4, 4, 4]
            ),

            ui.h4("Unit Economics"),
            ui.layout_columns(
                ui.value_box("AVG Discount Funded", ui.output_ui("avg_discount_funded_compared")),
                ui.value_box("Avg Delivery Revenue", ui.output_ui("avg_delivery_revenue_compared")),
                ui.value_box("CPO", ui.output_ui("cpo_compared")),
                 ui.value_box("RPO", ui.output_ui("rpo_compared")),
                ui.value_box("AOV", ui.output_ui("aov_compared")),
                ui.value_box("Avg KM", ui.output_ui("avg_km_compared")),
                col_widths=[2,2,2,2,2,2]
            ),

            ui.h4('L1 & L2 Trend'),
            ui.layout_columns(
                ui.value_box("L1 Rate", ui.output_ui("l1_compared")),
                ui.value_box("L2 Rate", ui.output_ui("l2_compared")),
                col_widths=[6,6]
            ),

            ui.card(
                ui.card_header("Delivered Orders Trend (Hourly)"),
                output_widget("orders_plot"),
                full_screen=True
            ),
            ui.layout_columns(
                ui.card(ui.card_header("City Breakdown (Current)"), ui.output_data_frame("city")),
                ui.card(ui.card_header("City Breakdown (Previous)"), ui.output_data_frame("city_prev")),
                col_widths=[6, 6]
            ),

            ui.layout_columns(
            ui.card(
                ui.card_header("Current Period"), 
                ui.output_data_frame("merchant")
            ),
            ui.card(
                ui.card_header("Previous Period"), 
                ui.output_data_frame("merchant_prev")
            ),
            col_widths=[6, 6]))
        ),
    ui.nav_panel("Taxi Trips", 
    ui.page_sidebar(
            ui.sidebar(
                ui.input_select("taxi_date_preset", "Current Preset", CURRENT_PRESET_CHOICES, selected="yesterday"),
                ui.input_date_range("taxi_date_range", "Current Period", 
                                    start=get_yesterday(), end=get_yesterday()),
                ui.input_select("taxi_prev_date_preset", "Previous Preset", PREVIOUS_PRESET_CHOICES, selected="day_before_yesterday"),
                ui.input_date_range("taxi_prev_date_range", "Previous Period", 
                                    start=get_day_before(), end=get_day_before()),
                ui.hr(),
                ui.h6("Filters"),
                ui.input_selectize("taxi_filter_city", "City", choices=["All"], selected="All", multiple=True),
                ui.hr(),
                ui.output_text("taxi_last_update"),
                title="Controls", width=350, open="desktop"
            ),
            ui.tags.head(
                    ui.tags.style("""
                        .bslib-value-box { 
                            background-color: white !important; 
                            color: #2c3e50 !important;
                            border-radius: 12px !important;
                            box-shadow: 0 4px 12px rgba(0,0,0,0.05) !important;
                            border: 1px solid #eef0f2 !important;
                            min-height: 100px !important;
                        }
                        .bslib-grid { gap: 15px !important; margin-bottom: 25px !important; }
                        .bslib-value-box .value-box-title { 
                            font-size: 0.75rem !important; 
                            text-transform: uppercase; 
                            color: #6c757d !important; 
                            font-weight: 600; 
                        }
                        body { background-color: #f8f9fa; }
                        h4 { color: #2c3e50; font-weight: 700; margin: 30px 0 15px 5px; border-left: 5px solid #007bff; padding-left: 12px; font-size: 1.1rem; }
                        .date-header { font-size: 1.0rem; color: #6c757d; font-weight: 500; margin-bottom: 20px; }
                    """)
                ),
            ui.h2("Taxi Trips Performance Dashboard", class_="ps-2 pt-2"),
            ui.div(ui.output_text("taxi_date_range_text"), class_="ps-2 date-header"),

            ui.h4("Operations Volume"),
            ui.layout_columns(
                ui.value_box("All Requests", ui.output_ui("all_trips_compared")),
                ui.value_box("Successful Trips", ui.output_ui("trips_success_compared")),
                ui.value_box("Canceled Trips", ui.output_ui("trips_canceled_compared")),
                ui.value_box("Timed Out", ui.output_ui("trips_timed_out_compared")),
                col_widths=[3, 3, 3, 3]
            ),

            ui.h4("Quality & Performance"),
            ui.layout_columns(
                ui.value_box("Success Rate %", ui.output_ui("trips_success_pct_compared")),
                ui.value_box("Cancellation Rate %", ui.output_ui("trips_cancelation_pct_compared")),
                ui.value_box("Timeout Rate %", ui.output_ui("trips_timeout_pct_compared")),
                col_widths=[4, 4, 4]
            ),

            ui.h4("Revenue & Growth"),
            ui.layout_columns(
                ui.value_box("GMV", ui.output_ui("taxi_gmv_compared")),
                ui.value_box("NMV", ui.output_ui("taxi_nmv_compared")),
                ui.value_box("Commission", ui.output_ui("taxi_commission_compared")),
                ui.value_box("Discount Funded", ui.output_ui("taxi_discount_funded_compared")),
                col_widths=[3, 3, 3, 3]
            ),  

            ui.h4("Unit Economics"),
            ui.layout_columns(
                ui.value_box("Avg Trip Price", ui.output_ui("taxi_avg_trip_price_compared")),
                ui.value_box("Avg Commission", ui.output_ui("taxi_avg_commission_compared")),
                ui.value_box("AVG Discount Funded", ui.output_ui("taxi_avg_discount_funded_compared")),
                col_widths=[6, 3, 3]
            ),

            ui.h4("Logistics"),
            ui.layout_columns(
                ui.value_box("Avg Trip Distance (KM)", ui.output_ui("taxi_avg_trip_km_compared")),
                ui.value_box("Avg Trip Duration (Minutes)", ui.output_ui("taxi_avg_trip_duration_compared")),
                ui.value_box("Drivers", ui.output_ui("taxi_unique_drivers_compared")),
                ui.value_box("Trips Per Driver", ui.output_ui("taxi_trip_per_drivers_compared")),
                col_widths=[3,3,3,3]
            ),

            ui.h4("Passengers"),
            ui.layout_columns(
                ui.value_box("Passengers", ui.output_ui("taxi_unique_users_compared")),
                ui.value_box("Trips Per Passenger", ui.output_ui("taxi_trip_per_users_compared")),
                ui.value_box("Unique Passengers Cancelation", ui.output_ui("taxi_unique_users_cancelation_compared")),
                col_widths=[4,4,4]
            ),
    ),),
    header=ui.TagList(
        ui.div("DEMO · All data is synthetic · Live sync disabled", class_="alert alert-info m-3") if DEMO_MODE else None,
        ui.tags.head(
        ui.tags.style("""
            .bslib-value-box { 
                background-color: white !important; 
                color: #2c3e50 !important;
                border-radius: 12px !important;
                box-shadow: 0 4px 12px rgba(0,0,0,0.05) !important;
            }
            /* Rest of your CSS... */
        """)
    ),

    ),
    title="Main Operations Hub",
    id="main_tabs"
)

# Taxi is small enough to warm once; order history is loaded lazily by year.
load_taxi()

# ---------------------
# SERVER
# ---------------------
def server(input, output, session):
    taxi_store = reactive.Value(None)
    order_filter_state = {}

    def city_filter_selection(city_choices: list[str]) -> list[str] | str:
        visible_cities = [choice for choice in city_choices if choice != "All"]
        if len(visible_cities) == 1:
            return visible_cities
        return "All"

    def initialize_date_defaults():
        for input_id, (start, end) in get_default_date_ranges().items():
            ui.update_date_range(input_id, start=start, end=end)

    def apply_preset_to_input(input_id, preset_key):
        start_date, end_date = resolve_preset_range(preset_key)
        ui.update_date_range(input_id, start=start_date, end=end_date)

    @reactive.effect
    def _initial_load():
        initialize_date_defaults()
        choices, _, _, error = load_order_choices()
        if error is None:
            city_choices = choices.get("city", ["All"])
            ui.update_selectize(
                "filter_city",
                choices=city_choices,
                selected=city_filter_selection(city_choices),
                server=True,
            )
            ui.update_selectize("filter_merchant", choices=choices.get("merchant", ["All"]), selected="All", server=True)
            ui.update_selectize("filter_channel", choices=choices.get("channel", ["All"]), selected="All", server=True)
            ui.update_selectize(
                "filter_account_manager",
                choices=choices.get("account_manager", ["All"]),
                selected="All",
                server=True,
            )
        else:
            ui.notification_show(f"Could not load delivery data: {error}", type="error", duration=10)

        df_taxi, taxi_choices, taxi_error = load_taxi()
        if taxi_error is None and df_taxi is not None:
            taxi_store.set(df_taxi)
            city_choices = taxi_choices.get("city", ["All"])
            ui.update_selectize(
                "taxi_filter_city",
                choices=city_choices,
                selected=city_filter_selection(city_choices),
                server=True,
            )
        else:
            ui.notification_show(f"Could not load taxi data: {taxi_error}", type="error", duration=10)

    @reactive.effect
    @reactive.event(input.date_preset)
    def _sync_delivery_preset():
        preset_key = input.date_preset() or "yesterday"
        if preset_key == "custom":
            return
        apply_preset_to_input("date_range", preset_key)

    @reactive.effect
    @reactive.event(input.prev_date_preset)
    def _sync_delivery_previous_preset():
        preset_key = input.prev_date_preset() or "day_before_yesterday"
        if preset_key == "custom":
            return
        apply_preset_to_input("prev_date_range", preset_key)

    @reactive.effect
    @reactive.event(input.taxi_date_preset)
    def _sync_taxi_preset():
        preset_key = input.taxi_date_preset() or "yesterday"
        if preset_key == "custom":
            return
        apply_preset_to_input("taxi_date_range", preset_key)

    @reactive.effect
    @reactive.event(input.taxi_prev_date_preset)
    def _sync_taxi_previous_preset():
        preset_key = input.taxi_prev_date_preset() or "day_before_yesterday"
        if preset_key == "custom":
            return
        apply_preset_to_input("taxi_prev_date_range", preset_key)

    @reactive.calc
    def orders_df():
        selected_years = _years_for_date_ranges(input.date_range(), input.prev_date_range())
        df, _, error = load_orders(selected_years)
        if error is not None:
            ui.notification_show(f"Could not load delivery data: {error}", type="error", duration=10)
        return df

    @reactive.calc
    def orders_city_scoped_df():
        df = orders_df()
        if df is None or df.empty:
            return pd.DataFrame()

        city_values = normalize_selectize_selection(input.filter_city())
        if city_values and "city" in df.columns:
            return df[df["city"].isin(city_values)].copy()

        return df

    @reactive.effect
    def _sync_dependent_order_filters():
        nonlocal order_filter_state
        df = orders_city_scoped_df()

        merchant_choices = _selectize_choices(df, "merchant_city")
        channel_choices = _selectize_choices(df, "channel")
        account_manager_choices = _selectize_choices(df, "account_manager")

        with reactive.isolate():
            current_merchants = normalize_selectize_selection(input.filter_merchant())
            current_channels = normalize_selectize_selection(input.filter_channel())
            current_account_managers = normalize_selectize_selection(
                input.filter_account_manager()
            )

        selected_merchants = [value for value in current_merchants if value in merchant_choices]
        selected_channels = [value for value in current_channels if value in channel_choices]
        selected_account_managers = [
            value for value in current_account_managers if value in account_manager_choices
        ]

        next_state = {
            "filter_merchant": {
                "choices": merchant_choices,
                "selected": normalize_selectize_value(selected_merchants or "All"),
            },
            "filter_channel": {
                "choices": channel_choices,
                "selected": normalize_selectize_value(selected_channels or "All"),
            },
            "filter_account_manager": {
                "choices": account_manager_choices,
                "selected": normalize_selectize_value(selected_account_managers or "All"),
            },
        }
        previous_state = order_filter_state

        if previous_state.get("filter_merchant") != next_state["filter_merchant"]:
            ui.update_selectize(
                "filter_merchant",
                choices=merchant_choices,
                selected=selected_merchants or "All",
                server=True,
            )
        if previous_state.get("filter_channel") != next_state["filter_channel"]:
            ui.update_selectize(
                "filter_channel",
                choices=channel_choices,
                selected=selected_channels or "All",
                server=True,
            )
        if previous_state.get("filter_account_manager") != next_state["filter_account_manager"]:
            ui.update_selectize(
                "filter_account_manager",
                choices=account_manager_choices,
                selected=selected_account_managers or "All",
                server=True,
            )

        order_filter_state = next_state

    @reactive.calc
    def filtered_dfs():
        df = orders_df()
        if df is None or df.empty: return pd.DataFrame(), pd.DataFrame()
        mask = pd.Series(True, index=df.index)
        for col, val in [
            ("city", input.filter_city()),
            ("merchant_city", input.filter_merchant()),
            ("channel", input.filter_channel()),
            ("account_manager", input.filter_account_manager()),
        ]:
            val = normalize_selectize_selection(val)
            if val and col in df.columns:
                mask &= df[col].isin(val)
        s, e = input.date_range()
        ps, pe = input.prev_date_range()
        s, e, ps, pe = map(_normal_date, [s, e, ps, pe])
        dates = df["date_without_time"]
        curr = df[mask & dates.between(s, e)]
        prev = df[mask & dates.between(ps, pe)]
        return curr, prev

    @reactive.calc
    def taxi_df(): return taxi_store.get()

    @render.text
    def last_update():
        _, latest, _, error = load_order_choices()
        if error is not None:
            return "No data"
        return f"Last update: {latest:%B %d, %H:%M}" if latest is not None else "No data"
    
    @render.text
    def taxi_last_update():
        df =taxi_df()
        return f"Last update: {df['created_date'].max():%B %d, %H:%M}" if df is not None and not df.empty else "No data"
    
    def humanize(v):
        if pd.isna(v) or v is None: return "0"
        if abs(v) >= 1_000_000: return f"{v/1_000_000:.1f}M"
        if abs(v) >= 10_000: return f"{v/1_000:.1f}K"
        return f"{v:,.0f}"

    def get_comp_text(curr_val, prev_val, is_pct=False, is_decimal=False, is_rev=False):
        curr_val = _safe_number(curr_val)
        prev_val = _safe_number(prev_val)
        # Format strings for the raw values
        if is_pct: c_str, p_str = f"{curr_val:.1f}%", f"{prev_val:.1f}%"
        elif is_decimal: c_str, p_str = f"{curr_val:,.1f}", f"{prev_val:,.1f}"
        else: c_str, p_str = humanize(curr_val), humanize(prev_val)
        
        # Calculate Delta and Arrow
        delta = curr_val - prev_val
        arrow = "↑" if delta >= 0 else "↓"
        if is_rev:
            trend_color = "#dc3545" if delta >= 0 else "#28a745" # Green if up, Red if down
        else:
            trend_color = "#28a745" if delta >= 0 else "#dc3545" # Green if up, Red if down

        # Calculate percentage change for non-percentage metrics
        if not is_pct:
            pct_change = (abs(delta) / prev_val * 100) if prev_val != 0 else 0
            trend_str = f" {arrow}{pct_change:.1f}%"
        else:
            trend_str = f" {arrow}{abs(delta):.1f}pp" # Percentage Point difference

        return ui.TagList(
            ui.tags.span(c_str, style="color: #2c3e50; font-weight: 800; font-size: 1.8rem;"),
            ui.tags.span(trend_str, style=f"color: {trend_color}; font-size: 0.9rem; font-weight: 700; margin-left: 5px;"),
            ui.tags.span(f" vs {p_str}", style="color: #3498db; font-size: 1.2rem; font-weight: 800; margin-left: 8px;")
        )

    @render.text
    def date_range_text():
        s, e = input.date_range()
        ps, pe = input.prev_date_range()
        return f"Current Period: {s} to {e} | Previous: {ps} to {pe}"

    @render.text
    def taxi_date_range_text():
        s, e = input.taxi_date_range()
        ps, pe = input.taxi_prev_date_range()
        return f"Current Period: {s} to {e} | Previous: {ps} to {pe}"

    # Delivery----------------------------------------------------------------------------------------------
    # --- METRICS RENDER UI ---
    @render.ui
    def all_orders_compared():
        c, p = filtered_dfs()
        return get_comp_text(c["total_orders"].sum(), p["total_orders"].sum())

    @render.ui
    def delivered_compared():
        c, p = filtered_dfs()
        return get_comp_text(c["delivered_orders"].sum(), p["delivered_orders"].sum())

    @render.ui
    def canceled_compared():
        c, p = filtered_dfs()
        return get_comp_text(c["cancelled_orders"].sum(), p["cancelled_orders"].sum(), is_rev=True)

    @render.ui
    def merchant_canceled_compared():
        c, p = filtered_dfs()
        return get_comp_text(c["canceled_orders_by_branch"].sum(), p["canceled_orders_by_branch"].sum(), is_rev=True)

    @render.ui
    def not_delivered_compared():
        c, p = filtered_dfs()
        return get_comp_text(c["not_delivered_orders"].sum(), p["not_delivered_orders"].sum(), is_rev=True)

    @render.ui
    def voided_compared():
        c, p = filtered_dfs()
        return get_comp_text(c["voided_orders"].sum(), p["voided_orders"].sum(), is_rev=True)

    @render.ui
    def delivered_prct_compared():
        c, p = filtered_dfs()
        c_v = (c["delivered_orders"].sum() / c["total_orders"].sum() * 100) if c["total_orders"].sum() > 0 else 0
        p_v = (p["delivered_orders"].sum() / p["total_orders"].sum() * 100) if p["total_orders"].sum() > 0 else 0
        return get_comp_text(c_v, p_v, is_pct=True)

    @render.ui
    def cancelation_prct_compared():
        c, p = filtered_dfs()
        c_v = (c["cancelled_orders"].sum() / c["total_orders"].sum() * 100) if c["total_orders"].sum() > 0 else 0
        p_v = (p["cancelled_orders"].sum() / p["total_orders"].sum() * 100) if p["total_orders"].sum() > 0 else 0
        return get_comp_text(c_v, p_v, is_pct=True, is_rev=True)

    @render.ui
    def merchant_cancellation_prct_compared():
        c, p = filtered_dfs()
        c_v = (c["canceled_orders_by_branch"].sum() / c["total_orders"].sum() * 100) if c["total_orders"].sum() > 0 else 0
        p_v = (p["canceled_orders_by_branch"].sum() / p["total_orders"].sum() * 100) if p["total_orders"].sum() > 0 else 0
        return get_comp_text(c_v, p_v, is_pct=True, is_rev=True)

    @render.ui
    def rejection_prct_compared():
        c, p = filtered_dfs()
        c_v = (c["not_delivered_orders"].sum() / c["total_orders"].sum() * 100) if c["total_orders"].sum() > 0 else 0
        p_v = (p["not_delivered_orders"].sum() / p["total_orders"].sum() * 100) if p["total_orders"].sum() > 0 else 0
        return get_comp_text(c_v, p_v, is_pct=True, is_rev=True)

    @render.ui
    def voided_prct_compared():
        c, p = filtered_dfs()
        c_v = (c["voided_orders"].sum() / c["total_orders"].sum() * 100) if c["total_orders"].sum() > 0 else 0
        p_v = (p["voided_orders"].sum() / p["total_orders"].sum() * 100) if p["total_orders"].sum() > 0 else 0
        return get_comp_text(c_v, p_v, is_pct=True, is_rev=True)

    @render.ui
    def gmv_compared():
        c, p = filtered_dfs()
        return get_comp_text(c["total_orders_amount"].sum(), p["total_orders_amount"].sum())

    @render.ui
    def nmv_compared():
        c, p = filtered_dfs()
        return get_comp_text(c["delivered_paid_amount"].sum(), p["delivered_paid_amount"].sum())

    @render.ui
    def discount_funded_compared():
        c, p = filtered_dfs()
        return get_comp_text(c["discount_funded"].sum(), p["discount_funded"].sum(), is_rev=True)

    @render.ui
    def avg_discount_funded_compared():
        c, p = filtered_dfs()
        return get_comp_text(
            _average_per_order(c, "discount_funded", "delivered_orders", "not_delivered_orders"),
            _average_per_order(p, "discount_funded", "delivered_orders", "not_delivered_orders"),
                             is_rev=True, is_decimal=True)

    @render.ui
    def aov_compared():
        c, p = filtered_dfs()
        return get_comp_text(
            _average_per_order(c, "total_orders_amount", "total_orders"), 
            _average_per_order(p, "total_orders_amount", "total_orders"),  is_decimal=True)

    @render.ui
    def delivery_fee_compared():
        c, p = filtered_dfs()
        return get_comp_text(c["sum_delivery_fee"].sum(), p["sum_delivery_fee"].sum())

    @render.ui
    def total_order_cost():
        c, p = filtered_dfs()
        return get_comp_text(c["sum_driver_earning"].sum(), p["sum_driver_earning"].sum(), is_rev=True)

    @render.ui
    def total_order_revenue():
        c, p = filtered_dfs()
        return get_comp_text(c["service_commission"].sum(), p["service_commission"].sum())

    @render.ui
    def service_delivery_share_compared():
        c, p = filtered_dfs()
        return get_comp_text(c["tiptop_share_delivery_discount"].sum(), p["tiptop_share_delivery_discount"].sum(), is_rev=True)

    @render.ui
    def avg_delivery_revenue_compared():
        c, p = filtered_dfs()
        c_v = (c["sum_delivery_fee"].sum() - c["tiptop_share_delivery_discount"].sum()) / c["delivered_orders"].sum() if c["delivered_orders"].sum() > 0 else 0
        p_v = (p["sum_delivery_fee"].sum() - p["tiptop_share_delivery_discount"].sum()) / p["delivered_orders"].sum() if p["delivered_orders"].sum() > 0 else 0
        return get_comp_text(c_v, p_v, is_decimal=True)

    @render.ui
    def cpo_compared():
        c, p = filtered_dfs()
        c_denom = (c["delivered_orders"].sum() + c["not_delivered_orders"].sum())
        p_denom = (p["delivered_orders"].sum() + p["not_delivered_orders"].sum())
        c_v = c["sum_driver_earning"].sum() / c_denom if c_denom > 0 else 0
        p_v = p["sum_driver_earning"].sum() / p_denom if p_denom > 0 else 0
        return get_comp_text(c_v, p_v, is_rev=True, is_decimal=True)

    @render.ui
    def rpo_compared():
        c, p = filtered_dfs()
        c_denom = (c["delivered_orders"].sum() + c["not_delivered_orders"].sum())
        p_denom = (p["delivered_orders"].sum() + p["not_delivered_orders"].sum())
        c_v = c["service_commission"].sum() / c_denom if c_denom > 0 else 0
        p_v = p["service_commission"].sum() / p_denom if p_denom > 0 else 0
        return get_comp_text(c_v, p_v, is_decimal=True)

    @render.ui
    def l1_compared():
        c, p = filtered_dfs()
        c_v = (c["firstlevel"].sum() / (c["delivered_orders"].sum() + c["not_delivered_orders"].sum())) if (c["delivered_orders"].sum() + c["not_delivered_orders"].sum()) > 0 else 0
        p_v = (p["firstlevel"].sum() / (p["delivered_orders"].sum() + p["not_delivered_orders"].sum())) if (p["delivered_orders"].sum() + p["not_delivered_orders"].sum()) > 0 else 0
        return get_comp_text(c_v, p_v, is_decimal=True)

    @render.ui
    def l2_compared():
        c, p = filtered_dfs()
        c_v = ((c["l2_l1"].sum() - c["coupon_on_delivery_tiptop_share"].sum() - c["coupon_on_item_discount_tiptop_share"].sum()) / (c["delivered_orders"].sum() + c["not_delivered_orders"].sum())) if (c["delivered_orders"].sum() + c["not_delivered_orders"].sum()) > 0 else 0
        p_v = ((p["l2_l1"].sum() - p["coupon_on_delivery_tiptop_share"].sum() - p["coupon_on_item_discount_tiptop_share"].sum()) / (p["delivered_orders"].sum() + p["not_delivered_orders"].sum())) if (p["delivered_orders"].sum() + p["not_delivered_orders"].sum()) > 0 else 0
        return get_comp_text(c_v, p_v, is_decimal=True)

    @render.ui
    def avg_km_compared():
        c, p = filtered_dfs()
        c_v = _average_per_order(c, "direct_distance", "delivered_orders")
        p_v = _average_per_order(p, "direct_distance", "delivered_orders")
        return get_comp_text(c_v, p_v, is_decimal=True, is_rev=True)

    @render_widget
    def orders_plot():
        c, p = filtered_dfs()
        df = pd.concat([c, p])
        if df.empty: return px.scatter(title="No data matches filters")
        hourly = df.groupby([df['date'].dt.hour, 'date_without_time'], observed=True)['delivered_orders'].sum().reset_index()
        hourly.columns = ['Hour', 'Date', 'Delivered']
        hourly['Date'] = hourly['Date'].astype(str)
        return px.line(hourly, x='Hour', y='Delivered', color='Date', markers=True, template='plotly_white')

    def get_breakdown(df, col):
        if df is None or df.empty: 
            return pd.DataFrame(columns=["Name", "Orders", "Delivered", "CPO", "RPO", "AVG DFunded"])
        
        # Ensure the column exists in the dataframe to avoid KeyErrors
        if col not in df.columns:
            return pd.DataFrame(columns=[col, "Orders", "Delivered", "CPO", "RPO", "AVG DFunded"])

        res = df.groupby(col, observed=True).agg({
            'total_orders': 'sum',
            'delivered_orders': 'sum',
            'sum_driver_earning': 'sum', # Switched to sum to calculate real CPO
            'service_commission': 'sum',
            'not_delivered_orders': 'sum', # Ensure not_delivered_orders is included for accurate CPO/RPO
            'cancelled_orders': 'sum', # Include cancelled_orders for discount calculation
            'discount_funded': 'sum' # Sum for total discount funded
        }).reset_index()
        
        # Calculate CPO/RPO based on totals for accuracy
        order_counts = res["delivered_orders"] + res["not_delivered_orders"]
        denominator = order_counts.where(order_counts > 0)
        res["CPO"] = (res["sum_driver_earning"] / denominator).fillna(0)
        res["RPO"] = (res["service_commission"] / denominator).fillna(0)
        res["AVG DFunded"] = (res["discount_funded"] / denominator).fillna(0)
        # Explicit display formatting prevents float32 serialization artifacts.
        for metric in ["CPO", "RPO", "AVG DFunded"]:
            res[metric] = res[metric].map(lambda value: f"{value:.1f}")
        
        # Select and rename final columns
        res = res[[col, "total_orders", "delivered_orders", "CPO", "RPO", "AVG DFunded"]]
        res.columns = ["Name", "Orders", "Delivered", "CPO", "RPO", "AVG DFunded"]

        return res.sort_values(by="Orders", ascending=False)

    @render.data_frame
    def city(): return get_breakdown(filtered_dfs()[0], "city")

    @render.data_frame
    def city_prev(): return get_breakdown(filtered_dfs()[1], "city")

    @render.data_frame
    def merchant():
        curr, _ = filtered_dfs()
        # Explicitly use merchant_city as per your parquet structure
        df = get_breakdown(curr, "merchant_city")
        return render.DataTable(df, filters=True, selection_mode="row")

    @render.data_frame
    def merchant_prev():
        _, prev = filtered_dfs()
        df = get_breakdown(prev, "merchant_city")
        return render.DataTable(df, filters=True, selection_mode="row")
    
    #----------------------------------------------------------------------------------------------
    ## Taxi trips
    @reactive.calc
    def taxi_filtered_dfs():
        df = taxi_store.get()
        if df is None: return pd.DataFrame(), pd.DataFrame()

        mask = pd.Series(True, index=df.index)

        for col, val in [("city", input.taxi_filter_city())]:
            val = normalize_selectize_selection(val)
            if val and col in df.columns:
                mask &= df[col].isin(val)
        
        s, e = input.taxi_date_range() # Or a separate input.taxi_date_range()
        ps, pe = input.taxi_prev_date_range()
        s, e, ps, pe = map(_normal_date, [s, e, ps, pe])
        dates = df["date_without_time"]
        curr = df[mask & dates.between(s, e)]
        prev = df[mask & dates.between(ps, pe)]
        return curr, prev

    CANCEL_STATUSES = {
        'Canceled Before Reserved',
        'Canceled After Reserved',
        'Canceled After Arrived',
        'Canceled After Start',
    }

    @render.ui
    def all_trips_compared():
        c, p = taxi_filtered_dfs()
        return get_comp_text(c["request_id"].count(), p["request_id"].count())

    @render.ui
    def trips_success_compared():
        c, p = taxi_filtered_dfs()
        return get_comp_text(c[c['status'] == 'Successful']["request_id"].count(), p[p['status'] == 'Successful']["request_id"].count())

    @render.ui
    def trips_canceled_compared():
        c, p = taxi_filtered_dfs()
        current_count = c[c['status'].isin(CANCEL_STATUSES)]["request_id"].count()
        previous_count = p[p['status'].isin(CANCEL_STATUSES)]["request_id"].count()
        return get_comp_text(current_count, previous_count, is_rev=True)
    
    @render.ui
    def trips_timed_out_compared():
        c, p = taxi_filtered_dfs()
        return get_comp_text(c[c['status'] == 'Timeout']["request_id"].count(), p[p['status'] == 'Timeout']["request_id"].count(), is_rev=True)
    
#% success - % canceled - % timeouts
    @render.ui
    def trips_success_pct_compared():
        c, p = taxi_filtered_dfs()
        c =c[c['status'] == 'Successful']["request_id"].count()/c["request_id"].count()*100 if c["request_id"].count() > 0 else 0
        p =p[p['status'] == 'Successful']["request_id"].count()/p["request_id"].count()*100 if p["request_id"].count() > 0 else 0
        return get_comp_text(c,p, is_pct=True)
    
    @render.ui
    def trips_cancelation_pct_compared():
        c, p = taxi_filtered_dfs()
        c =c[c['status'].isin(CANCEL_STATUSES)]["request_id"].count()/c["request_id"].count()*100 if c["request_id"].count() > 0 else 0
        p =p[p['status'].isin(CANCEL_STATUSES)]["request_id"].count()/p["request_id"].count()*100 if p["request_id"].count() > 0 else 0
        return get_comp_text(c,p, is_pct=True, is_rev=True)

    @render.ui
    def trips_timeout_pct_compared():
        c, p = taxi_filtered_dfs()
        c =c[c['status'] == 'Timeout']["request_id"].count()/c["request_id"].count()*100 if c["request_id"].count() > 0 else 0
        p =p[p['status'] == 'Timeout']["request_id"].count()/p["request_id"].count()*100 if p["request_id"].count() > 0 else 0
        return get_comp_text(c,p, is_pct=True, is_rev=True)

#GMV - NMV - TT commission - avg commission - discount funded - penalties
    @render.ui
    def taxi_gmv_compared():
        c, p = taxi_filtered_dfs()
        return get_comp_text(c["price"].sum(), p["price"].sum())

    @render.ui
    def taxi_nmv_compared():
        c, p = taxi_filtered_dfs()
        c=c[c['status'] == 'Successful']["price"].sum()        
        p=p[p['status'] == 'Successful']["price"].sum()
        return get_comp_text(c, p)

    @render.ui
    def taxi_discount_funded_compared():
        c, p = taxi_filtered_dfs()
        c=c[c['status'] == 'Successful']["offer_discount_amount"].sum() + c[c['status'] == 'Successful']["coupon_discount_amount"].sum()      
        p=p[p['status'] == 'Successful']["offer_discount_amount"].sum() + p[p['status'] == 'Successful']["coupon_discount_amount"].sum()
        return get_comp_text(c, p, is_rev=True)

    @render.ui
    def taxi_commission_compared():
        c, p = taxi_filtered_dfs()
        c=c[c['status'] == 'Successful']["tiptop_commission"].sum()     
        p=p[p['status'] == 'Successful']["tiptop_commission"].sum()
        return get_comp_text(c, p)

    @render.ui
    def taxi_avg_commission_compared():
        c, p = taxi_filtered_dfs()
        c=c[c['status'] == 'Successful']["tiptop_commission"].mean()     
        p=p[p['status'] == 'Successful']["tiptop_commission"].mean()
        return get_comp_text(c, p, is_decimal=True)

    @render.ui
    def taxi_avg_discount_funded_compared():
        c, p = taxi_filtered_dfs()
        c_success = c[c['status'] == 'Successful']
        p_success = p[p['status'] == 'Successful']
        c_avg = (c_success["offer_discount_amount"].fillna(0) + 
                c_success["coupon_discount_amount"].fillna(0)).mean()    
        p_avg = (p_success["offer_discount_amount"].fillna(0) + 
                p_success["coupon_discount_amount"].fillna(0)).mean()
        return get_comp_text(c_avg, p_avg, is_rev=True)
    
#avg trip price - avg trip km - avg trip duration

    @render.ui
    def taxi_avg_trip_price_compared():
        c, p = taxi_filtered_dfs()
        c=c[c['status'] == 'Successful']["price"].mean()        
        p=p[p['status'] == 'Successful']["price"].mean()
        return get_comp_text(c, p)

    @render.ui
    def taxi_avg_trip_km_compared():
        c, p = taxi_filtered_dfs()
        c=c[c['status'] == 'Successful']["distance"].mean()        
        p=p[p['status'] == 'Successful']["distance"].mean()
        return get_comp_text(c, p, is_decimal=True, is_rev=True)
    
    @render.ui
    def taxi_avg_trip_duration_compared():
        c, p = taxi_filtered_dfs()
        c=c[c['status'] == 'Successful']["duration"].mean()        
        p=p[p['status'] == 'Successful']["duration"].mean()
        return get_comp_text(c, p, is_decimal=True, is_rev=True)
    
# unique drivers - unique passengers - trips per driver - trips per passenger
    @render.ui
    def taxi_unique_users_compared():
        c, p = taxi_filtered_dfs()
        c=c[c['status'] == 'Successful']["user_id"].nunique()
        p=p[p['status'] == 'Successful']["user_id"].nunique()
        # c=c["user_id"].nunique()
        # p=p["user_id"].nunique()
        return get_comp_text(c, p)
    
    @render.ui
    def taxi_unique_users_cancelation_compared():
        cancel_statuses = list(CANCEL_STATUSES) + ['Timeout']
        c, p = taxi_filtered_dfs()
        c=c[c['status'].isin(cancel_statuses)]["user_id"].nunique()        
        p=p[p['status'].isin(cancel_statuses)]["user_id"].nunique() 
        return get_comp_text(c, p, is_rev=True)
    
    @render.ui
    def taxi_unique_drivers_compared():
        c, p = taxi_filtered_dfs()
        # c=c[c['status'] == 'Successful']["driver_id"].nunique()        
        # p=p[p['status'] == 'Successful']["driver_id"].nunique() 
        c=c["driver_id"].nunique()        
        p=p["driver_id"].nunique() 
        return get_comp_text(c, p)
    
    @render.ui
    def taxi_trip_per_users_compared():
        c, p = taxi_filtered_dfs()
        c=c[c['status'] == 'Successful']["request_id"].count() / c[c['status'] == 'Successful']["user_id"].nunique() if c[c['status'] == 'Successful']["request_id"].count() > 0 else 0     
        p=p[p['status'] == 'Successful']["request_id"].count() / p[p['status'] == 'Successful']["user_id"].nunique() if p[p['status'] == 'Successful']["request_id"].count() > 0 else 0
        return get_comp_text(c, p, is_decimal=True)
    
    @render.ui
    def taxi_trip_per_drivers_compared():
        c, p = taxi_filtered_dfs()
        c=c[c['status'] == 'Successful']["request_id"].count() / c[c['status'] == 'Successful']["driver_id"].nunique() if c[c['status'] == 'Successful']["request_id"].count() > 0 else 0     
        p=p[p['status'] == 'Successful']["request_id"].count() / p[p['status'] == 'Successful']["driver_id"].nunique() if p[p['status'] == 'Successful']["request_id"].count() > 0 else 0
        return get_comp_text(c, p, is_decimal=True)

# cancelation reasons breakdown - cancelers - 

    

app = App(app_ui, server)


if __name__ == "__main__":
    from shiny import run_app

    run_app(
        "app:app",
        app_dir=str(BASE_DIR),
        host="127.0.0.1",
        port=8000,
        launch_browser=True,
    )
