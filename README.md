import pandas as pd
import streamlit as st
from pathlib import Path

DATA_ROOT = Path("data")
CATEGORY_TABS = [
    "Contract Overview",
    "Satellite Cities",
    "Satellite Fee",
    "KAM fees",
    "Recall assistance fee",
    "Defective items and penalty",
    "Min Ads Spend",
    "Off-Invoice/ Turnover Discount",
]


def find_parquet_files_for_category(category):
    if not DATA_ROOT.exists():
        return []
    keywords = [token for token in category.lower().replace("/", " ").split() if token]
    matches = []
    for path in DATA_ROOT.rglob("*.parquet"):
        name = path.name.lower()
        if all(keyword in name for keyword in keywords):
            matches.append(path)
    return sorted(matches)


def load_parquet_files(files):
    frames = []
    for path in files:
        try:
            frames.append(pd.read_parquet(path))
        except Exception as exc:
            st.warning(f"Could not load {path}: {exc}")
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True, sort=False)


def parse_date_columns(df):
    for col in df.columns:
        if any(token in col.lower() for token in ("date", "start", "end", "effective", "from", "to")):
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def filter_by_manufacturer(df, manufacturer):
    if df.empty or not manufacturer:
        return df
    manufacturer_columns = [
        col
        for col in df.columns
        if any(word in col.lower() for word in ("manufacturer", "vendor", "brand", "supplier"))
    ]
    if not manufacturer_columns:
        return df
    mask = pd.Series(False, index=df.index)
    for col in manufacturer_columns:
        mask |= df[col].astype(str).str.contains(manufacturer, case=False, na=False)
    return df[mask]


def filter_by_date_range(df, start_date, end_date):
    if df.empty or start_date is None or end_date is None:
        return df
    df = parse_date_columns(df)
    date_cols = [col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col])]
    if not date_cols:
        return df
    mask = pd.Series(False, index=df.index)
    for col in date_cols:
        mask |= (df[col] >= pd.Timestamp(start_date)) & (df[col] <= pd.Timestamp(end_date))
    return df[mask]


def render_category(category, manufacturer, start_date, end_date):
    st.header(category)
    files = find_parquet_files_for_category(category)
    if not files:
        st.info(
            "No parquet files found for this section. "
            "Place files under `data/` and include the section keywords in the filename."
        )
        return

    st.write("**Loaded files:**")
    for path in files:
        st.write(f"- `{path}`")

    df = load_parquet_files(files)
    if df.empty:
        st.warning("No data available in the matched parquet files.")
        return

    df = parse_date_columns(df)
    df = filter_by_manufacturer(df, manufacturer)
    df = filter_by_date_range(df, start_date, end_date)

    st.markdown(f"**Rows after filter:** {len(df)}")
    st.dataframe(df.head(100), use_container_width=True)

    if not df.empty:
        st.markdown("### Schema")
        st.write(df.dtypes.astype(str).to_dict())

        numeric = df.select_dtypes(include="number")
        if not numeric.empty:
            st.markdown("### Numeric summary")
            st.dataframe(numeric.describe().transpose(), use_container_width=True)

        date_columns = [col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col])]
        if date_columns:
            st.markdown("### Detected date columns")
            st.write(date_columns)


def main():
    st.set_page_config(page_title="IOCC-Contracts and fees", layout="wide")
    st.title("IOCC-Contracts and fees")

    with st.sidebar:
        st.header("Filters")
        manufacturer = st.text_input("Search by manufacturer / vendor")
        date_range = st.date_input("Filter by date range", [])
        start_date = end_date = None
        if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
            start_date, end_date = date_range
        if start_date and end_date and start_date > end_date:
            st.error("Start date must be before end date.")
            start_date = end_date = None

    tabs = st.tabs(CATEGORY_TABS)
    for tab, category in zip(tabs, CATEGORY_TABS):
        with tab:
            render_category(category, manufacturer, start_date, end_date)


if __name__ == "__main__":
    main()