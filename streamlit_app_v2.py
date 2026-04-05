"""아정당 운영 추천 대시보드 — Snowflake Hackathon 2026 Korea (v2)

내부 계약 데이터(V01-V11)와 외부 신호(날씨·공휴일·뉴스)를 함께 읽어
지금 가장 먼저 점검해야 할 지역·상품·채널을 추천합니다.

v2: 전략 분류·추천·시나리오를 Cortex AI가 동적으로 생성하는 하이브리드 구조
"""
import altair as alt
import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session

# ── 데이터베이스 상수 ─────────────────────────────────────────────────────────
MKT_DB = "SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION"
MKT_SCHEMA = "TELECOM_INSIGHTS"
MKT = f"{MKT_DB}.{MKT_SCHEMA}"

EXT_DB = "GUNNI_HACKATHON_LAB"
EXT_SCHEMA = "HACKATHON_EXTERNAL_SIGNALS"
EXT = f"{EXT_DB}.{EXT_SCHEMA}"

HACKATHON_MKT = f"{EXT_DB}.HACKATHON_MARKETING"

# ── 페이지 설정 ───────────────────────────────────────────────────────────────
st.set_page_config(page_title="아정당 운영 추천 대시보드", page_icon="📊", layout="wide")


@st.cache_resource
def get_session():
    return get_active_session()


def q(sql: str) -> pd.DataFrame:
    return get_session().sql(sql).to_pandas()


# ── 유틸 함수 ────────────────────────────────────────────────────────────────
def fmt_int(v) -> str:
    return f"{int(round(float(v))):,}"

def fmt_pct(v) -> str:
    return f"{float(v):.2f}%"

def fmt_num(v) -> str:
    return f"{float(v):,.1f}"

def clamp01(v: float) -> float:
    return max(0.0, min(1.0, v))

def safe_rate(df: pd.DataFrame, col: str = "PAYEND_RATE", multiplier: float = 100) -> float:
    if df.empty or col not in df.columns:
        return 0.0
    val = df[col].iloc[0]
    return float(val) * multiplier if pd.notna(val) else 0.0

def safe_float(val, default: float = 0.0) -> float:
    try:
        return float(val) if pd.notna(val) else default
    except (ValueError, TypeError):
        return default

def metric_card(title: str, value: str, note: str = "") -> None:
    st.markdown(
        f"""<div style="padding:16px;border:1px solid #dbe5f0;border-radius:16px;
        background:#fff;height:100%;">
        <div style="font-size:13px;color:#53657a;">{title}</div>
        <div style="font-size:28px;font-weight:800;color:#133a5e;margin-top:8px;">{value}</div>
        <div style="font-size:12px;color:#6a7d91;margin-top:6px;line-height:1.55;">{note}</div>
        </div>""",
        unsafe_allow_html=True,
    )


# ── 공통 CSS ──────────────────────────────────────────────────────────────────
st.markdown("""<style>
.main {background: linear-gradient(180deg,#f5f8fb 0%,#fff 22%);}
.block-container {padding-top:2rem;padding-bottom:3rem;}
.summary-box {margin-top:10px;padding:12px 14px;border-radius:14px;
  background:#eef6ff;border:1px solid #d5e7fb;color:#173b63;font-weight:700;}
.hero {padding:24px;border-radius:24px;
  background:linear-gradient(135deg,#0f766e 0%,#155e75 52%,#1d4ed8 100%);
  color:#f8fbff;box-shadow:0 16px 36px rgba(15,39,64,.10);margin-bottom:18px;}
.hero-kicker {font-size:13px;font-weight:700;opacity:.9;}
.hero-title {font-size:32px;font-weight:800;margin-top:8px;line-height:1.3;}
.hero-text {margin-top:12px;font-size:15px;line-height:1.7;color:rgba(248,251,255,.94);}
.hero-pill {display:inline-block;padding:5px 11px;border-radius:999px;
  background:rgba(255,255,255,.16);color:#fff;font-size:12px;margin-right:6px;margin-top:4px;}
.mini-card {border:1px solid #dbe5f0;border-radius:18px;background:#fff;padding:16px;height:100%;}
.mini-card-title {font-size:13px;color:#5a7085;}
.mini-card-main {margin-top:8px;font-size:20px;font-weight:800;color:#173b63;line-height:1.4;}
.mini-card-note {margin-top:10px;font-size:13px;color:#5f7386;line-height:1.6;}
.ai-box {margin-top:14px;padding:16px 18px;border-radius:16px;
  background:linear-gradient(135deg,#f0f7ff,#e8f5e9);border:1px solid #c8e6c9;
  color:#1b5e20;font-size:14px;line-height:1.7;}
.ai-box-title {font-size:12px;font-weight:700;color:#2e7d32;margin-bottom:6px;}
.pill {display:inline-block;padding:4px 10px;border-radius:999px;
  background:#e8f7f5;color:#0f766e;font-size:12px;margin-right:6px;margin-top:4px;}
.pill-orange {display:inline-block;padding:4px 10px;border-radius:999px;
  background:#ffe6d6;color:#8a3307;font-size:12px;margin-right:6px;margin-top:4px;}
.pill-blue {display:inline-block;padding:4px 10px;border-radius:999px;
  background:#dbe8ff;color:#1e429f;font-size:12px;margin-right:6px;margin-top:4px;}
.pill-red {display:inline-block;padding:4px 10px;border-radius:999px;
  background:#ffe0e0;color:#991b1b;font-size:12px;margin-right:6px;margin-top:4px;}
.pill-green {display:inline-block;padding:4px 10px;border-radius:999px;
  background:#dcfce7;color:#166534;font-size:12px;margin-right:6px;margin-top:4px;}
.strategy-card {border:1px solid #dbe5f0;border-radius:18px;background:#fff;padding:20px;
  height:100%;transition:box-shadow .2s;}
.strategy-card:hover {box-shadow:0 8px 24px rgba(15,39,64,.12);}
.strategy-rank {display:inline-block;padding:3px 10px;border-radius:999px;
  font-size:11px;font-weight:700;margin-bottom:8px;}
.rank-1 {background:#0f766e;color:#fff;}
.rank-2 {background:#155e75;color:#fff;}
.rank-3 {background:#1d4ed8;color:#fff;}
.scenario-active {border:2px solid #0f766e;border-radius:18px;background:#f0fdf4;padding:16px;}
.chat-msg {padding:12px 16px;border-radius:14px;margin-bottom:8px;line-height:1.7;font-size:14px;}
.chat-user {background:#e8f0fe;color:#1a3c63;border-bottom-right-radius:4px;}
.chat-ai {background:linear-gradient(135deg,#f0f7ff,#e8f5e9);color:#1b5e20;border-bottom-left-radius:4px;}
</style>""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# 데이터 로드
# ══════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=3600)
def _load_v01_all():
    """V01 원본 로드 + 파생 데이터 한 번에 캐시"""
    raw = q(f"""
        SELECT YEAR_MONTH, INSTALL_STATE AS REGION, MAIN_CATEGORY_NAME,
               CONTRACT_COUNT, PAYEND_COUNT, TOTAL_NET_SALES
        FROM (
            SELECT YEAR_MONTH, INSTALL_STATE, MAIN_CATEGORY_NAME,
                   CONTRACT_COUNT, PAYEND_COUNT, TOTAL_NET_SALES
            FROM {MKT}.V01_MONTHLY_REGIONAL_CONTRACT_STATS
            WHERE YEAR(YEAR_MONTH) IN (2024, 2025)
        ) sub
    """)
    raw["YEAR_MONTH"] = pd.to_datetime(raw["YEAR_MONTH"])
    raw["YR"] = raw["YEAR_MONTH"].dt.year
    raw["Q_NUM"] = raw["YEAR_MONTH"].dt.quarter
    raw["LOSS_COUNT"] = raw["CONTRACT_COUNT"] - raw["PAYEND_COUNT"]

    # yearly
    yr = raw.groupby(["YR", "MAIN_CATEGORY_NAME"], as_index=False).agg(
        CONTRACT_COUNT=("CONTRACT_COUNT", "sum"),
        PAYEND_COUNT=("PAYEND_COUNT", "sum"),
        LOSS_COUNT=("LOSS_COUNT", "sum"),
        TOTAL_NET_SALES=("TOTAL_NET_SALES", "sum"),
    )
    yr["PAYEND_RATE"] = (yr["PAYEND_COUNT"] / yr["CONTRACT_COUNT"].replace(0, pd.NA)).round(4)
    yr = yr.sort_values(["YR", "CONTRACT_COUNT"], ascending=[True, False])

    # monthly
    mo = raw.groupby(["YEAR_MONTH", "MAIN_CATEGORY_NAME"], as_index=False).agg(
        CONTRACT_COUNT=("CONTRACT_COUNT", "sum"),
        PAYEND_COUNT=("PAYEND_COUNT", "sum"),
        LOSS_COUNT=("LOSS_COUNT", "sum"),
        TOTAL_NET_SALES=("TOTAL_NET_SALES", "sum"),
    )
    mo["PAYEND_RATE"] = (mo["PAYEND_COUNT"] / mo["CONTRACT_COUNT"].replace(0, pd.NA)).round(4)
    mo = mo.sort_values(["YEAR_MONTH", "CONTRACT_COUNT"], ascending=[False, False])

    # quarterly
    inet_rental = raw[raw["MAIN_CATEGORY_NAME"].isin(["인터넷", "렌탈"])]
    qtr = inet_rental.groupby(["YR", "Q_NUM", "MAIN_CATEGORY_NAME"], as_index=False).agg(
        CONTRACT_COUNT=("CONTRACT_COUNT", "sum"),
        PAYEND_COUNT=("PAYEND_COUNT", "sum"),
        LOSS_COUNT=("LOSS_COUNT", "sum"),
    )
    qtr["QTR"] = qtr["YR"].astype(str) + " " + qtr["Q_NUM"].astype(str) + "Q"
    qtr["PAYEND_RATE_PCT"] = ((qtr["PAYEND_COUNT"] / qtr["CONTRACT_COUNT"].replace(0, pd.NA)) * 100).round(2)
    qtr = qtr.sort_values(["YR", "Q_NUM", "MAIN_CATEGORY_NAME"])

    # regional (2025만)
    r25 = raw[raw["YR"] == 2025]
    reg = r25.groupby(["REGION", "MAIN_CATEGORY_NAME"], as_index=False).agg(
        CONTRACT_COUNT=("CONTRACT_COUNT", "sum"),
        PAYEND_COUNT=("PAYEND_COUNT", "sum"),
        LOSS_COUNT=("LOSS_COUNT", "sum"),
        NET_SALES=("TOTAL_NET_SALES", "sum"),
    )
    reg["PAYEND_RATE_PCT"] = ((reg["PAYEND_COUNT"] / reg["CONTRACT_COUNT"].replace(0, pd.NA)) * 100).round(2)
    reg = reg.sort_values("CONTRACT_COUNT", ascending=False)

    return yr, mo, qtr, reg


@st.cache_data(ttl=3600)
def load_v03_funnel() -> pd.DataFrame:
    return q(f"""
        SELECT YR, MAIN_CATEGORY_NAME,
            SUM(TOTAL_COUNT) AS TOTAL_COUNT,
            SUM(CONSULT_REQ) AS CONSULT_REQ,
            SUM(SUBSCRIPTION) AS SUBSCRIPTION,
            SUM(REGISTEND) AS REGISTEND,
            SUM(PAYEND) AS PAYEND,
            ROUND(SUM(CONSULT_REQ) / NULLIF(SUM(TOTAL_COUNT),0)*100, 2) AS TOTAL_TO_CONSULT_PCT,
            ROUND(SUM(SUBSCRIPTION) / NULLIF(SUM(TOTAL_COUNT),0)*100, 2) AS TOTAL_TO_SUB_PCT,
            ROUND(SUM(REGISTEND) / NULLIF(SUM(SUBSCRIPTION),0)*100, 2) AS SUB_TO_REG_PCT,
            ROUND(SUM(PAYEND) / NULLIF(SUM(TOTAL_COUNT),0)*100, 2) AS TOTAL_PAYEND_PCT
        FROM (
            SELECT YEAR(YEAR_MONTH) AS YR, MAIN_CATEGORY_NAME,
                   TOTAL_COUNT, CONSULT_REQUEST_COUNT AS CONSULT_REQ,
                   SUBSCRIPTION_COUNT AS SUBSCRIPTION,
                   REGISTEND_COUNT AS REGISTEND, PAYEND_COUNT AS PAYEND
            FROM {MKT}.V03_CONTRACT_FUNNEL_CONVERSION
            WHERE YEAR(YEAR_MONTH) IN (2024, 2025)
        ) sub
        GROUP BY 1, 2
        ORDER BY 1, 3 DESC
    """)


@st.cache_data(ttl=3600)
def _load_v06_all():
    """V06 원본 로드 + 파생 데이터 한 번에 캐시"""
    raw = q(f"""
        SELECT INSTALL_STATE AS REGION, RENTAL_SUB_CATEGORY,
               CONTRACT_COUNT, PAYEND_COUNT
        FROM (
            SELECT INSTALL_STATE, RENTAL_SUB_CATEGORY, CONTRACT_COUNT, PAYEND_COUNT
            FROM {MKT}.V06_RENTAL_CATEGORY_TRENDS
            WHERE YEAR(YEAR_MONTH) = 2025
        ) sub
    """)
    raw["LOSS_COUNT"] = raw["CONTRACT_COUNT"] - raw["PAYEND_COUNT"]

    prod = raw.groupby("RENTAL_SUB_CATEGORY", as_index=False).agg(
        CONTRACT_COUNT=("CONTRACT_COUNT", "sum"),
        PAYEND_COUNT=("PAYEND_COUNT", "sum"),
        LOSS_COUNT=("LOSS_COUNT", "sum"),
    )
    prod["PAYEND_RATE_PCT"] = ((prod["PAYEND_COUNT"] / prod["CONTRACT_COUNT"].replace(0, pd.NA)) * 100).round(2)
    prod = prod.sort_values("CONTRACT_COUNT", ascending=False)

    reg = raw.groupby(["REGION", "RENTAL_SUB_CATEGORY"], as_index=False).agg(
        CONTRACT_COUNT=("CONTRACT_COUNT", "sum"),
        PAYEND_COUNT=("PAYEND_COUNT", "sum"),
        LOSS_COUNT=("LOSS_COUNT", "sum"),
    )
    reg["PAYEND_RATE_PCT"] = ((reg["PAYEND_COUNT"] / reg["CONTRACT_COUNT"].replace(0, pd.NA)) * 100).round(2)
    reg = reg[reg["CONTRACT_COUNT"] >= 100].sort_values(["PAYEND_RATE_PCT", "LOSS_COUNT"], ascending=[True, False])

    return prod, reg


@st.cache_data(ttl=3600)
def load_v04_channel() -> pd.DataFrame:
    return q(f"""
        SELECT MAIN_CATEGORY_NAME, RECEIVE_PATH_NAME, INFLOW_PATH_NAME,
            SUM(CONTRACT_COUNT) AS CONTRACT_COUNT,
            SUM(PAYEND_COUNT) AS PAYEND_COUNT,
            ROUND(SUM(PAYEND_COUNT) / NULLIF(SUM(CONTRACT_COUNT),0) * 100, 2) AS PAYEND_RATE_PCT
        FROM (
            SELECT MAIN_CATEGORY_NAME, RECEIVE_PATH_NAME, INFLOW_PATH_NAME,
                   CONTRACT_COUNT, PAYEND_COUNT
            FROM {MKT}.V04_CHANNEL_CONTRACT_PERFORMANCE
            WHERE YEAR(YEAR_MONTH) = 2025
        ) sub
        GROUP BY 1, 2, 3
        HAVING SUM(CONTRACT_COUNT) >= 50
        ORDER BY 4 DESC
    """)


@st.cache_data(ttl=3600)
def load_v09_call_stats() -> pd.DataFrame:
    return q(f"""
        SELECT MAIN_CATEGORY_NAME, DIVISION_NAME,
            SUM(CALL_COUNT) AS CALL_COUNT,
            SUM(CONNECTED_COUNT) AS CONNECTED_COUNT,
            ROUND(SUM(CONNECTED_COUNT) / NULLIF(SUM(CALL_COUNT),0) * 100, 2) AS CONNECT_RATE_PCT,
            ROUND(AVG(AVG_BILL_MIN), 2) AS AVG_BILL_MIN
        FROM (
            SELECT MAIN_CATEGORY_NAME, DIVISION_NAME,
                   CALL_COUNT, CONNECTED_COUNT, AVG_BILL_MINUTE AS AVG_BILL_MIN
            FROM {MKT}.V09_MONTHLY_CALL_STATS
            WHERE YEAR(YEAR_MONTH) = 2025
        ) sub
        GROUP BY 1, 2
        ORDER BY 1, 2
    """)


@st.cache_data(ttl=3600)
def load_v11_call_cvr() -> pd.DataFrame:
    return q(f"""
        SELECT MAIN_CATEGORY_NAME, DIVISION_NAME,
            SUM(TOTAL_CALLS) AS TOTAL_CALLS,
            SUM(LINKED_CONTRACTS) AS LINKED_CONTRACTS,
            ROUND(SUM(LINKED_CONTRACTS) / NULLIF(SUM(TOTAL_CALLS),0) * 100, 2) AS CALL_CVR_PCT
        FROM (
            SELECT MAIN_CATEGORY_NAME, DIVISION_NAME, TOTAL_CALLS, LINKED_CONTRACTS
            FROM {MKT}.V11_CALL_TO_CONTRACT_CONVERSION
            WHERE YEAR(YEAR_MONTH) = 2025
        ) sub
        GROUP BY 1, 2
        ORDER BY 1, 2
    """)


def _safe_q(sql: str) -> pd.DataFrame:
    try:
        return q(sql)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=1800)
def load_external_news() -> pd.DataFrame:
    df = _safe_q(f"""
        SELECT KEYWORD_GROUP, TITLE, PUBLISHED_AT, SUMMARY_SNIPPET AS SUMMARY
        FROM {EXT}.SRC_KR_PUBLIC_POLICY_ARTICLES
        ORDER BY PUBLISHED_AT DESC
        LIMIT 30
    """)
    if df.empty:
        df = _safe_q(f"""
            SELECT KEYWORD_GROUP, TITLE, PUBLISHED_AT, SUMMARY
            FROM {EXT}.KR_NEWS_ARTICLES
            ORDER BY PUBLISHED_AT DESC
            LIMIT 30
        """)
    return df


@st.cache_data(ttl=1800)
def load_weather(ref_date: str) -> pd.DataFrame:
    return _safe_q(f"""
        SELECT REGION, FORECAST_DATE, AVG_TEMP_C, WEATHER_CONDITION, PRECIPITATION_MM
        FROM {EXT}.SRC_KR_WEATHER_DAILY_FORECAST
        WHERE FORECAST_DATE >= '{ref_date}'
        ORDER BY FORECAST_DATE ASC
        LIMIT 50
    """)


@st.cache_data(ttl=1800)
def load_holidays(ref_date: str) -> pd.DataFrame:
    return _safe_q(f"""
        SELECT HOLIDAY_DATE, HOLIDAY_NAME, IS_HOLIDAY
        FROM {EXT}.SRC_KR_HOLIDAY_CALENDAR
        WHERE HOLIDAY_DATE BETWEEN '{ref_date}' AND DATEADD(day, 14, '{ref_date}'::DATE)
        ORDER BY HOLIDAY_DATE ASC
    """)


@st.cache_data(ttl=86400)
def load_freshness() -> pd.DataFrame:
    return _safe_q(f"""
        WITH weather AS (
          SELECT '날씨 예보' AS NAME, MAX(FETCHED_AT) AS UPDATED_AT FROM {EXT}.SRC_KR_WEATHER_DAILY_FORECAST
        ), holiday AS (
          SELECT '공휴일' AS NAME, MAX(FETCHED_AT) AS UPDATED_AT FROM {EXT}.SRC_KR_HOLIDAY_CALENDAR
        ), policy AS (
          SELECT '정책뉴스' AS NAME, MAX(FETCHED_AT) AS UPDATED_AT FROM {EXT}.SRC_KR_PUBLIC_POLICY_ARTICLES
        ), mart AS (
          SELECT '외부 신호 마트' AS NAME, MAX(BUILT_AT) AS UPDATED_AT FROM {EXT}.MART_KR_DAILY_EXTERNAL_SIGNALS
        )
        SELECT * FROM weather UNION ALL SELECT * FROM holiday
        UNION ALL SELECT * FROM policy UNION ALL SELECT * FROM mart
    """)


def cortex_available() -> bool:
    """Cortex 사용 가능 여부 — 낙관적 접근 (첫 호출 시 자동 감지, ping 제거)"""
    if "cortex_ok" in st.session_state:
        return st.session_state["cortex_ok"]
    # 첫 로드 시 ping 하지 않음 — 실제 Cortex 호출 시 에러나면 False로 전환
    return True


def _escape_for_sql(text: str) -> str:
    """Cortex AI 호출용 SQL 문자열 이스케이프"""
    return text.replace("\\", "\\\\").replace("'", "''")


@st.cache_data(ttl=60, show_spinner=False)
def cortex_chat(user_question: str, context: str) -> str:
    prompt = (
        "당신은 아정당(통신·렌탈 플랫폼) 운영 추천 대시보드의 AI 어시스턴트입니다.\n\n"
        "[필수 규칙]\n"
        "- 모든 답변에 아래 데이터의 구체적 수치(계약 수, 완료율, 전환율, 건수)를 반드시 인용하세요\n"
        "- 제안할 때는 현재 수치 → 목표 수치 → 예상 개선 효과(건수/%)를 함께 제시하세요\n"
        "- 추세가 있으면 방향(상승/하락)과 변화폭(%p)을 명시하세요\n"
        "- 일반적 조언만 하지 말고, 이 데이터에서 도출 가능한 구체적 인사이트를 제시하세요\n"
        "- 모르는 내용은 모른다고 하세요\n\n"
        f"[대시보드 데이터 컨텍스트]\n{context}\n\n[질문]\n{user_question}"
    )
    safe = _escape_for_sql(prompt)
    try:
        session = get_session()
        df = session.sql(
            "SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', ?) AS R",
            params=[prompt],
        ).to_pandas()
        st.session_state["cortex_ok"] = True
        return str(df.iloc[0]["R"]).strip() if not df.empty else "답변을 생성하지 못했습니다."
    except Exception:
        try:
            df = q(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2','{safe}') AS R")
            st.session_state["cortex_ok"] = True
            return str(df.iloc[0]["R"]).strip() if not df.empty else "답변을 생성하지 못했습니다."
        except Exception as e:
            st.session_state["cortex_ok"] = False
            return f"Cortex AI 오류: {e}"


@st.cache_data(ttl=300, show_spinner=False)
def cortex_summarize(text: str) -> str:
    prompt = text[:6000]
    safe = _escape_for_sql(prompt)
    try:
        session = get_session()
        df = session.sql(
            "SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', ?) AS R",
            params=[prompt],
        ).to_pandas()
        st.session_state["cortex_ok"] = True
        return str(df.iloc[0]["R"]).strip() if not df.empty else ""
    except Exception:
        try:
            df = q(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2','{safe}') AS R")
            st.session_state["cortex_ok"] = True
            return str(df.iloc[0]["R"]).strip() if not df.empty else ""
        except Exception:
            st.session_state["cortex_ok"] = False
            return ""


# ══════════════════════════════════════════════════════════════════════════════
# [1층] 데이터 팩트 추출 — 확정적, 빠름
# ══════════════════════════════════════════════════════════════════════════════

def build_data_facts(
    v01_reg: pd.DataFrame,
    v06_product: pd.DataFrame,
    v06_region: pd.DataFrame,
    v04_channel: pd.DataFrame,
    v11_cvr: pd.DataFrame,
    ext_news: pd.DataFrame,
    weather_df: pd.DataFrame,
    holiday_df: pd.DataFrame,
) -> str:
    """모든 데이터를 하나의 텍스트 요약으로 변환 — AI 프롬프트 컨텍스트용"""
    parts = []

    # 지역×카테고리 현황
    if not v01_reg.empty:
        parts.append("=== 지역×카테고리 현황 (2025) ===")
        inet_avg = float(v01_reg[v01_reg["MAIN_CATEGORY_NAME"] == "인터넷"]["PAYEND_RATE_PCT"].mean()) if not v01_reg[v01_reg["MAIN_CATEGORY_NAME"] == "인터넷"].empty else 0
        parts.append(f"인터넷 전국 평균 완료율: {inet_avg:.1f}%")
        for _, r in v01_reg.iterrows():
            parts.append(
                f"  {r['REGION']} {r['MAIN_CATEGORY_NAME']}: "
                f"계약 {fmt_int(r['CONTRACT_COUNT'])}건, 완료율 {r['PAYEND_RATE_PCT']}%, "
                f"미완료 {fmt_int(r['LOSS_COUNT'])}건"
            )

    # 렌탈 상품별
    if not v06_product.empty:
        parts.append("\n=== 렌탈 세부상품 현황 (2025) ===")
        for _, r in v06_product.iterrows():
            parts.append(
                f"  {r['RENTAL_SUB_CATEGORY']}: 계약 {fmt_int(r['CONTRACT_COUNT'])}건, "
                f"완료율 {r['PAYEND_RATE_PCT']}%, 손실 {fmt_int(r['LOSS_COUNT'])}건"
            )

    # 렌탈 지역×상품 하위
    if not v06_region.empty:
        parts.append("\n=== 렌탈 지역×상품 완료율 하위 10 ===")
        for _, r in v06_region.head(10).iterrows():
            parts.append(
                f"  {r['REGION']} {r['RENTAL_SUB_CATEGORY']}: "
                f"계약 {fmt_int(r['CONTRACT_COUNT'])}건, 완료율 {r['PAYEND_RATE_PCT']}%"
            )

    # 채널 성과
    if not v04_channel.empty:
        parts.append("\n=== 채널별 성과 (2025) ===")
        for _, r in v04_channel.head(8).iterrows():
            parts.append(
                f"  {r['MAIN_CATEGORY_NAME']} {r['RECEIVE_PATH_NAME']}/{r['INFLOW_PATH_NAME']}: "
                f"계약 {fmt_int(r['CONTRACT_COUNT'])}건, 완료율 {r['PAYEND_RATE_PCT']}%"
            )

    # 콜센터
    if not v11_cvr.empty:
        parts.append("\n=== 콜센터 전환율 (2025) ===")
        for _, r in v11_cvr.iterrows():
            parts.append(
                f"  {r['MAIN_CATEGORY_NAME']} {r['DIVISION_NAME']}: "
                f"전환율 {r['CALL_CVR_PCT']}%, 연결 계약 {fmt_int(r['LINKED_CONTRACTS'])}건"
            )

    # 외부 신호
    ext_parts = []
    if not weather_df.empty and "AVG_TEMP_C" in weather_df.columns:
        max_temp = weather_df["AVG_TEMP_C"].max()
        if max_temp >= 30:
            ext_parts.append(f"폭염 예보 (최고 {max_temp:.0f}°C)")
        if "PRECIPITATION_MM" in weather_df.columns and weather_df["PRECIPITATION_MM"].max() >= 20:
            ext_parts.append("강우 예보")
    if not holiday_df.empty and "IS_HOLIDAY" in holiday_df.columns:
        upcoming = holiday_df[holiday_df["IS_HOLIDAY"] == True]
        if not upcoming.empty and "HOLIDAY_NAME" in upcoming.columns:
            ext_parts.append(f"공휴일 임박: {', '.join(upcoming['HOLIDAY_NAME'].head(3).tolist())}")
    if not ext_news.empty:
        kws = ext_news["KEYWORD_GROUP"].dropna().unique()[:5]
        if len(kws) > 0:
            ext_parts.append(f"뉴스 키워드: {', '.join(kws)}")

    if ext_parts:
        parts.append("\n=== 외부 신호 ===")
        for p in ext_parts:
            parts.append(f"  {p}")

    return "\n".join(parts)


def build_data_facts_compact(
    v01_reg: pd.DataFrame,
    v06_product: pd.DataFrame,
    v04_channel: pd.DataFrame,
    v11_cvr: pd.DataFrame,
) -> str:
    """AI 질의용 경량 컨텍스트 — 핵심 지표만 압축 (data_facts의 1/3 크기)"""
    parts = []

    # 인터넷/렌탈 전국 요약만
    if not v01_reg.empty:
        parts.append("=== 카테고리 요약 (2025) ===")
        for cat in ["인터넷", "렌탈"]:
            cat_df = v01_reg[v01_reg["MAIN_CATEGORY_NAME"] == cat]
            if cat_df.empty:
                continue
            total_c = int(cat_df["CONTRACT_COUNT"].sum())
            total_p = int(cat_df["PAYEND_COUNT"].sum())
            total_l = int(cat_df["LOSS_COUNT"].sum())
            rate = total_p / total_c * 100 if total_c else 0
            parts.append(f"  {cat}: 계약 {total_c:,}건, 완료율 {rate:.1f}%, 미완료 {total_l:,}건")
        # 하위 3개 지역만
        bottom3 = v01_reg.sort_values("PAYEND_RATE_PCT").head(3)
        parts.append("  완료율 하위 지역: " + ", ".join(
            f"{r['REGION']} {r['MAIN_CATEGORY_NAME']}({r['PAYEND_RATE_PCT']}%)"
            for _, r in bottom3.iterrows()
        ))

    # 렌탈 상품 상위/하위 3개만
    if not v06_product.empty:
        parts.append("=== 렌탈 상품 요약 ===")
        top3 = v06_product.head(3)
        bot3 = v06_product.sort_values("PAYEND_RATE_PCT").head(3)
        parts.append("  계약 多: " + ", ".join(
            f"{r['RENTAL_SUB_CATEGORY']}({fmt_int(r['CONTRACT_COUNT'])}건/{r['PAYEND_RATE_PCT']}%)"
            for _, r in top3.iterrows()
        ))
        parts.append("  완료율 低: " + ", ".join(
            f"{r['RENTAL_SUB_CATEGORY']}({r['PAYEND_RATE_PCT']}%)"
            for _, r in bot3.iterrows()
        ))

    # 채널 상위 3개만
    if not v04_channel.empty:
        parts.append("=== 채널 TOP 3 ===")
        for _, r in v04_channel.head(3).iterrows():
            parts.append(
                f"  {r['MAIN_CATEGORY_NAME']} {r['RECEIVE_PATH_NAME']}: "
                f"{fmt_int(r['CONTRACT_COUNT'])}건, 완료율 {r['PAYEND_RATE_PCT']}%"
            )

    # 콜센터 요약
    if not v11_cvr.empty:
        parts.append("=== 콜센터 요약 ===")
        for _, r in v11_cvr.head(4).iterrows():
            parts.append(f"  {r['MAIN_CATEGORY_NAME']} {r['DIVISION_NAME']}: 전환율 {r['CALL_CVR_PCT']}%")

    return "\n".join(parts)


def build_trend_context(v01_monthly: pd.DataFrame, v01_qtr: pd.DataFrame, ref_date_str: str) -> str:
    """기준일 기준 최근 추세 분석 — AI가 '방향성'과 '긴급도'를 판단할 수 있도록"""
    ref_dt = pd.Timestamp(ref_date_str)
    parts = []

    if v01_monthly.empty:
        return ""

    mo = v01_monthly.copy()
    mo["YEAR_MONTH"] = pd.to_datetime(mo["YEAR_MONTH"])
    # 기준일 이전 데이터만
    mo = mo[mo["YEAR_MONTH"] < ref_dt]
    if mo.empty:
        return ""

    parts.append("=== 최근 월별 추이 (기준일 직전 6개월) ===")
    recent_months = sorted(mo["YEAR_MONTH"].unique())[-6:]

    for cat in ["인터넷", "렌탈"]:
        cat_mo = mo[(mo["MAIN_CATEGORY_NAME"] == cat) & (mo["YEAR_MONTH"].isin(recent_months))]
        cat_mo = cat_mo.sort_values("YEAR_MONTH")
        if cat_mo.empty:
            continue

        rates = []
        for _, r in cat_mo.iterrows():
            ym = r["YEAR_MONTH"].strftime("%m월")
            cc = int(r["CONTRACT_COUNT"])
            rate = float(r["PAYEND_RATE"]) * 100 if "PAYEND_RATE" in r.index and pd.notna(r["PAYEND_RATE"]) else (
                int(r["PAYEND_COUNT"]) / cc * 100 if cc > 0 else 0
            )
            rates.append({"month": ym, "rate": rate, "contracts": cc, "loss": int(r["LOSS_COUNT"])})

        if len(rates) >= 2:
            first_rate = rates[0]["rate"]
            last_rate = rates[-1]["rate"]
            trend_chg = last_rate - first_rate
            # 최근 3개월 평균 vs 이전 3개월 평균
            if len(rates) >= 4:
                recent_avg = sum(r["rate"] for r in rates[-3:]) / 3
                older_avg = sum(r["rate"] for r in rates[:-3]) / max(len(rates) - 3, 1)
                momentum = recent_avg - older_avg
                if momentum < -1:
                    trend_word = "악화 가속"
                elif momentum < 0:
                    trend_word = "소폭 하락"
                elif momentum < 1:
                    trend_word = "소폭 개선"
                else:
                    trend_word = "개선 가속"
            else:
                trend_word = "상승" if trend_chg > 0 else "하락"
                momentum = trend_chg

            rate_str = " → ".join([f"{r['month']} {r['rate']:.1f}%" for r in rates])
            parts.append(f"  {cat} 완료율 추이: {rate_str}")
            parts.append(f"    방향: {trend_word} ({trend_chg:+.1f}%p), 최근 모멘텀: {momentum:+.1f}%p")

            # 최근 달 이상 징후
            if len(rates) >= 2:
                last = rates[-1]
                prev = rates[-2]
                mom_chg = last["rate"] - prev["rate"]
                if abs(mom_chg) >= 2:
                    parts.append(f"    ⚠ 직전월 대비 {mom_chg:+.1f}%p {'급락' if mom_chg < 0 else '급등'} (주의)")

    # 분기별 추세 요약
    if not v01_qtr.empty:
        parts.append("\n=== 분기별 추세 ===")
        for cat in ["인터넷", "렌탈"]:
            cat_q = v01_qtr[v01_qtr["MAIN_CATEGORY_NAME"] == cat].sort_values("QTR")
            if len(cat_q) >= 2:
                last_q = cat_q.iloc[-1]
                prev_q = cat_q.iloc[-2]
                q_chg = float(last_q["PAYEND_RATE_PCT"]) - float(prev_q["PAYEND_RATE_PCT"])
                parts.append(
                    f"  {cat}: {prev_q['QTR']} {prev_q['PAYEND_RATE_PCT']}% → "
                    f"{last_q['QTR']} {last_q['PAYEND_RATE_PCT']}% ({q_chg:+.1f}%p)"
                )

    return "\n".join(parts)


# ══════════════════════════════════════════════════════════════════════════════
# [2층] AI 전략 프레이밍 — Cortex AI가 비즈니스 관점으로 재구성
# ══════════════════════════════════════════════════════════════════════════════

STRATEGY_PROMPT_TEMPLATE = """당신은 통신·렌탈 플랫폼 '아정당'의 CMO(최고마케팅책임자) 보좌관입니다.
아래 데이터를 분석하고, CEO와 마케팅 담당자가 지금 가장 먼저 실행해야 할 전략 5개를 도출하세요.

[중요: 추세 반영 원칙]
- 최근 월별 추이의 '방향'과 '모멘텀'을 반드시 반영하세요.
- 완료율이 하락 추세인 영역은 긴급도를 높이고, 개선 추세인 영역은 성장기회로 분류하세요.
- 직전월 대비 급변(±2%p 이상)이 있으면 최우선으로 다루세요.
- 오래된 데이터보다 최근 3개월 데이터에 더 높은 가중치를 두세요.

[규칙]
1. 각 전략은 반드시 아래 5가지 비즈니스 카테고리 중 하나로 분류하세요:
   - 매출긴급: 당장 매출 손실이 발생하고 있어 즉시 조치가 필요한 건
   - 이탈방지: 고객이 중도 이탈하고 있어 리텐션 조치가 필요한 건
   - 성장기회: 데이터상 성과가 좋아서 더 투자하면 성장할 수 있는 건
   - 효율개선: 채널·프로세스 최적화로 같은 비용으로 더 나은 결과를 얻을 수 있는 건
   - 외부대응: 날씨·정책·경쟁 등 외부 환경 변화에 선제 대응해야 하는 건

2. 각 전략은 아래 형식으로 출력하세요 (5개, 우선순위순):
전략1:
카테고리: (위 5가지 중 하나)
제목: (20자 이내, 마케터가 바로 이해할 수 있는 액션 중심)
대상: (지역, 상품, 채널 등 구체적 대상)
긴급도: (상/중/하)
핵심근거: (데이터 수치를 인용한 1~2문장)
실행방안: (마케터가 바로 실행할 수 있는 구체적 액션 2~3문장)
기대효과: (정량적 개선 기대치 1문장)

[현재 시점 & 연간 실적]
{strategy_context}

[최근 추세 (월별/분기별)]
{trend_context}

[상세 데이터]
{data_facts}"""


@st.cache_data(ttl=300, show_spinner=False)
def cortex_generate_strategies(data_facts: str, strategy_context: str, trend_context: str) -> str:
    """Cortex AI가 추세 데이터를 포함하여 전략을 생성"""
    prompt = STRATEGY_PROMPT_TEMPLATE.format(
        data_facts=data_facts,
        strategy_context=strategy_context,
        trend_context=trend_context,
    )
    return cortex_summarize(prompt)


def parse_ai_strategies(ai_text: str) -> list[dict]:
    """AI 응답을 파싱하여 전략 리스트로 변환 — 다양한 Cortex 출력 형식 대응"""
    import re
    strategies = []
    current = {}

    for line in ai_text.split("\n"):
        # 마크다운/리스트 기호 정리: **볼드**, -, *, #, 번호 등 제거
        cleaned = line.strip()
        cleaned = re.sub(r'^[#*\-\s]*', '', cleaned)  # 앞쪽 마크다운 기호
        cleaned = re.sub(r'\*\*', '', cleaned)          # 볼드 마크다운
        cleaned = cleaned.strip()
        if not cleaned:
            continue

        # 전략 구분자 감지: "전략1:", "전략 1:", "1. 전략:", "전략 1." 등
        if re.match(r'^(전략\s*\d|(\d+)\s*[.:\)]\s*전략)', cleaned):
            if current and any(k in current for k in ["title", "category"]):
                strategies.append(current)
            current = {}
            continue

        # 필드 매칭 — 콜론 앞뒤 공백, 마크다운 허용
        for patterns, field in [
            (["카테고리"], "category"),
            (["제목"], "title"),
            (["대상"], "target"),
            (["긴급도"], "urgency"),
            (["핵심근거", "핵심 근거", "근거"], "evidence"),
            (["실행방안", "실행 방안", "액션"], "action"),
            (["기대효과", "기대 효과", "효과"], "impact"),
        ]:
            matched = False
            for pat in patterns:
                # "카테고리:", "카테고리 :", "- 카테고리:" 등 모두 매칭
                m = re.match(rf'^{pat}\s*[:：]\s*(.*)', cleaned)
                if m:
                    current[field] = m.group(1).strip()
                    matched = True
                    break
            if matched:
                break

    if current and any(k in current for k in ["title", "category"]):
        strategies.append(current)

    return strategies


# ══════════════════════════════════════════════════════════════════════════════
# 시뮬레이션 시나리오 — 앱 내 직접 정의
# ══════════════════════════════════════════════════════════════════════════════

BUILT_IN_SCENARIOS = [
    {
        "SCENARIO_NAME": "폭염으로 에어컨 문의 폭증",
        "DESCRIPTION": "여름철 폭염(35도 이상 지속)으로 에어컨 신규 계약이 급증하지만, "
                       "설치 기사 부족으로 완료율이 떨어지는 상황",
        "TARGET_CATEGORY": "렌탈",
        "TARGET_PRODUCT": "에어컨",
        "CONTRACT_MULTIPLIER": 1.4,
        "COMPLETION_RATE_DELTA_PCT": -5.0,
        "ICON": "🌡️",
    },
    {
        "SCENARIO_NAME": "장마 시즌 설치 지연",
        "DESCRIPTION": "장마·집중호우로 가전 설치 작업이 지연되고, "
                       "정수기·공기청정기 같은 실내 제품 상담은 증가하는 상황",
        "TARGET_CATEGORY": "렌탈",
        "TARGET_PRODUCT": "정수기",
        "CONTRACT_MULTIPLIER": 1.15,
        "COMPLETION_RATE_DELTA_PCT": -8.0,
        "ICON": "🌧️",
    },
    {
        "SCENARIO_NAME": "추석 연휴 상담 적체",
        "DESCRIPTION": "명절 연휴 전후로 콜센터 상담 적체가 발생하고, "
                       "연휴 후 계약 처리 지연으로 완료율이 일시 하락하는 상황",
        "TARGET_CATEGORY": "인터넷",
        "TARGET_PRODUCT": None,
        "CONTRACT_MULTIPLIER": 0.7,
        "COMPLETION_RATE_DELTA_PCT": -3.0,
        "ICON": "🎑",
    },
    {
        "SCENARIO_NAME": "이사 시즌 인터넷 수요 급증",
        "DESCRIPTION": "봄/가을 이사 시즌에 인터넷 신규 가입이 크게 늘어나고, "
                       "경쟁사 대비 빠른 개통이 핵심 경쟁력이 되는 상황",
        "TARGET_CATEGORY": "인터넷",
        "TARGET_PRODUCT": None,
        "CONTRACT_MULTIPLIER": 1.3,
        "COMPLETION_RATE_DELTA_PCT": -2.0,
        "ICON": "🏠",
    },
    {
        "SCENARIO_NAME": "정부 에너지 효율 정책 발표",
        "DESCRIPTION": "정부의 에너지 효율 가전 보조금 정책 발표로 "
                       "에어컨·냉장고 등 고효율 렌탈 제품 수요가 일시적으로 급증하는 상황",
        "TARGET_CATEGORY": "렌탈",
        "TARGET_PRODUCT": "에어컨",
        "CONTRACT_MULTIPLIER": 1.25,
        "COMPLETION_RATE_DELTA_PCT": 0.0,
        "ICON": "📋",
    },
    {
        "SCENARIO_NAME": "경쟁사 공격적 프로모션",
        "DESCRIPTION": "경쟁 통신사가 인터넷 결합 할인·렌탈 무료 체험 등 "
                       "공격적 프로모션을 진행하여 신규 유입이 줄고 기존 고객 이탈 위험이 높아지는 상황",
        "TARGET_CATEGORY": "인터넷",
        "TARGET_PRODUCT": None,
        "CONTRACT_MULTIPLIER": 0.85,
        "COMPLETION_RATE_DELTA_PCT": -4.0,
        "ICON": "⚔️",
    },
]


# ══════════════════════════════════════════════════════════════════════════════
# 사이드바 (데이터 로드보다 먼저 — 기준일이 외부 데이터 쿼리에 영향)
# ══════════════════════════════════════════════════════════════════════════════
from datetime import date as _date

with st.sidebar:
    st.header("필터 & 설정")

    # 기준일 선택 — "오늘이 이 날이라면?" 시나리오
    dashboard_date = st.date_input(
        "📅 오늘 날짜 (기준일)",
        value=_date(2026, 1, 5),
        min_value=_date(2025, 1, 1),
        max_value=_date(2026, 1, 31),
        help="이 날짜를 '오늘'로 간주하여 외부 신호와 전략을 도출합니다. 날짜를 바꾸면 전략이 달라집니다.",
        key="dashboard_date",
    )
    DASHBOARD_DATE = dashboard_date.strftime("%Y-%m-%d")
    DASHBOARD_DATE_LABEL = dashboard_date.strftime("%Y년 %m월 %d일").replace(" 0", " ")

    # 날짜 변경 감지 → AI 캐시 자동 클리어 (session_state + cache_data 모두)
    _prev_date = st.session_state.get("_prev_dashboard_date")
    if _prev_date is not None and _prev_date != DASHBOARD_DATE:
        st.cache_data.clear()
        for k in list(st.session_state.keys()):
            if k.startswith(("data_facts_", "trend_ctx_", "strategies_",
                             "tab1_ai_", "diag1_ai_", "diag2_ai_", "diag3_ai_",
                             "classify_ai_", "sc_ai_")):
                del st.session_state[k]
    st.session_state["_prev_dashboard_date"] = DASHBOARD_DATE

    if st.button("데이터 새로고침", use_container_width=True):
        st.cache_data.clear()
        # session_state 캐시도 클리어
        for k in list(st.session_state.keys()):
            if k.startswith(("data_facts_", "trend_ctx_", "strategies_", "cortex_ok",
                             "tab1_ai_", "diag1_ai_", "diag2_ai_", "diag3_ai_",
                             "classify_ai_", "sc_ai_")):
                del st.session_state[k]
        st.rerun()

    st.markdown("---")

# ══════════════════════════════════════════════════════════════════════════════
# 데이터 불러오기
# ══════════════════════════════════════════════════════════════════════════════
with st.spinner("Snowflake에서 데이터를 불러오는 중..."):
    v01_yr, v01_monthly, v01_qtr, v01_reg = _load_v01_all()
    v03_funnel = load_v03_funnel()
    v06_product, v06_region = _load_v06_all()
    v04_channel = load_v04_channel()
    v09_call = load_v09_call_stats()
    v11_cvr = load_v11_call_cvr()

    ext_news = load_external_news()
    weather_df = load_weather(DASHBOARD_DATE)
    holiday_df = load_holidays(DASHBOARD_DATE)

    # [1층] 데이터 팩트 — session_state에 캐시 (DataFrame 해싱 회피)
    _facts_key = f"data_facts_{DASHBOARD_DATE}"
    if _facts_key not in st.session_state:
        st.session_state[_facts_key] = build_data_facts(
            v01_reg, v06_product, v06_region, v04_channel, v11_cvr,
            ext_news, weather_df, holiday_df,
        )
    data_facts = st.session_state[_facts_key]

    _compact_key = f"data_facts_compact_{DASHBOARD_DATE}"
    if _compact_key not in st.session_state:
        st.session_state[_compact_key] = build_data_facts_compact(
            v01_reg, v06_product, v04_channel, v11_cvr,
        )
    data_facts_compact = st.session_state[_compact_key]

    # 전략 도출용 컨텍스트 — 기준일 기준, 해당 시점까지의 실적 + 전년 대비
    _ref_year = dashboard_date.year
    _data_year = _ref_year - 1 if dashboard_date.month == 1 else _ref_year  # 1월이면 전년도 실적 기준
    _prev_year = _data_year - 1
    _s_data = v01_yr[v01_yr["YR"] == _data_year]
    _s_prev = v01_yr[v01_yr["YR"] == _prev_year]
    _sc_parts = [f"현재 시점: {DASHBOARD_DATE_LABEL}. {_data_year}년 실적 기준 ({_prev_year}년 대비 추세 포함)"]
    for cat in ["인터넷", "렌탈"]:
        c_data = _s_data[_s_data["MAIN_CATEGORY_NAME"] == cat]
        c_prev = _s_prev[_s_prev["MAIN_CATEGORY_NAME"] == cat]
        if not c_data.empty:
            cc = int(c_data["CONTRACT_COUNT"].iloc[0])
            pp = int(c_data["PAYEND_COUNT"].iloc[0])
            ll = int(c_data["LOSS_COUNT"].iloc[0])
            rr = float(c_data["PAYEND_RATE"].iloc[0]) * 100
            line = f"{cat}: 계약 {cc:,}건, 완료 {pp:,}건, 완료율 {rr:.1f}%, 미완료 {ll:,}건"
            if not c_prev.empty:
                rr_prev = float(c_prev["PAYEND_RATE"].iloc[0]) * 100
                cc_prev = int(c_prev["CONTRACT_COUNT"].iloc[0])
                chg_c = (cc - cc_prev) / cc_prev * 100 if cc_prev else 0
                chg_r = rr - rr_prev
                line += f" (전년 대비 계약 {chg_c:+.1f}%, 완료율 {chg_r:+.1f}%p)"
            _sc_parts.append(line)
    strategy_context = "\n".join(_sc_parts)

    # 추세 컨텍스트 — session_state에 캐시
    _trend_key = f"trend_ctx_{DASHBOARD_DATE}"
    if _trend_key not in st.session_state:
        st.session_state[_trend_key] = build_trend_context(v01_monthly, v01_qtr, DASHBOARD_DATE)
    trend_context = st.session_state[_trend_key]

cortex_ready = cortex_available()

# ══════════════════════════════════════════════════════════════════════════════
# 사이드바 나머지 (데이터 로드 후)
# ══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    # 지역 필터
    all_regions = sorted(v01_reg["REGION"].dropna().unique().tolist()) if not v01_reg.empty else []
    sel_regions = st.multiselect("지역 필터", all_regions, default=all_regions, key="region_filter")

    st.markdown("---")
    st.markdown("##### Snowflake 기술 활용")
    techs = [
        ("Snowpark", True, "데이터 처리"),
        ("Streamlit", True, "대시보드 UI"),
        ("Cortex AI", cortex_ready, "전략·진단·시나리오 전체"),
        ("Marketplace 데이터", True, "아정당 데이터"),
        ("Snowflake Forecast", True, "수요 예측"),
    ]
    for name, ok, detail in techs:
        st.markdown(f"{'✅' if ok else '⬜'} **{name}** <small style='color:#888;'>({detail})</small>", unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("##### 데이터 최신성")
    _freshness = load_freshness()
    if not _freshness.empty:
        for _, row in _freshness.iterrows():
            st.caption(f"{row['NAME']}: {row['UPDATED_AT']}")


# ══════════════════════════════════════════════════════════════════════════════
# 헤더
# ══════════════════════════════════════════════════════════════════════════════
st.title("아정당 운영 추천 대시보드")
st.caption(
    f"오늘: **{DASHBOARD_DATE_LABEL}** | "
    "내부 계약 데이터(V01~V11)와 외부 신호(날씨·공휴일·뉴스)를 종합하여 전략을 도출합니다. "
    "사이드바에서 날짜를 변경하면 해당 시점의 전략이 달라집니다."
)

# ── 기간 선택 + 핵심 현황 분석 ──
v01_monthly["YEAR_MONTH"] = pd.to_datetime(v01_monthly["YEAR_MONTH"])
all_months = sorted(v01_monthly["YEAR_MONTH"].dt.to_period("M").unique())

period_cols = st.columns([1, 1, 2])
with period_cols[0]:
    start_options = [str(m) for m in all_months]
    # 기본값: 기준일 기준 최근 12개월 시작점
    _ref_ym = dashboard_date.strftime("%Y-%m")
    default_start_target = f"{dashboard_date.year - 1}-{dashboard_date.month:02d}"
    default_start_idx = next(
        (i for i, m in enumerate(start_options) if m >= default_start_target), max(0, len(start_options) - 12)
    )
    sel_start = st.selectbox("시작월", start_options, index=default_start_idx, key="hero_start")
with period_cols[1]:
    end_options = [str(m) for m in all_months]
    # 기본값: 기준일 직전 월 (또는 마지막 가용 월)
    default_end_target = f"{dashboard_date.year}-{dashboard_date.month:02d}" if dashboard_date.month > 1 else f"{dashboard_date.year - 1}-12"
    default_end_idx = next(
        (i for i, m in enumerate(end_options) if m >= default_end_target), len(end_options) - 1
    )
    default_end_idx = min(default_end_idx, len(end_options) - 1)
    sel_end = st.selectbox("종료월", end_options, index=default_end_idx, key="hero_end")
with period_cols[2]:
    badges = []
    if not weather_df.empty and "AVG_TEMP_C" in weather_df.columns:
        if weather_df["AVG_TEMP_C"].max() >= 30:
            badges.append("폭염 예보")
        elif "PRECIPITATION_MM" in weather_df.columns and weather_df["PRECIPITATION_MM"].max() >= 20:
            badges.append("강우 예보")
    if not holiday_df.empty and "IS_HOLIDAY" in holiday_df.columns and holiday_df["IS_HOLIDAY"].any():
        badges.append("공휴일 임박")
    if not ext_news.empty:
        badges.append(f"뉴스 {len(ext_news)}건")
    badge_html = " ".join([f'<span class="pill">{b}</span>' for b in badges]) if badges else '<span class="pill">외부 특이 신호 없음</span>'
    st.markdown(f'**외부 신호** {badge_html}', unsafe_allow_html=True)

# 선택 기간 데이터 필터링
sel_start_dt = pd.Period(sel_start, freq="M").to_timestamp()
sel_end_dt = pd.Period(sel_end, freq="M").to_timestamp()
if sel_start_dt > sel_end_dt:
    sel_start_dt, sel_end_dt = sel_end_dt, sel_start_dt

period_data = v01_monthly[
    (v01_monthly["YEAR_MONTH"] >= sel_start_dt) & (v01_monthly["YEAR_MONTH"] <= sel_end_dt)
]
period_label = f"{sel_start} ~ {sel_end}"

# 선택 기간 KPI
total_contracts = int(period_data["CONTRACT_COUNT"].sum())
total_payends = int(period_data["PAYEND_COUNT"].sum())
total_loss = int(period_data["LOSS_COUNT"].sum())
total_rate = total_payends / total_contracts * 100 if total_contracts else 0

inet = period_data[period_data["MAIN_CATEGORY_NAME"] == "인터넷"]
rental = period_data[period_data["MAIN_CATEGORY_NAME"] == "렌탈"]
inet_share = float(inet["CONTRACT_COUNT"].sum()) / total_contracts * 100 if total_contracts else 0
rental_share = float(rental["CONTRACT_COUNT"].sum()) / total_contracts * 100 if total_contracts else 0
inet_rate_val = float(inet["PAYEND_COUNT"].sum()) / float(inet["CONTRACT_COUNT"].sum()) * 100 if float(inet["CONTRACT_COUNT"].sum()) else 0
rental_rate_val = float(rental["PAYEND_COUNT"].sum()) / float(rental["CONTRACT_COUNT"].sum()) * 100 if float(rental["CONTRACT_COUNT"].sum()) else 0

# 비교 기간 KPI
period_len = (sel_end_dt - sel_start_dt).days + 31
prev_end_dt = sel_start_dt - pd.DateOffset(months=1)
prev_start_dt = prev_end_dt - pd.DateOffset(days=period_len - 31)
prev_data = v01_monthly[
    (v01_monthly["YEAR_MONTH"] >= prev_start_dt) & (v01_monthly["YEAR_MONTH"] <= prev_end_dt)
]
prev_contracts = int(prev_data["CONTRACT_COUNT"].sum()) if not prev_data.empty else 0
prev_payends = int(prev_data["PAYEND_COUNT"].sum()) if not prev_data.empty else 0
prev_rate = prev_payends / prev_contracts * 100 if prev_contracts else 0

# 기간 컨텍스트 (AI 프롬프트용)
period_context = (
    f"기간: {period_label}\n"
    f"총 계약 {fmt_int(total_contracts)}건, 완료 {fmt_int(total_payends)}건, 완료율 {total_rate:.1f}%\n"
    f"미완료 {fmt_int(total_loss)}건\n"
    f"인터넷 비중 {inet_share:.1f}% (완료율 {inet_rate_val:.1f}%), 렌탈 비중 {rental_share:.1f}% (완료율 {rental_rate_val:.1f}%)"
)
if prev_contracts > 0:
    contract_chg = (total_contracts - prev_contracts) / prev_contracts * 100
    rate_chg = total_rate - prev_rate
    period_context += f"\n직전 동기 대비 계약 {contract_chg:+.1f}%, 완료율 {rate_chg:+.1f}%p"

# 헤드라인 — 데이터 기반 즉시 생성 (Cortex 호출 제거 → 0초)
hero_title = ""
hero_detail = ""

if prev_contracts > 0:
    contract_chg = (total_contracts - prev_contracts) / prev_contracts * 100
    rate_chg = total_rate - prev_rate
    hero_detail = f"직전 동기 대비 계약 {contract_chg:+.1f}%, 완료율 {rate_chg:+.1f}%p 변화. "

    if contract_chg > 0 and rate_chg < 0:
        hero_title = f"계약 {contract_chg:+.1f}% 증가, 완료율은 {rate_chg:+.1f}%p 하락 — 품질 관리 필요"
    elif contract_chg > 0 and rate_chg >= 0:
        hero_title = f"계약 {contract_chg:+.1f}% 증가, 완료율도 {rate_chg:+.1f}%p 개선 — 성장 가속"
    elif contract_chg <= 0 and rate_chg < 0:
        hero_title = f"계약 {contract_chg:+.1f}% 감소, 완료율도 {rate_chg:+.1f}%p 하락 — 긴급 점검"
    else:
        hero_title = f"계약 {contract_chg:+.1f}% 감소, 완료율은 {rate_chg:+.1f}%p 개선 — 효율 집중"
else:
    hero_title = "선택 기간의 사업 현황입니다."
    hero_detail = ""

top_cats = period_data.groupby("MAIN_CATEGORY_NAME")["CONTRACT_COUNT"].sum().sort_values(ascending=False).head(2)
if len(top_cats) >= 2:
    c1_name, c1_cnt = top_cats.index[0], top_cats.iloc[0]
    c2_name, c2_cnt = top_cats.index[1], top_cats.iloc[1]
    cat_text = (
        f"{c1_name}({c1_cnt/total_contracts*100:.1f}%)과 "
        f"{c2_name}({c2_cnt/total_contracts*100:.1f}%)이 "
        f"전체의 {(c1_cnt+c2_cnt)/total_contracts*100:.1f}%를 차지합니다."
    )
else:
    cat_text = ""

st.markdown(
    f"""<div class="hero">
    <div class="hero-kicker">오늘: {DASHBOARD_DATE_LABEL} | {period_label} 핵심 현황</div>
    <div class="hero-title">{hero_title}</div>
    <div style="margin-top:12px;">
      <span class="hero-pill">시작된 계약 {fmt_int(total_contracts)}건</span>
      <span class="hero-pill">최종 완료 {fmt_int(total_payends)}건</span>
      <span class="hero-pill">완료율 {total_rate:.1f}%</span>
      <span class="hero-pill">끝까지 못 간 {fmt_int(total_loss)}건</span>
    </div>
    <div class="hero-text">
      {hero_detail}{cat_text}
    </div>
    </div>""",
    unsafe_allow_html=True,
)

# ── 정적 분류 함수 (데이터 기반, 즉시 표시용) ──
def _show_static_classification(filtered_v06):
    avg_loss = filtered_v06["LOSS_COUNT"].mean()
    avg_rate = filtered_v06["PAYEND_RATE_PCT"].mean()
    rate_threshold = avg_rate * 0.7

    volume_type = filtered_v06[
        (filtered_v06["LOSS_COUNT"] > avg_loss) & (filtered_v06["PAYEND_RATE_PCT"] >= rate_threshold)
    ].head(3)
    rate_type = filtered_v06[
        (filtered_v06["LOSS_COUNT"] <= avg_loss) & (filtered_v06["PAYEND_RATE_PCT"] < rate_threshold)
    ].head(3)
    mixed_type = filtered_v06[
        (filtered_v06["LOSS_COUNT"] > avg_loss) & (filtered_v06["PAYEND_RATE_PCT"] < rate_threshold)
    ].head(3)

    def _type_items(df):
        if df.empty:
            return "해당 없음"
        return "<br/>".join([
            f"{r['REGION']} {r['RENTAL_SUB_CATEGORY']} ({r['PAYEND_RATE_PCT']}%)"
            for _, r in df.iterrows()
        ])

    type_cols = st.columns(3)
    with type_cols[0]:
        st.markdown(
            f'<div class="mini-card"><div class="mini-card-title">매출 영향 大</div>'
            f'<div class="mini-card-note">{_type_items(volume_type)}<br/><br/>'
            f'비율은 평균 이상이지만 규모가 커서 매출 영향이 큼</div></div>',
            unsafe_allow_html=True,
        )
    with type_cols[1]:
        st.markdown(
            f'<div class="mini-card"><div class="mini-card-title">이탈 리스크</div>'
            f'<div class="mini-card-note">{_type_items(rate_type)}<br/><br/>'
            f'완료율이 평균({avg_rate:.1f}%)의 70% 미만으로 고객 이탈 심각</div></div>',
            unsafe_allow_html=True,
        )
    with type_cols[2]:
        st.markdown(
            f'<div class="mini-card"><div class="mini-card-title">긴급 조치 필요</div>'
            f'<div class="mini-card-note">{_type_items(mixed_type)}<br/><br/>'
            f'건수도 크고 비율도 낮아 최우선 점검 대상</div></div>',
            unsafe_allow_html=True,
        )


# ══════════════════════════════════════════════════════════════════════════════
# 탭
# ══════════════════════════════════════════════════════════════════════════════
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "1. 사업 현황",
    "2. 문제 진단",
    "3. 전략 추천",
    "4. 시뮬레이션",
    "5. AI 질의",
])

# ── Tab 1: 사업 현황 ─────────────────────────────────────────────────────────
with tab1:
    st.subheader(f"사업 현황: {period_label} 완료율 추이와 퍼널")
    if prev_contracts > 0:
        if total_rate < prev_rate:
            tab1_summary = f"선택 기간 계약 {fmt_int(total_contracts)}건, 완료율 {total_rate:.1f}%. 직전 동기({prev_rate:.1f}%) 대비 하락했습니다."
        else:
            tab1_summary = f"선택 기간 계약 {fmt_int(total_contracts)}건, 완료율이 {prev_rate:.1f}%에서 {total_rate:.1f}%로 개선되었습니다."
    else:
        tab1_summary = f"선택 기간 총 계약 {fmt_int(total_contracts)}건 중 {total_rate:.1f}%가 최종 완료되었습니다."
    st.markdown(f'<div class="summary-box">한 줄 요약: {tab1_summary}</div>', unsafe_allow_html=True)

    kpi_cols = st.columns(4)
    with kpi_cols[0]:
        metric_card("총 계약", fmt_int(total_contracts), f"{period_label}")
    with kpi_cols[1]:
        metric_card("최종 완료", fmt_int(total_payends), f"완료율 {total_rate:.1f}%")
    with kpi_cols[2]:
        metric_card("인터넷 완료율", f"{inet_rate_val:.1f}%", f"계약 {fmt_int(inet['CONTRACT_COUNT'].sum())}건")
    with kpi_cols[3]:
        metric_card("렌탈 완료율", f"{rental_rate_val:.1f}%", f"계약 {fmt_int(rental['CONTRACT_COUNT'].sum())}건")

    st.markdown("### 분기별 최종 완료율 추이")
    left, right = st.columns(2)
    with left:
        rate_chart = (
            alt.Chart(v01_qtr)
            .mark_line(point=True, strokeWidth=2.5)
            .encode(
                x=alt.X("QTR:N", title="분기", sort=None),
                y=alt.Y("PAYEND_RATE_PCT:Q", title="최종 완료율(%)", scale=alt.Scale(domain=[65, 80])),
                color=alt.Color("MAIN_CATEGORY_NAME:N", title="카테고리",
                                scale=alt.Scale(domain=["인터넷", "렌탈"], range=["#0f766e", "#c2410c"])),
                tooltip=["QTR", "MAIN_CATEGORY_NAME", "PAYEND_RATE_PCT", "CONTRACT_COUNT"],
            )
            .properties(height=320)
        )
        st.altair_chart(rate_chart, use_container_width=True)

    with right:
        loss_chart = (
            alt.Chart(v01_qtr)
            .mark_bar(cornerRadiusTopLeft=6, cornerRadiusTopRight=6)
            .encode(
                x=alt.X("QTR:N", title="분기", sort=None),
                y=alt.Y("LOSS_COUNT:Q", title="끝까지 못 간 수"),
                color=alt.Color("MAIN_CATEGORY_NAME:N", title="카테고리",
                                scale=alt.Scale(domain=["인터넷", "렌탈"], range=["#0f766e", "#c2410c"])),
                tooltip=["QTR", "MAIN_CATEGORY_NAME", "LOSS_COUNT", "CONTRACT_COUNT"],
            )
            .properties(height=320)
        )
        st.altair_chart(loss_chart, use_container_width=True)

    st.markdown("### 초반 단계 전환율 변화 (2024 vs 2025)")
    funnel_inet = v03_funnel[v03_funnel["MAIN_CATEGORY_NAME"].isin(["인터넷", "렌탈"])]
    if not funnel_inet.empty:
        funnel_cols = st.columns(2)
        for idx, cat in enumerate(["인터넷", "렌탈"]):
            cat_data = funnel_inet[funnel_inet["MAIN_CATEGORY_NAME"] == cat]
            if cat_data.empty:
                continue
            with funnel_cols[idx]:
                st.markdown(f"**{cat}**")
                st.dataframe(
                    cat_data[["YR", "TOTAL_TO_CONSULT_PCT", "TOTAL_TO_SUB_PCT", "SUB_TO_REG_PCT", "TOTAL_PAYEND_PCT"]].rename(columns={
                        "YR": "연도", "TOTAL_TO_CONSULT_PCT": "전체→상담(%)",
                        "TOTAL_TO_SUB_PCT": "전체→신청(%)", "SUB_TO_REG_PCT": "신청→등록(%)",
                        "TOTAL_PAYEND_PCT": "전체→완료(%)",
                    }),
                    use_container_width=True, hide_index=True,
                )

    # Cortex AI: 사업 현황 분석 — 버튼 클릭 시에만 호출
    if cortex_ready:
        with st.expander("🤖 Cortex AI: 사업 현황 종합 분석"):
            _tab1_ai_key = f"tab1_ai_{DASHBOARD_DATE}_{sel_start}_{sel_end}"
            if _tab1_ai_key in st.session_state:
                st.markdown(
                    f'<div class="ai-box"><div class="ai-box-title">Cortex AI 사업 현황 분석</div>{st.session_state[_tab1_ai_key]}</div>',
                    unsafe_allow_html=True,
                )
            elif st.button("분석 시작", key="btn_tab1_ai"):
                tab1_ai_parts = [
                    f"기간: {period_label}",
                    f"총 계약 {fmt_int(total_contracts)}건, 완료율 {total_rate:.1f}%",
                    f"인터넷 완료율 {inet_rate_val:.1f}%, 렌탈 완료율 {rental_rate_val:.1f}%",
                ]
                if prev_contracts > 0:
                    tab1_ai_parts.append(f"직전 동기 대비 완료율 변화: {total_rate - prev_rate:+.1f}%p")
                if not v03_funnel.empty:
                    fn25 = v03_funnel[v03_funnel["YR"] == 2025]
                    for _, fr in fn25.iterrows():
                        tab1_ai_parts.append(
                            f"{fr['MAIN_CATEGORY_NAME']}: 전체→상담 {fr['TOTAL_TO_CONSULT_PCT']}%, "
                            f"전체→신청 {fr['TOTAL_TO_SUB_PCT']}%, 신청→등록 {fr['SUB_TO_REG_PCT']}%"
                        )
                tab1_ai_ctx = "\n".join(tab1_ai_parts)
                tab1_prompt = (
                    "아래 아정당의 사업 데이터를 분석하여:\n"
                    "1) 완료율 변동의 근본 원인 3가지 — 반드시 데이터 수치(계약 수, 완료율, 전환율 등)를 인용\n"
                    "2) 각 원인에 대한 구체적 개선 방향 — 개선 시 예상되는 정량적 효과 포함\n"
                    "3) 가장 시급하게 조치해야 할 1가지와 미조치 시 예상 손실 규모\n"
                    "[필수] 모든 분석에 구체적 숫자(건수, %, %p 변화)를 포함하세요. 일반론 금지.\n\n"
                    + tab1_ai_ctx + "\n\n" + data_facts_compact
                )
                with st.spinner("Cortex AI 분석 중..."):
                    tab1_analysis = cortex_summarize(tab1_prompt)
                if tab1_analysis:
                    st.session_state[_tab1_ai_key] = tab1_analysis
                    st.rerun()
                else:
                    st.info("Cortex AI 분석을 생성하지 못했습니다.")


# ── Tab 2: 문제 진단 ─────────────────────────────────────────────────────────
with tab2:
    st.subheader("문제 진단: 어디서 고객이 멈추는가")

    diag_tab1, diag_tab2, diag_tab3 = st.tabs(["렌탈 상품별", "지역×상품 후보", "채널·콜센터"])

    with diag_tab1:
        st.markdown("### 렌탈 세부상품별 손실 현황 (2025)")
        if not v06_product.empty:
            left, right = st.columns(2)
            with left:
                st.markdown("**건수 기준 (정수기 제외)**")
                v06_excl = v06_product[v06_product["RENTAL_SUB_CATEGORY"] != "정수기"].head(8)
                loss_bar = (
                    alt.Chart(v06_excl)
                    .mark_bar(cornerRadiusTopRight=8, cornerRadiusBottomRight=8, color="#0f766e")
                    .encode(
                        x=alt.X("LOSS_COUNT:Q", title="끝까지 못 간 수"),
                        y=alt.Y("RENTAL_SUB_CATEGORY:N", sort="-x", title="상품"),
                        tooltip=["RENTAL_SUB_CATEGORY", "CONTRACT_COUNT", "LOSS_COUNT", "PAYEND_RATE_PCT"],
                    )
                    .properties(height=280)
                )
                st.altair_chart(loss_bar, use_container_width=True)

            with right:
                st.markdown("**완료율 기준 (하위)**")
                v06_low = v06_product.sort_values("PAYEND_RATE_PCT").head(8)
                rate_bar = (
                    alt.Chart(v06_low)
                    .mark_bar(cornerRadiusTopRight=8, cornerRadiusBottomRight=8, color="#c2410c")
                    .encode(
                        x=alt.X("PAYEND_RATE_PCT:Q", title="최종 완료율(%)"),
                        y=alt.Y("RENTAL_SUB_CATEGORY:N", sort="x", title="상품"),
                        tooltip=["RENTAL_SUB_CATEGORY", "CONTRACT_COUNT", "PAYEND_RATE_PCT"],
                    )
                    .properties(height=280)
                )
                st.altair_chart(rate_bar, use_container_width=True)

            st.dataframe(
                v06_product.rename(columns={
                    "RENTAL_SUB_CATEGORY": "상품", "CONTRACT_COUNT": "계약 수",
                    "PAYEND_COUNT": "완료 수", "LOSS_COUNT": "손실 수", "PAYEND_RATE_PCT": "완료율(%)",
                }),
                use_container_width=True, hide_index=True,
            )

            if cortex_ready:
                with st.expander("🤖 Cortex AI: 렌탈 상품 완료율 원인 분석"):
                    _diag1_key = f"diag1_ai_{DASHBOARD_DATE}"
                    if _diag1_key in st.session_state:
                        st.markdown(
                            f'<div class="ai-box"><div class="ai-box-title">Cortex AI 렌탈 상품 진단</div>{st.session_state[_diag1_key]}</div>',
                            unsafe_allow_html=True,
                        )
                    elif st.button("분석 시작", key="btn_diag1_ai"):
                        low5 = v06_product.sort_values("PAYEND_RATE_PCT").head(5)
                        product_ctx = "\n".join([
                            f"- {r['RENTAL_SUB_CATEGORY']}: 계약 {fmt_int(r['CONTRACT_COUNT'])}건, "
                            f"완료율 {r['PAYEND_RATE_PCT']}%, 손실 {fmt_int(r['LOSS_COUNT'])}건"
                            for _, r in low5.iterrows()
                        ])
                        diag1_prompt = (
                            "다음 렌탈 상품들의 완료율이 낮은 근본 원인을 분석하세요.\n\n"
                            "[필수 규칙]\n"
                            "- 각 상품의 데이터 수치(계약 수, 완료율, 손실 건수)를 반드시 인용하세요\n"
                            "- 상품별 특성(설치 난이도, 계절성, 가격대, 고객층)을 고려하세요\n"
                            "- 각 상품별 개선 시 예상 효과를 정량적으로 제시하세요 (예: 완료율 X%p 개선 → 약 Y건 추가 완료)\n"
                            "- 일반적 조언이 아닌, 이 데이터에서만 도출 가능한 인사이트를 제시하세요\n\n"
                            f"[렌탈 상품 데이터]\n{product_ctx}\n\n"
                            f"[전체 현황 참고]\n{strategy_context}"
                        )
                        with st.spinner("Cortex AI 분석 중..."):
                            diag1_result = cortex_summarize(diag1_prompt)
                        if diag1_result:
                            st.session_state[_diag1_key] = diag1_result
                            st.rerun()
                        else:
                            st.info("Cortex AI 분석을 생성하지 못했습니다.")

        st.caption("이 탭은 전국 합산 데이터이며, 사이드바 지역 필터가 적용되지 않습니다.")

    with diag_tab2:
        st.markdown("### 우선 점검 후보: 지역×렌탈 상품 (2025)")
        if not v06_region.empty:
            filtered_v06 = v06_region[v06_region["REGION"].isin(sel_regions)] if sel_regions else v06_region
            priority = filtered_v06.head(15)
            st.dataframe(
                priority.rename(columns={
                    "REGION": "지역", "RENTAL_SUB_CATEGORY": "상품",
                    "CONTRACT_COUNT": "계약 수", "PAYEND_COUNT": "완료 수",
                    "LOSS_COUNT": "손실 수", "PAYEND_RATE_PCT": "완료율(%)",
                }),
                use_container_width=True, hide_index=True,
            )

            # 문제 유형 분류 — 데이터 기반 정적 분류 즉시 표시
            st.markdown("### 문제 유형 분류")
            _show_static_classification(filtered_v06)

            # AI 심층 분류는 버튼 클릭 시에만 호출
            if cortex_ready:
                with st.expander("🤖 Cortex AI: 문제 유형 심층 분류"):
                    _classify_key = f"classify_ai_{DASHBOARD_DATE}"
                    if _classify_key in st.session_state:
                        st.markdown(
                            f'<div class="ai-box"><div class="ai-box-title">Cortex AI 문제 유형 심층 분류</div>{st.session_state[_classify_key]}</div>',
                            unsafe_allow_html=True,
                        )
                    elif st.button("분석 시작", key="btn_classify_ai"):
                        top10_ctx = "\n".join([
                            f"- {r['REGION']} {r['RENTAL_SUB_CATEGORY']}: "
                            f"계약 {fmt_int(r['CONTRACT_COUNT'])}건, 완료율 {r['PAYEND_RATE_PCT']}%, 손실 {fmt_int(r['LOSS_COUNT'])}건"
                            for _, r in filtered_v06.head(10).iterrows()
                        ])
                        classify_prompt = (
                            "아래 아정당 렌탈 사업의 지역×상품 데이터를 보고, "
                            "마케팅 담당자가 바로 이해할 수 있는 문제 유형으로 분류하세요.\n\n"
                            "반드시 아래 형식으로 3~4개 유형을 출력하세요:\n"
                            "유형1:\n이름: (비즈니스 용어로 10자 이내)\n해당항목: (지역 상품 나열, 최대 3개)\n"
                            "핵심수치: (해당 항목의 계약 수, 완료율, 손실 건수를 인용)\n"
                            "설명: (왜 이 유형으로 분류했는지 — 데이터 근거 포함 1~2문장)\n"
                            "예상효과: (개선 시 기대되는 정량적 변화)\n"
                            "대응방향: (마케터 액션 1문장)\n\n"
                            f"[데이터]\n{top10_ctx}\n\n[전체 현황 참고]\n{strategy_context}"
                        )
                        with st.spinner("Cortex AI가 문제를 분류하고 있습니다..."):
                            classify_result = cortex_summarize(classify_prompt)
                        if classify_result:
                            st.session_state[_classify_key] = classify_result
                            st.rerun()
                        else:
                            st.info("Cortex AI 분류를 생성하지 못했습니다.")

            if cortex_ready:
                with st.expander("🤖 Cortex AI: 우선 점검 구간 심층 분석"):
                    _diag2_key = f"diag2_ai_{DASHBOARD_DATE}"
                    if _diag2_key in st.session_state:
                        st.markdown(
                            f'<div class="ai-box"><div class="ai-box-title">Cortex AI 지역×상품 진단</div>{st.session_state[_diag2_key]}</div>',
                            unsafe_allow_html=True,
                        )
                    elif st.button("분석 시작", key="btn_diag2_ai"):
                        top5 = filtered_v06.head(5)
                        diag2_ctx = "\n".join([
                            f"- {r['REGION']} {r['RENTAL_SUB_CATEGORY']}: "
                            f"계약 {fmt_int(r['CONTRACT_COUNT'])}건, 완료율 {r['PAYEND_RATE_PCT']}%"
                            for _, r in top5.iterrows()
                        ])
                        diag2_prompt = (
                            "다음은 아정당 렌탈 사업에서 완료율이 가장 낮은 지역×상품 조합입니다.\n\n"
                            "[필수 규칙]\n"
                            "- 각 구간의 데이터 수치(계약 수, 완료율)를 반드시 인용하세요\n"
                            "- 지역 특성(인구밀도, 접근성, 경쟁)과 상품 특성(설치 복잡도, 계절 수요)을 고려하세요\n"
                            "- 각 구간별 완료율을 전국 평균과 비교하여 격차를 명시하세요\n"
                            "- 개선 시 예상되는 추가 완료 건수를 추정하세요\n"
                            "- 우선 조치 사항을 긴급도 순으로 제시하세요\n\n"
                            f"[데이터]\n{diag2_ctx}\n\n[전체 현황 참고]\n{strategy_context}"
                        )
                        with st.spinner("Cortex AI 분석 중..."):
                            diag2_result = cortex_summarize(diag2_prompt)
                        if diag2_result:
                            st.session_state[_diag2_key] = diag2_result
                            st.rerun()
                        else:
                            st.info("Cortex AI 분석을 생성하지 못했습니다.")

    with diag_tab3:
        st.markdown("### 채널별 성과 (2025)")
        if not v04_channel.empty:
            ch_top = v04_channel.head(15)
            st.dataframe(
                ch_top.rename(columns={
                    "MAIN_CATEGORY_NAME": "카테고리", "RECEIVE_PATH_NAME": "접수 경로",
                    "INFLOW_PATH_NAME": "유입 경로", "CONTRACT_COUNT": "계약 수",
                    "PAYEND_COUNT": "완료 수", "PAYEND_RATE_PCT": "완료율(%)",
                }),
                use_container_width=True, hide_index=True,
            )

        st.markdown("### 콜센터: 통화→계약 전환율 (2025)")
        if not v11_cvr.empty:
            left, right = st.columns(2)
            with left:
                call_chart = (
                    alt.Chart(v11_cvr)
                    .mark_bar(cornerRadiusTopLeft=8, cornerRadiusTopRight=8)
                    .encode(
                        x=alt.X("MAIN_CATEGORY_NAME:N", title="카테고리"),
                        y=alt.Y("CALL_CVR_PCT:Q", title="통화→계약 전환율(%)"),
                        color=alt.Color("DIVISION_NAME:N", title="수신/발신",
                                        scale=alt.Scale(range=["#1d4ed8", "#93c5fd"])),
                        xOffset="DIVISION_NAME:N",
                        tooltip=["MAIN_CATEGORY_NAME", "DIVISION_NAME", "CALL_CVR_PCT", "LINKED_CONTRACTS"],
                    )
                    .properties(height=280)
                )
                st.altair_chart(call_chart, use_container_width=True)

            with right:
                linked_chart = (
                    alt.Chart(v11_cvr)
                    .mark_bar(cornerRadiusTopLeft=8, cornerRadiusTopRight=8)
                    .encode(
                        x=alt.X("MAIN_CATEGORY_NAME:N", title="카테고리"),
                        y=alt.Y("LINKED_CONTRACTS:Q", title="연결된 계약 수"),
                        color=alt.Color("DIVISION_NAME:N", title="수신/발신",
                                        scale=alt.Scale(range=["#1d4ed8", "#93c5fd"])),
                        xOffset="DIVISION_NAME:N",
                        tooltip=["MAIN_CATEGORY_NAME", "DIVISION_NAME", "LINKED_CONTRACTS", "CALL_CVR_PCT"],
                    )
                    .properties(height=280)
                )
                st.altair_chart(linked_chart, use_container_width=True)

        if cortex_ready:
            with st.expander("🤖 Cortex AI: 채널·콜센터 최적화 제안"):
                _diag3_key = f"diag3_ai_{DASHBOARD_DATE}"
                if _diag3_key in st.session_state:
                    st.markdown(
                        f'<div class="ai-box"><div class="ai-box-title">Cortex AI 채널 최적화 제안</div>{st.session_state[_diag3_key]}</div>',
                        unsafe_allow_html=True,
                    )
                elif st.button("분석 시작", key="btn_diag3_ai"):
                    ch_ctx_parts = []
                    if not v04_channel.empty:
                        ch_ctx_parts.append("채널별 성과:")
                        for _, r in v04_channel.head(5).iterrows():
                            ch_ctx_parts.append(
                                f"- {r['MAIN_CATEGORY_NAME']} {r['RECEIVE_PATH_NAME']}/{r['INFLOW_PATH_NAME']}: "
                                f"계약 {fmt_int(r['CONTRACT_COUNT'])}건, 완료율 {r['PAYEND_RATE_PCT']}%"
                            )
                    if not v11_cvr.empty:
                        ch_ctx_parts.append("\n콜센터 전환율:")
                        for _, r in v11_cvr.iterrows():
                            ch_ctx_parts.append(
                                f"- {r['MAIN_CATEGORY_NAME']} {r['DIVISION_NAME']}: "
                                f"전환율 {r['CALL_CVR_PCT']}%, 연결 계약 {fmt_int(r['LINKED_CONTRACTS'])}건"
                            )
                    diag3_prompt = (
                        "아래 아정당의 채널별·콜센터별 성과 데이터를 분석하여:\n"
                        "1) 가장 효율적인 채널과 그 이유 — 계약 수, 완료율 수치 인용\n"
                        "2) 개선이 시급한 채널 — 현재 수치와 목표 수치 제시, 개선 시 예상 추가 완료 건수\n"
                        "3) 마케팅 예산 재배분 제안 — 현재 채널별 비중 대비 성과를 근거로 구체적 비율 제시\n"
                        "[필수] 모든 분석에 데이터 수치를 인용하고, 일반론이 아닌 이 데이터 기반의 인사이트를 제시하세요.\n\n"
                        + "\n".join(ch_ctx_parts) + f"\n\n[전체 현황 참고]\n{strategy_context}"
                    )
                    with st.spinner("Cortex AI 분석 중..."):
                        diag3_result = cortex_summarize(diag3_prompt)
                    if diag3_result:
                        st.session_state[_diag3_key] = diag3_result
                        st.rerun()
                    else:
                        st.info("Cortex AI 분석을 생성하지 못했습니다.")

        st.caption("이 탭은 전국 합산 데이터이며, 사이드바 지역 필터가 적용되지 않습니다.")


# ── Tab 3: 전략 추천 (AI 하이브리드) ─────────────────────────────────────────
with tab3:
    st.subheader(f"전략 추천: {DASHBOARD_DATE_LABEL} 기준, 무엇을 먼저 할 것인가")
    st.markdown(
        f'<div class="summary-box">한 줄 요약: Cortex AI가 <b>{DASHBOARD_DATE_LABEL}</b> 기준, '
        f'해당 시점까지의 <b>전체 실적·월별 추세·외부 신호</b>를 종합 분석하여 최우선 전략을 도출합니다. '
        f'(사이드바에서 오늘 날짜를 변경하면 전략이 달라집니다)</div>',
        unsafe_allow_html=True,
    )

    if cortex_ready:
        # session_state에 캐시 — 탭 전환/rerun 시 재호출 방지 (기준일별로 별도 캐시)
        cache_key = f"strategies_{DASHBOARD_DATE}"
        if cache_key not in st.session_state:
            st.session_state[cache_key] = None

        if st.session_state[cache_key] is None:
            if st.button("🤖 Cortex AI 전략 도출 시작", type="primary", use_container_width=True, key="gen_strat"):
                with st.spinner("Cortex AI가 데이터를 분석하여 전략을 도출하고 있습니다..."):
                    result = cortex_generate_strategies(data_facts, strategy_context, trend_context)
                st.session_state[cache_key] = result if result else ""
                st.rerun()

        ai_strategies_raw = st.session_state.get(cache_key)

        if ai_strategies_raw:
            strategies = parse_ai_strategies(ai_strategies_raw)

            if strategies:
                # ── 카테고리별 색상 매핑 ──
                cat_pill_map = {
                    "매출긴급": "pill-red",
                    "이탈방지": "pill-orange",
                    "성장기회": "pill-green",
                    "효율개선": "pill-blue",
                    "외부대응": "pill",
                }
                urgency_icon = {"상": "🔴", "중": "🟡", "하": "🟢"}

                # ── TOP 3 카드 ──
                st.markdown("### 최우선 전략 TOP 3")
                top3 = strategies[:3]
                card_cols = st.columns(3)
                rank_classes = ["rank-1", "rank-2", "rank-3"]
                for idx, s in enumerate(top3):
                    cat = s.get("category", "")
                    pill_cls = cat_pill_map.get(cat, "pill")
                    urg = s.get("urgency", "중")
                    urg_icon = urgency_icon.get(urg, "🟡")
                    with card_cols[idx]:
                        st.markdown(
                            f"""<div class="strategy-card">
                            <span class="strategy-rank {rank_classes[idx]}">#{idx+1}</span>
                            <span class="{pill_cls}">{cat}</span>
                            <span class="pill">{urg_icon} 긴급도: {urg}</span>
                            <div class="mini-card-main" style="margin-top:12px;">{s.get('title', '')}</div>
                            <div class="mini-card-note">
                              <b>대상:</b> {s.get('target', '')}<br/>
                              <b>근거:</b> {s.get('evidence', '')}<br/>
                              <b>기대효과:</b> {s.get('impact', '')}
                            </div>
                            </div>""",
                            unsafe_allow_html=True,
                        )

                # ── 카테고리 필터 ──
                all_cats = list(dict.fromkeys([s.get("category", "") for s in strategies]))
                sel_cats = st.multiselect(
                    "전략 카테고리 필터", all_cats, default=all_cats, key="strat_cat_filter"
                )
                filtered_strategies = [s for s in strategies if s.get("category", "") in sel_cats]

                # ── 전략 상세 보기 ──
                st.markdown("### 전략 상세 보기")
                if filtered_strategies:
                    labels = [
                        f"#{i+1} [{s.get('category', '')}] {s.get('title', '')}"
                        for i, s in enumerate(filtered_strategies)
                    ]
                    sel_label = st.selectbox("전략 선택", labels, index=0)
                    sel_idx = labels.index(sel_label)
                    sel_s = filtered_strategies[sel_idx]

                    detail_cols = st.columns(2)
                    with detail_cols[0]:
                        cat = sel_s.get("category", "")
                        pill_cls = cat_pill_map.get(cat, "pill")
                        urg = sel_s.get("urgency", "")
                        st.markdown(
                            f"""<div class="mini-card">
                            <div class="mini-card-title">전략 개요</div>
                            <div style="margin-top:8px;">
                              <span class="{pill_cls}">{cat}</span>
                              <span class="pill">{urgency_icon.get(urg, '🟡')} 긴급도: {urg}</span>
                            </div>
                            <div class="mini-card-main" style="margin-top:12px;">{sel_s.get('title', '')}</div>
                            <div class="mini-card-note" style="margin-top:8px;">
                              <b>대상:</b> {sel_s.get('target', '')}<br/>
                              <b>핵심 근거:</b> {sel_s.get('evidence', '')}
                            </div>
                            </div>""",
                            unsafe_allow_html=True,
                        )
                    with detail_cols[1]:
                        st.markdown(
                            f"""<div class="mini-card">
                            <div class="mini-card-title">🤖 AI 실행 가이드</div>
                            <div class="mini-card-note" style="margin-top:8px;font-size:14px;">
                              {sel_s.get('action', '')}
                            </div>
                            <div style="margin-top:12px;padding-top:8px;border-top:1px solid #eee;">
                              <div class="mini-card-title">기대 효과</div>
                              <div class="mini-card-note">{sel_s.get('impact', '')}</div>
                            </div>
                            </div>""",
                            unsafe_allow_html=True,
                        )

                    # ── 전략 근거 데이터 ──
                    st.markdown("### 이 전략의 근거 데이터")
                    target_text = sel_s.get("target", "").lower()
                    evidence_text = sel_s.get("evidence", "").lower()
                    search_text = target_text + " " + evidence_text

                    ev_cols = st.columns(2)

                    # 지역 데이터 매칭
                    matched_regions = []
                    if not v01_reg.empty:
                        for region in v01_reg["REGION"].unique():
                            if region.lower() in search_text or region[:2] in search_text:
                                matched_regions.append(region)

                    with ev_cols[0]:
                        if matched_regions:
                            st.markdown(f"**관련 지역 실적**")
                            region_data = v01_reg[v01_reg["REGION"].isin(matched_regions)]
                            st.dataframe(
                                region_data[["REGION", "MAIN_CATEGORY_NAME", "CONTRACT_COUNT", "PAYEND_RATE_PCT", "LOSS_COUNT"]].rename(columns={
                                    "REGION": "지역", "MAIN_CATEGORY_NAME": "카테고리",
                                    "CONTRACT_COUNT": "계약 수", "PAYEND_RATE_PCT": "완료율(%)", "LOSS_COUNT": "손실 수",
                                }),
                                use_container_width=True, hide_index=True,
                            )
                        elif "인터넷" in search_text:
                            st.markdown("**인터넷 지역별 현황**")
                            inet_top = v01_reg[v01_reg["MAIN_CATEGORY_NAME"] == "인터넷"].head(5)
                            st.dataframe(
                                inet_top[["REGION", "CONTRACT_COUNT", "PAYEND_RATE_PCT", "LOSS_COUNT"]].rename(columns={
                                    "REGION": "지역", "CONTRACT_COUNT": "계약 수",
                                    "PAYEND_RATE_PCT": "완료율(%)", "LOSS_COUNT": "손실 수",
                                }),
                                use_container_width=True, hide_index=True,
                            )
                        elif "렌탈" in search_text and not v06_product.empty:
                            st.markdown("**렌탈 상품별 현황**")
                            st.dataframe(
                                v06_product.head(5)[["RENTAL_SUB_CATEGORY", "CONTRACT_COUNT", "PAYEND_RATE_PCT", "LOSS_COUNT"]].rename(columns={
                                    "RENTAL_SUB_CATEGORY": "상품", "CONTRACT_COUNT": "계약 수",
                                    "PAYEND_RATE_PCT": "완료율(%)", "LOSS_COUNT": "손실 수",
                                }),
                                use_container_width=True, hide_index=True,
                            )
                        else:
                            st.markdown("**전체 카테고리 요약**")
                            cat_summary = v01_reg.groupby("MAIN_CATEGORY_NAME", as_index=False).agg(
                                CONTRACT_COUNT=("CONTRACT_COUNT", "sum"),
                                LOSS_COUNT=("LOSS_COUNT", "sum"),
                            )
                            cat_summary["PAYEND_RATE_PCT"] = (
                                (cat_summary["CONTRACT_COUNT"] - cat_summary["LOSS_COUNT"]) / cat_summary["CONTRACT_COUNT"] * 100
                            ).round(2)
                            st.dataframe(
                                cat_summary.rename(columns={
                                    "MAIN_CATEGORY_NAME": "카테고리", "CONTRACT_COUNT": "계약 수",
                                    "LOSS_COUNT": "손실 수", "PAYEND_RATE_PCT": "완료율(%)",
                                }),
                                use_container_width=True, hide_index=True,
                            )

                    with ev_cols[1]:
                        # 상품 매칭
                        matched_products = []
                        if not v06_product.empty:
                            for prod in v06_product["RENTAL_SUB_CATEGORY"].unique():
                                if prod.lower() in search_text or prod[:2] in search_text:
                                    matched_products.append(prod)

                        if matched_products:
                            st.markdown(f"**관련 상품 실적**")
                            prod_data = v06_product[v06_product["RENTAL_SUB_CATEGORY"].isin(matched_products)]
                            rate_chart = (
                                alt.Chart(prod_data)
                                .mark_bar(cornerRadiusTopLeft=6, cornerRadiusTopRight=6, color="#0f766e")
                                .encode(
                                    x=alt.X("RENTAL_SUB_CATEGORY:N", title="상품"),
                                    y=alt.Y("PAYEND_RATE_PCT:Q", title="완료율(%)"),
                                    tooltip=["RENTAL_SUB_CATEGORY", "CONTRACT_COUNT", "PAYEND_RATE_PCT"],
                                )
                                .properties(height=200)
                            )
                            st.altair_chart(rate_chart, use_container_width=True)
                        elif "채널" in search_text or "콜센터" in search_text:
                            st.markdown("**채널·콜센터 성과**")
                            if not v04_channel.empty:
                                st.dataframe(
                                    v04_channel.head(5)[["MAIN_CATEGORY_NAME", "RECEIVE_PATH_NAME", "CONTRACT_COUNT", "PAYEND_RATE_PCT"]].rename(columns={
                                        "MAIN_CATEGORY_NAME": "카테고리", "RECEIVE_PATH_NAME": "채널",
                                        "CONTRACT_COUNT": "계약 수", "PAYEND_RATE_PCT": "완료율(%)",
                                    }),
                                    use_container_width=True, hide_index=True,
                                )
                        else:
                            # 외부 신호 요약
                            st.markdown("**외부 신호 현황**")
                            ext_items = []
                            if not weather_df.empty and "AVG_TEMP_C" in weather_df.columns:
                                max_t = weather_df["AVG_TEMP_C"].max()
                                ext_items.append(f"날씨: 최고 {max_t:.0f}°C")
                            if not holiday_df.empty and "IS_HOLIDAY" in holiday_df.columns:
                                hols = holiday_df[holiday_df["IS_HOLIDAY"] == True]
                                if not hols.empty and "HOLIDAY_NAME" in hols.columns:
                                    ext_items.append(f"공휴일: {', '.join(hols['HOLIDAY_NAME'].head(3).tolist())}")
                            if not ext_news.empty:
                                ext_items.append(f"정책 뉴스 {len(ext_news)}건")
                            if ext_items:
                                for item in ext_items:
                                    st.write(f"- {item}")
                            else:
                                st.caption("특이 외부 신호 없음")

                # 전체 AI 원문
                with st.expander("Cortex AI 전략 분석 원문"):
                    st.markdown(
                        f'<div class="ai-box"><div class="ai-box-title">Cortex AI 전략 도출 원문</div>{ai_strategies_raw}</div>',
                        unsafe_allow_html=True,
                    )
            else:
                st.warning("AI 응답을 파싱하지 못했습니다.")
                st.markdown(
                    f'<div class="ai-box">{ai_strategies_raw}</div>',
                    unsafe_allow_html=True,
                )
        elif ai_strategies_raw == "":
            st.warning("Cortex AI가 전략을 생성하지 못했습니다. 다시 시도하려면 아래 버튼을 누르세요.")
            if st.button("재시도", key="retry_strat"):
                st.session_state[cache_key] = None
                st.rerun()
        # ai_strategies_raw is None → 버튼 아직 안 눌림 (위에서 버튼 표시됨)
    else:
        st.info("Cortex AI가 비활성 상태입니다. Cortex AI가 활성화되면 데이터 기반 전략 추천이 표시됩니다.")

    # ── 외부 데이터 근거 ──
    st.markdown("---")
    st.markdown("### 외부 데이터 근거")
    ext_c1, ext_c2, ext_c3 = st.columns(3)
    with ext_c1:
        st.markdown("**최근 정책 뉴스**")
        if not ext_news.empty:
            for _, row in ext_news.head(5).iterrows():
                kw = row.get("KEYWORD_GROUP", "")
                st.write(f"- [{kw}] {row['TITLE']}")
        else:
            st.caption("뉴스 데이터 없음")

    with ext_c2:
        st.markdown("**날씨 예보**")
        if not weather_df.empty:
            for _, row in weather_df.head(5).iterrows():
                region = row.get("REGION", "")
                temp = row.get("AVG_TEMP_C", "")
                cond = row.get("WEATHER_CONDITION", "")
                fdate = row.get("FORECAST_DATE", "")
                st.write(f"- {fdate} {region}: {temp}°C {cond}")
        else:
            st.caption("날씨 데이터 없음")

    with ext_c3:
        st.markdown("**다가오는 공휴일**")
        if not holiday_df.empty:
            for _, row in holiday_df.iterrows():
                st.write(f"- {row['HOLIDAY_DATE']}: {row['HOLIDAY_NAME']}")
        else:
            st.caption("공휴일 데이터 없음")


# ── Tab 4: 시뮬레이션 ────────────────────────────────────────────────────────
with tab4:
    st.subheader("시뮬레이션: 외부 상황이 바뀌면 어떤 구간을 먼저 볼까")
    st.markdown(
        '<div class="summary-box">한 줄 요약: 폭염, 장마, 연휴, 이사 시즌, 정부 정책 등 6가지 시나리오가 '
        '계약 수와 완료율에 미치는 영향을 시뮬레이션합니다.</div>',
        unsafe_allow_html=True,
    )

    scenario_names = [f"{s['ICON']} {s['SCENARIO_NAME']}" for s in BUILT_IN_SCENARIOS]
    sel_scenario_name = st.selectbox("시나리오 선택", scenario_names, index=0)
    sel_sc_idx = scenario_names.index(sel_scenario_name)
    sel_sc = BUILT_IN_SCENARIOS[sel_sc_idx]

    st.markdown(
        f"""<div class="scenario-active">
        <div style="font-size:24px;font-weight:800;color:#0f766e;margin-bottom:8px;">
          {sel_sc['ICON']} {sel_sc['SCENARIO_NAME']}
        </div>
        <div style="font-size:14px;color:#333;line-height:1.7;margin-bottom:12px;">
          {sel_sc['DESCRIPTION']}
        </div>
        <div style="display:flex;gap:16px;flex-wrap:wrap;">
          <span class="pill">대상: {sel_sc['TARGET_CATEGORY']}{(' / ' + sel_sc['TARGET_PRODUCT']) if sel_sc['TARGET_PRODUCT'] else ''}</span>
          <span class="pill-orange">계약 배수: {sel_sc['CONTRACT_MULTIPLIER']}x</span>
          <span class="pill-blue">완료율 변화: {sel_sc['COMPLETION_RATE_DELTA_PCT']:+.1f}%p</span>
        </div>
        </div>""",
        unsafe_allow_html=True,
    )

    st.markdown("### 파라미터 조정")
    adj_cols = st.columns(2)
    with adj_cols[0]:
        adj_multiplier = st.slider(
            "계약 수 배수 조정", 0.5, 2.0,
            float(sel_sc["CONTRACT_MULTIPLIER"]), 0.05, key="sc_mult",
        )
    with adj_cols[1]:
        adj_delta = st.slider(
            "완료율 변화(%p) 조정", -15.0, 10.0,
            float(sel_sc["COMPLETION_RATE_DELTA_PCT"]), 0.5, key="sc_delta",
        )

    st.markdown("### 시뮬레이션 결과")
    target_cat = sel_sc["TARGET_CATEGORY"]
    target_prod = sel_sc["TARGET_PRODUCT"]

    presets = {
        "보수적": {"mult": 1 + (adj_multiplier - 1) * 0.5, "delta": adj_delta * 0.5},
        "기본": {"mult": adj_multiplier, "delta": adj_delta},
        "공격적": {"mult": 1 + (adj_multiplier - 1) * 1.5, "delta": adj_delta * 1.5},
    }

    def run_simulation(base_df, label_col, label_name, params):
        sim_rows = []
        for _, row in base_df.iterrows():
            bc = safe_float(row["CONTRACT_COUNT"])
            br = safe_float(row["PAYEND_RATE_PCT"]) / 100
            ec = bc * params["mult"]
            er = clamp01(br + params["delta"] / 100)
            bp, ep = bc * br, ec * er
            sim_rows.append({
                label_name: row[label_col],
                "기준 계약": fmt_int(bc), "시나리오 후 계약": fmt_int(ec),
                "기준 완료율": f"{br*100:.1f}%", "시나리오 후 완료율": f"{er*100:.1f}%",
                "기준 완료": fmt_int(bp), "시나리오 후 완료": fmt_int(ep),
                "차이": fmt_int(ep - bp),
            })
        return pd.DataFrame(sim_rows)

    if target_cat == "렌탈" and not v06_product.empty:
        base_df = v06_product.copy()
        if target_prod and target_prod in base_df["RENTAL_SUB_CATEGORY"].values:
            base_df = base_df[base_df["RENTAL_SUB_CATEGORY"] == target_prod]
        else:
            base_df = base_df.head(5)

        for preset_name, params in presets.items():
            st.markdown(f"**{preset_name} 시나리오** (배수 {params['mult']:.2f}x, 완료율 {params['delta']:+.1f}%p)")
            st.dataframe(run_simulation(base_df, "RENTAL_SUB_CATEGORY", "상품", params), use_container_width=True, hide_index=True)

    elif target_cat == "인터넷" and not v01_reg.empty:
        inet_data = v01_reg[v01_reg["MAIN_CATEGORY_NAME"] == "인터넷"].copy()
        if sel_regions:
            inet_data = inet_data[inet_data["REGION"].isin(sel_regions)]
        inet_data = inet_data.head(5)

        for preset_name, params in presets.items():
            st.markdown(f"**{preset_name} 시나리오** (배수 {params['mult']:.2f}x, 완료율 {params['delta']:+.1f}%p)")
            st.dataframe(run_simulation(inet_data, "REGION", "지역", params), use_container_width=True, hide_index=True)

    # 시나리오 대응 전략 — 버튼 클릭 시에만 Cortex 호출
    if cortex_ready:
        with st.expander("🤖 Cortex AI: 대응 전략 보기"):
            _sc_key = f"sc_ai_{sel_sc['SCENARIO_NAME']}_{adj_multiplier}_{adj_delta}"
            if _sc_key in st.session_state:
                st.markdown(
                    f'<div class="ai-box"><div class="ai-box-title">Cortex AI 대응 전략</div>{st.session_state[_sc_key]}</div>',
                    unsafe_allow_html=True,
                )
            elif st.button("대응 전략 생성", key="btn_sc_ai"):
                with st.spinner("Cortex AI가 대응 전략을 생성하고 있습니다..."):
                    scenario_prompt = (
                        f"당신은 통신·렌탈 플랫폼 운영 전략가입니다. 아래 시나리오에 대한 대응 전략을 작성하세요.\n\n"
                        f"시나리오: {sel_sc['SCENARIO_NAME']}\n"
                        f"설명: {sel_sc['DESCRIPTION']}\n"
                        f"대상: {sel_sc['TARGET_CATEGORY']}"
                        f"{(' / ' + sel_sc['TARGET_PRODUCT']) if sel_sc['TARGET_PRODUCT'] else ''}\n"
                        f"시뮬레이션 파라미터: 계약 배수 {adj_multiplier}x, 완료율 변화 {adj_delta:+.1f}%p\n\n"
                        f"[현재 실적]\n{strategy_context}\n\n"
                        f"[상세 데이터]\n{data_facts_compact}\n\n"
                        f"아래 내용을 작성하세요:\n"
                        f"1) 즉시 실행할 대응 액션 3가지 — 현재 데이터 수치를 인용하여 구체적 목표치와 기간 포함\n"
                        f"2) 가장 영향 받는 구간 — 현재 계약 수·완료율 기준으로 예상 변동 규모(건수) 추정\n"
                        f"3) 리스크와 완화 방안 — 과거 유사 패턴이 있다면 인용\n"
                        f"[필수] 모든 제안에 현재 데이터 수치를 근거로 포함하세요."
                    )
                    sc_analysis = cortex_summarize(scenario_prompt)
                if sc_analysis:
                    st.session_state[_sc_key] = sc_analysis
                    st.rerun()
                else:
                    st.info("Cortex AI가 대응 전략을 생성하지 못했습니다.")
    else:
        st.info("Cortex AI가 비활성 상태입니다.")

    # ── 직접 시뮬레이션 ──
    st.markdown("---")
    st.markdown("### 직접 시뮬레이션 (커스텀)")
    sim_cat = st.selectbox("카테고리", ["인터넷", "렌탈"], key="sim_cat")
    sim_base = v01_yr[(v01_yr["YR"] == 2025) & (v01_yr["MAIN_CATEGORY_NAME"] == sim_cat)]
    if not sim_base.empty:
        base_c = float(sim_base["CONTRACT_COUNT"].iloc[0])
        base_r = float(sim_base["PAYEND_RATE"].iloc[0])
        uplift = st.slider("계약 수 증감(%)", -20.0, 30.0, 0.0, 0.5, key="sim_up")
        delta = st.slider("완료율 변화(%p)", -10.0, 10.0, 0.0, 0.1, key="sim_delta")

        exp_c = base_c * (1 + uplift / 100)
        exp_r = clamp01(base_r + delta / 100)
        base_p = base_c * base_r
        exp_p = exp_c * exp_r

        sim_cols = st.columns(4)
        with sim_cols[0]:
            metric_card("기준 계약", fmt_int(base_c))
        with sim_cols[1]:
            metric_card("시뮬레이션 계약", fmt_int(exp_c), f"{uplift:+.1f}%")
        with sim_cols[2]:
            metric_card("기준 완료", fmt_int(base_p), f"완료율 {base_r*100:.1f}%")
        with sim_cols[3]:
            metric_card("시뮬레이션 완료", fmt_int(exp_p), f"완료율 {exp_r*100:.1f}% / 차이 {fmt_int(exp_p - base_p)}건")


# ── Tab 5: AI 질의 ───────────────────────────────────────────────────────────
with tab5:
    st.subheader("AI에게 질문하기 (Snowflake Cortex AI)")
    st.markdown(
        '<div class="summary-box">한 줄 요약: 대시보드의 전체 데이터와 분석 결과를 기반으로 '
        'Cortex AI가 전략적 질문에 답변합니다. 데이터에 근거한 구체적 답변을 제공합니다.</div>',
        unsafe_allow_html=True,
    )

    if not cortex_ready:
        st.info("Cortex AI를 사용할 수 없는 환경입니다. Snowflake 리전과 권한을 확인해 주세요.")
    else:
        # 경량 컨텍스트 — 전체 기준 + 추세 + 선택 기간 요약
        context_text = f"{strategy_context}\n{trend_context}\n선택 기간 참고: {period_context}\n\n{data_facts_compact}"

        # 퍼널 핵심 지표만 추가
        if not v03_funnel.empty:
            fn25 = v03_funnel[v03_funnel["YR"] == 2025]
            if not fn25.empty:
                funnel_parts = ["\n=== 퍼널 (2025) ==="]
                for _, row in fn25.iterrows():
                    funnel_parts.append(
                        f"  {row['MAIN_CATEGORY_NAME']}: 전체→완료 {row['TOTAL_PAYEND_PCT']}%, "
                        f"신청→등록 {row['SUB_TO_REG_PCT']}%"
                    )
                context_text += "\n".join(funnel_parts)

        st.markdown("**질문 예시** (클릭하면 바로 입력됩니다)")
        ex_categories = {
            "현황 분석": [
                "2025년 인터넷과 렌탈의 완료율이 2024년 대비 어떻게 변했어?",
                "렌탈 중 완료율이 가장 낮은 상품은 뭐고 왜 그런 거야?",
            ],
            "전략 제안": [
                "경기도 에어컨 완료율을 올리려면 어떤 전략이 효과적이야?",
                "콜센터 수신과 발신 중 어디에 더 투자해야 해?",
            ],
            "시나리오": [
                "폭염이 오면 어떤 상품을 먼저 준비해야 해?",
                "이사 시즌에 인터넷 계약이 몰리면 어떻게 대응해야 해?",
            ],
        }

        # 예시 클릭 → _pending_q에 저장 (위젯 렌더링 전이므로 안전)
        if "_pending_q" in st.session_state:
            st.session_state["ai_q_input"] = st.session_state.pop("_pending_q")

        for cat_name, examples in ex_categories.items():
            st.markdown(f"**{cat_name}**")
            ex_cols = st.columns(len(examples))
            for i, ex in enumerate(examples):
                with ex_cols[i]:
                    if st.button(ex, key=f"ex_{cat_name}_{i}", use_container_width=True):
                        st.session_state["_pending_q"] = ex
                        st.rerun()

        if "chat_history" not in st.session_state:
            st.session_state["chat_history"] = []

        user_q = st.text_input(
            "질문 입력",
            placeholder="예: 2025년 초반 단계가 왜 약해졌어?",
            key="ai_q_input",
        )

        col_send, col_clear = st.columns([1, 1])
        with col_send:
            send_clicked = st.button("질문하기", type="primary", use_container_width=True)
        with col_clear:
            if st.button("대화 초기화", use_container_width=True):
                st.session_state["chat_history"] = []
                st.session_state["_pending_q"] = ""
                st.rerun()

        if send_clicked and user_q:
            history_text = ""
            if st.session_state["chat_history"]:
                recent = st.session_state["chat_history"][-6:]
                history_parts = []
                for msg in recent:
                    role = "사용자" if msg["role"] == "user" else "AI"
                    history_parts.append(f"{role}: {msg['content'][:200]}")
                history_text = "\n\n[이전 대화]\n" + "\n".join(history_parts)

            full_context = context_text + history_text

            with st.spinner("Cortex AI가 답변을 생성하고 있습니다..."):
                answer = cortex_chat(user_q, full_context)

            st.session_state["chat_history"].append({"role": "user", "content": user_q})
            st.session_state["chat_history"].append({"role": "ai", "content": answer})
            st.session_state["_pending_q"] = ""
            st.rerun()

        if st.session_state["chat_history"]:
            st.markdown("### 대화 내역")
            for msg in st.session_state["chat_history"]:
                if msg["role"] == "user":
                    st.markdown(
                        f'<div class="chat-msg chat-user"><b>Q:</b> {msg["content"]}</div>',
                        unsafe_allow_html=True,
                    )
                else:
                    st.markdown(
                        f'<div class="chat-msg chat-ai"><b>A:</b> {msg["content"]}</div>',
                        unsafe_allow_html=True,
                    )
