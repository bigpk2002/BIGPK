# ╔══════════════════════════════════════════════════════════════╗
# ║   INSTITUTIONAL STOCK SCREENER  —  v3.13 (single-file)        ║
# ║   ข้อมูลหลักดึงอัตโนมัติวันละครั้งหลังตลาดปิด ผ่าน GitHub        ║
# ║   Action (ดู fetch_data.py + .github/workflows/prefetch.yml)  ║
# ╚══════════════════════════════════════════════════════════════╝
# v3.13: หมายเหตุจาก self-audit — comment บล็อกนี้เคยค้างเลข v3.4 ไว้ตลอดช่วง
# v3.7-v3.12 (ไม่มีใครอัปเดตตามตอนขึ้นเวอร์ชันใหม่ เพราะเป็นแค่ comment ไม่ใช่
# โค้ดที่รันจริง) ดู CHANGELOG.md สำหรับประวัติแบบละเอียดทุก version แทน —
# comment สรุปสั้นด้านล่างนี้จะไม่พยายามตามทุก version ให้ครบอีกต่อไป
#
# สรุปการเปลี่ยนแปลงสะสมจาก v2.0 เดิม (รายละเอียดเต็มอยู่ใน docstring/comment
# ของแต่ละฟังก์ชันด้านล่าง, และ CHANGELOG.md สำหรับ v3.6 ขึ้นไป):
#   1. ความแม่นยำ: แก้บั๊ก relative_strength เทียบ "ตำแหน่ง" ข้ามตลาดที่ปฏิทิน
#      วันเทรดต่างกัน (หุ้นไทย .BK vs SPY) + guard format ของ dividendYield
#   2. ความเร็ว/เสถียร: ลด network call ต่อ ticker, แยก cache fundamentals
#      ออกจาก cache ราคา, เพิ่ม retry+backoff (แยก backoff ยาวพิเศษสำหรับ
#      rate-limit โดยเฉพาะ), ดึง S&P500 จาก GitHub CSV แทน Wikipedia (403)
#   3. Backtest: เข้าซื้อที่ open แท่งถัดไป (ไม่ lookahead), เทียบ Buy&Hold,
#      เพิ่ม Max Drawdown และ Sharpe โดยประมาณ
#   4. ฟีเจอร์ใหม่: watchlist persist ข้าม session จริง (เซฟลง disk) +
#      แจ้งเตือนสัญญาณใหม่ (in-app + Telegram แบบออปชัน)
#   5. Prefetch architecture (v3.2-3.4): แยก "ดึงข้อมูล" กับ "ดู" ออกจากกัน
#      สมบูรณ์ — fetch_data.py รันผ่าน GitHub Action วันละครั้งหลังตลาด
#      สหรัฐฯ+ไทยปิดทั้งคู่ แอปแค่อ่านไฟล์ที่ดึงไว้แล้ว ไม่ยิง Yahoo ตอนคนดูเลย
#   6. v3.7-v3.12 (สรุปย่อ — ดู CHANGELOG.md ฉบับเต็ม): Weekly Trend filter,
#      แนวรับใช้กราฟรายสัปดาห์, Sector Heatmap ดึงล่วงหน้าอัตโนมัติ + ใช้ครบ
#      ทุกตัวใน sector, แก้บั๊กตัดข้อมูล Dashboard ตามตัวอักษร, จำกัดสแกนสด
#      กันกระทบผู้ใช้อื่น, แจ้งเตือน ticker ที่หาไม่เจอแทนตัดทิ้งเงียบๆ
import datetime
import hashlib
import json
import logging
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import wraps
from typing import Callable, Optional
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
import yfinance as yf

logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("screener")


# ════════════════════════════════════════════════════════
# [merged from lib/utils.py]
# ════════════════════════════════════════════════════════
# UTILITIES — ใช้ร่วมกันทุกโมดูล
#   • logging (เหมือน v2.0 เดิม)
#   • retry decorator พร้อม exponential backoff — (ใหม่ใน v3.0)
#     เดิม v2.0 ไม่มี retry เลย ถ้า Yahoo ตอบ rate-limit/timeout ครั้งเดียว
#     หุ้นตัวนั้นจะหายไปจากผลสแกนทันทีโดยไม่มีการลองใหม่
#   • to_date_indexed() — (ใหม่ใน v3.0) ใช้ normalize index ของราคาให้เป็น
#     "วันที่" ล้วน (ไม่มี time/timezone) สำหรับเทียบ 2 ซีรีส์ที่มาจาก
#     ตลาดคนละ timezone/ปฏิทินวันเทรด (เช่นหุ้นไทย .BK เทียบกับ SPY สหรัฐฯ)


logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("screener")


def log_err(context: str, e: Exception) -> None:
    """Log error แบบสั้น ไม่ทำให้ UI พัง แค่ไม่ให้ error หายไปเงียบๆ"""
    logger.warning("%s -> %s: %s", context, type(e).__name__, e)


def retry(times: int = 3, base_delay: float = 0.6, exceptions=(Exception,)):
    """
    Decorator: ลองใหม่แบบ exponential backoff + jitter เมื่อ network call ล่ม
    ชั่วคราว (เช่น Yahoo ตอบ 429 / timeout)

    v3.3: เพิ่ม backoff แบบยาวเป็นพิเศษเฉพาะ error ที่เป็น rate-limit จริงๆ
    (เห็นจาก log การรันจริงบน GitHub Actions ว่า "Too Many Requests" เกิดขึ้น
    เป็นชุดต่อเนื่องหลังยิง request รัวๆ — backoff สั้นแบบเดิม (<2 วินาทีรวม)
    ไม่พอให้ Yahoo คลายการบล็อก ลองใหม่กี่ครั้งก็ยังโดนซ้ำ) ตอนนี้ถ้า error
    message มีคำว่า rate limit ชัดๆ จะรอยาวขึ้นมาก (8s, 16s, 32s...) แทน
    """
    def deco(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(times):
                try:
                    return fn(*args, **kwargs)
                except exceptions as e:
                    last_exc = e
                    if attempt < times - 1:
                        msg = str(e)
                        is_rate_limit = ("Rate limit" in msg or "Too Many Requests" in msg
                                         or "429" in msg or "RateLimitError" in type(e).__name__)
                        if is_rate_limit:
                            delay = 8 * (2 ** attempt) + random.uniform(0, 2)
                        else:
                            delay = base_delay * (2 ** attempt) + random.uniform(0, 0.3)
                        time.sleep(delay)
            raise last_exc
        return wrapper
    return deco


def to_date_indexed(s: pd.Series) -> pd.Series:
    """
    Normalize index ของ Series ราคาให้เป็นวันที่ล้วน ตัด time + timezone ออก
    จำเป็นก่อนเทียบ 2 ซีรีส์ที่มี trading calendar ต่างกัน (เช่น SET ไทย vs
    NYSE สหรัฐฯ มีวันหยุดไม่ตรงกัน) ด้วย "วันที่จริง" แทนตำแหน่ง index
    """
    idx = pd.to_datetime(s.index)
    if getattr(idx, "tz", None) is not None:
        idx = idx.tz_localize(None)
    out = s.copy()
    out.index = idx.normalize()
    return out


# ════════════════════════════════════════════════════════
# [merged from lib/cache_store.py]
# ════════════════════════════════════════════════════════
# DISK CACHE & PERSISTENCE
#   • Scan-result cache ต่อ universe (เหมือน v2.0 เดิม ย้ายมาไว้ที่นี่)
#   • Watchlist persistence — (ใหม่ใน v3.0)
#     เดิม v2.0 watchlist อยู่ใน st.session_state ล้วนๆ → ปิดเบราว์เซอร์/รีโหลด
#     หน้าเว็บแล้วหายทันที ตอนนี้บันทึกลง disk เหมือน scan cache
#   • Last-signal snapshot — (ใหม่ใน v3.0) ใช้เทียบว่ามีหุ้นไหน "เพิ่งเปลี่ยน
#     เป็น Strong Buy/Breakout ตั้งแต่สแกนล่าสุด" เพื่อทำแถบแจ้งเตือนในแดชบอร์ด
# 
# ข้อจำกัดที่ควรรู้ (บอกตรงๆ ไม่ได้โฆษณาเกินจริง):
# Streamlit Community Cloud ใช้ container แบบ ephemeral — ไฟล์พวกนี้จะอยู่
# ข้าม "restart/sleep-wake" ตามปกติ แต่จะถูกล้างถ้า redeploy ใหม่จาก git push
# (filesystem ของ container ถูกสร้างใหม่ทั้งหมด) ถ้าต้องการ persistence แบบ
# ถาวร 100% ข้าม deploy ต้องต่อ external storage (Google Sheets/Supabase/S3)
# ซึ่งเป็นข้อจำกัดของแพลตฟอร์ม ไม่ใช่ของโค้ดส่วนนี้
from typing import Optional



CACHE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".scan_cache"
)
os.makedirs(CACHE_DIR, exist_ok=True)

WATCHLIST_PATH = os.path.join(CACHE_DIR, "watchlist.json")


# ─────────────────────────────────────────────────────────────
# SCAN-RESULT CACHE (เหมือน v2.0)
# ─────────────────────────────────────────────────────────────
def cache_key(universe: str, tickers: tuple, period: str, interval: str) -> str:
    raw = f"{universe}|{period}|{interval}|{','.join(sorted(tickers))}"
    h = hashlib.md5(raw.encode()).hexdigest()[:10]
    safe_name = "".join(c for c in universe if c.isalnum())[:20]
    return f"{safe_name}_{h}"


# v3.45: ลบ load_disk_cache() ทิ้ง — ตรวจสอบด้วย static analysis แล้วว่าไม่มี
# ที่ไหนเรียกใช้เลยทั้งไฟล์ (เขียนไว้ตั้งแต่ยุคก่อน v3.2 ที่เปลี่ยนมาใช้ระบบ
# auto-load จาก prefetched bundle เป็นหลัก — เส้นทางอ่าน cache กลับมาใช้ถูก
# เลิกใช้ไปตั้งแต่ตอนนั้น แต่ไม่มีใครลบฟังก์ชันทิ้ง) save_disk_cache() ยังใช้
# อยู่จริง (ให้ cache_age_label() อ่าน metadata ไปโชว์ "สแกนล่าสุดกี่นาทีที่
# แล้ว") เก็บไว้ตามเดิม ไม่ได้ลบ
def save_disk_cache(universe: str, tickers: tuple, period: str, interval: str, df: pd.DataFrame) -> None:
    key = cache_key(universe, tickers, period, interval)
    path = os.path.join(CACHE_DIR, f"{key}.json")
    try:
        payload = {
            "saved_at": datetime.datetime.now(ZoneInfo("Asia/Bangkok")).isoformat(),
            "universe": universe,
            "data": df.to_dict(orient="records"),
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, default=str, ensure_ascii=False)
    except Exception as e:
        log_err(f"save_disk_cache({universe})", e)


def cache_age_label(universe: str, tickers: tuple, period: str, interval: str) -> str:
    key = cache_key(universe, tickers, period, interval)
    path = os.path.join(CACHE_DIR, f"{key}.json")
    if not os.path.exists(path):
        return ""
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        saved_at = datetime.datetime.fromisoformat(payload["saved_at"])
        now = datetime.datetime.now(ZoneInfo("Asia/Bangkok"))
        delta_min = int((now - saved_at).total_seconds() / 60)
        if delta_min < 60:
            return f"สแกนล่าสุด {delta_min} นาทีที่แล้ว"
        elif delta_min < 1440:
            return f"สแกนล่าสุด {delta_min // 60} ชม.ที่แล้ว"
        return f"สแกนล่าสุด {saved_at.strftime('%d/%m %H:%M')}"
    except Exception as e:
        log_err(f"cache_age_label({universe})", e)
        return ""


def clear_cache_for(universe: str, tickers: tuple, period: str, interval: str) -> bool:
    """ลบ cache ของ universe นี้ — คืนค่า True ถ้ามีไฟล์ให้ลบจริง"""
    key = cache_key(universe, tickers, period, interval)
    path = os.path.join(CACHE_DIR, f"{key}.json")
    if os.path.exists(path):
        os.remove(path)
        return True
    return False


# ─────────────────────────────────────────────────────────────
# WATCHLIST PERSISTENCE (ใหม่ v3.0)
# ─────────────────────────────────────────────────────────────
def load_watchlist() -> list:
    if not os.path.exists(WATCHLIST_PATH):
        return []
    try:
        with open(WATCHLIST_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log_err("load_watchlist", e)
        return []


def save_watchlist(items: list) -> None:
    try:
        with open(WATCHLIST_PATH, "w", encoding="utf-8") as f:
            json.dump(items, f, ensure_ascii=False)
    except Exception as e:
        log_err("save_watchlist", e)


# ─────────────────────────────────────────────────────────────
# DECISION LOG PERSISTENCE (ใหม่ v3.18)
# ─────────────────────────────────────────────────────────────
# บันทึกการตัดสินใจของผู้ใช้เอง (ซื้อ/ไม่ซื้อ/ขาย) + ผลลัพธ์ทีหลัง (กำไร/
# ขาดทุน) — เก็บ local เหมือน watchlist (จำกัดเหมือนกัน: อยู่แค่บนเครื่องที่
# รัน Streamlit ไม่ sync ข้าม session/เครื่อง) เป้าหมายคือให้ผู้ใช้เห็นว่า
# "ตัวเองตัดสินใจแม่นแค่ไหน" ไม่ใช่แค่ระบบแม่นไหม เพราะสองอย่างนี้คนละเรื่อง
DECISION_LOG_PATH = os.path.join(CACHE_DIR, "decision_log.json")


def load_decision_log() -> list:
    if not os.path.exists(DECISION_LOG_PATH):
        return []
    try:
        with open(DECISION_LOG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log_err("load_decision_log", e)
        return []


def save_decision_log(items: list) -> None:
    try:
        with open(DECISION_LOG_PATH, "w", encoding="utf-8") as f:
            json.dump(items, f, ensure_ascii=False)
    except Exception as e:
        log_err("save_decision_log", e)


def check_losing_streak(log: list, threshold: int = 3) -> int:
    """
    v3.18: นับจำนวนไม้ที่ "ขาดทุน" ติดกันล่าสุด (เรียงจากรายการล่าสุดย้อนไป)
    หยุดนับทันทีที่เจอไม้ที่ยังไม่มีผลลัพธ์ (outcome=None) หรือกำไร — ใช้เตือน
    ให้พักคิดก่อนเข้าไม้ถัดไป (กัน revenge trading) คืนค่าจำนวนไม้ที่ขาดทุนติดกัน
    """
    streak = 0
    for entry in reversed(log):
        outcome = entry.get("outcome")
        if outcome == "loss":
            streak += 1
        elif outcome == "win":
            break
        # outcome None (ยังไม่ปิดไม้) ข้ามไปเรื่อยๆ ไม่นับ ไม่ตัด streak
    return streak


# ════════════════════════════════════════════════════════
# [merged from lib/universes.py]
# ════════════════════════════════════════════════════════
# MODULE — UNIVERSE FETCHERS
# ย้ายมาจาก v2.0 ตรงๆ ไม่มีบั๊กในส่วนนี้ที่ต้องแก้ไข เปลี่ยนแค่ตำแหน่งไฟล์
# เพื่อให้ app.py หลักไม่ต้องยาว 1,500+ บรรทัดในไฟล์เดียว



@st.cache_data(ttl=86400)
def fetch_sp500():
    """
    v3.3: Wikipedia บล็อก request จาก IP ของ cloud/datacenter (รวม GitHub
    Actions runner) ด้วย 403 Forbidden แบบไม่สนใจ User-Agent — ยืนยันจาก log
    การรันจริง ตอนนี้ใช้ CSV ที่ดูแลโดยชุมชน (datasets/s-and-p-500-companies
    บน GitHub ซึ่งโฮสต์ผ่าน raw.githubusercontent.com ไม่ถูกบล็อกแบบเดียวกัน)
    เป็นแหล่งหลัก แล้วค่อย fallback ไป Wikipedia (เผื่อรันจาก IP ที่ไม่ถูกบล็อก
    เช่น เครื่องคุณเอง) แล้ว fallback สุดท้ายเป็น list สั้นๆกันพังทั้งหมด
    """
    try:
        import requests
        url = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        from io import StringIO
        df = pd.read_csv(StringIO(resp.text))
        col = "Symbol" if "Symbol" in df.columns else df.columns[0]
        tickers = sorted([str(s).strip().replace(".", "-") for s in df[col].dropna()])
        if len(tickers) > 400:
            return tickers
    except Exception as e:
        log_err("fetch_sp500(github-csv)", e)

    try:
        t = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]
        return sorted([s.replace(".", "-") for s in t["Symbol"].tolist()])
    except Exception as e:
        log_err("fetch_sp500(wikipedia)", e)
        return ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA", "JPM", "V", "PG",
                "UNH", "JNJ", "XOM", "WMT", "MA", "HD", "CVX", "MRK", "ABBV", "KO",
                "PEP", "BAC", "AVGO", "COST", "TMO", "MCD", "CSCO", "ACN", "ABT", "DHR",
                "LIN", "ADBE", "CRM", "NFLX", "TXN", "NEE", "PM", "WFC", "RTX", "ORCL",
                "AMD", "QCOM", "UPS", "INTC", "HON", "UNP", "LOW", "IBM", "AMGN", "SBUX"]


@st.cache_data(ttl=86400)
def fetch_nasdaq100():
    """v3.3: Wikipedia 403 บล็อกจาก cloud IP เหมือนกับ fetch_sp500 — ยังไม่เจอ
    CSV ทางเลือกที่ verified ว่าเสถียรพอสำหรับ index นี้โดยเฉพาะ จึงพยายาม
    ดึงจาก Wikipedia ก่อน (อาจสำเร็จถ้ารันจาก IP ที่ไม่ถูกบล็อก) แล้ว fallback
    เป็น list ที่ใหญ่ขึ้นมาก (~95 ตัว เทียบจาก 10 ตัวเดิม) ถ้าดึงไม่ได้จริงๆ"""
    try:
        tables = pd.read_html("https://en.wikipedia.org/wiki/Nasdaq-100")
        for t in tables:
            cols = [c.lower() for c in t.columns]
            if "ticker" in cols or "symbol" in cols:
                col = "Ticker" if "Ticker" in t.columns else "Symbol"
                tk = [str(x).replace(".", "-") for x in t[col].dropna() if len(str(x)) <= 6]
                if len(tk) > 50:
                    return sorted(tk)
    except Exception as e:
        log_err("fetch_nasdaq100(wikipedia)", e)
    return sorted(["AAPL","MSFT","AMZN","NVDA","GOOGL","GOOG","META","TSLA","AVGO","COST",
        "NFLX","AMD","PEP","ADBE","CSCO","TMUS","INTC","CMCSA","QCOM","TXN",
        "AMAT","INTU","ISRG","HON","AMGN","BKNG","VRTX","SBUX","MDLZ","GILD",
        "ADI","REGN","PANW","LRCX","MU","PYPL","SNPS","CDNS","KLAC","MAR",
        "ORLY","CTAS","ASML","ABNB","MRVL","FTNT","CRWD","ADSK","NXPI","MNST",
        "PCAR","ROST","PAYX","KDP","ODFL","AEP","EXC","IDXX","FAST","EA",
        "CSGP","CPRT","DXCM","BIIB","GEHC","ON","MCHP","WBD","ANSS","TTD",
        "CCEP","DASH","MDB","TEAM","ZS","GFS","ILMN","WDAY","VRSK","CTSH",
        "BKR","XEL","DDOG","CDW","FANG","CHTR","LULU","MELI","EBAY","KHC",
        "TTWO","ALGN","ARM","APP","AXON","DECK","PLTR","CSX","GEN","LIN"])



@st.cache_data(ttl=86400)
def fetch_russell2000():
    # ⚠️ v3.5: list นี้เป็นของแข็ง พิมพ์ไว้ตายตัว ไม่ได้ดึงสดจาก Russell index
    # provider จริง (ไม่มี API ฟรีที่เชื่อถือได้สำหรับ index นี้) ตามเวลาที่ผ่านไป
    # บริษัทเข้า-ออก index จริงจะไม่ตรงกับ list นี้อีกต่อไป ควรเข้ามาอัปเดตเอง
    # เป็นระยะ (ดูรายชื่อล่าสุดได้จาก ETF อย่าง IWM ที่ track index นี้)
    return sorted(["ACVA","ALKT","ARCB","BJRI","CALX","CATO","CBRL","CLFD","COKE","CPSS",
        "CRAI","CRGY","CSWI","CVCO","DCOM","DFIN","DKNG","DNOW","DXPE","ECPG",
        "EFSC","EGHT","EPIX","ESCA","ETON","EVRI","EXPI","FBMS","FBNC","FCPT",
        "FFBC","FFIN","FISI","FIZZ","FLGT","FLNC","FMAO","FMNB","GDEN","GIII",
        "GNTY","GPOR","HAFC","HALO","HCAT","HCKT","HCSG","HIFS","HMST","HNVR",
        "HOPE","HTBK","HTLD","HURN","HWKN","HZO","IART","IBCP","IBP","IBTX",
        "ICAD","ICFI","JACK","JAMF","KALU","KLIC","KNSL","KTOS","LBRT","LCII",
        "LDOS","LECO","LEVI","LGND","LMAT","LMND","LNTH","LOCO","LUNA","LYFT",
        "MATX","MBLY","MEDP","MGNI","MLKN","MMSI","MORN","MRTN","MTSI","NABL",
        "NARI","NATI","NMIH","NOVT","NSIT","NTNX","NVST","OCGN","OMCL","ONTO",
        "OPCH","OSIS","PACK","PAHC","PCOR","PCRX","PDCO","PENN","PGNY","PLXS",
        "PODD","POWL","PRDO","PRGS","PRIM","PRLD","PSMT","PSTG","PTCT","PUMP",
        "QDEL","QTWO","RAMP","RARE","RCKT","RDNT","RGEN","RIOT","RNST","ROCK",
        "RPRX","RYTM","SAFE","SAGE","SAIA","SATS","SBCF","SFNC","SHLS","SHOO",
        "SILK","SITM","SKYW","SMCI","SMPL","SNOW","SNPS","SOUN","SPSC","STAA",
        "STNE","STRL","SUMO","SUPN","SWAV","SWKS","TASK","TDOC","TMDX","TORC",
        "TRMK","TRNO","TROW","TRST","TTGT","TTMI","TWST","UBCP","UCTT","UDMY",
        "ULCC","UNFI","UPST","USNA","USTR","VBTX","VERA","VIAV","VIRT","VLCN",
        "VNDA","VRNS","VRNT","VSEC","VSTO","WAFD","WERN","WEYS","WINA","WKME",
        "WOLF","WOOF","WSFS","WTFC","XPEL","XPOF","YELP","ZEUS","ZLAB","ZYXI"])


@st.cache_data(ttl=86400)
def fetch_set():
    # ⚠️ v3.5: list นี้เป็นของแข็งเหมือนกัน — หุ้นไทยเข้า-ออก SET/mai index
    # จริงเปลี่ยนเป็นระยะ ควรเข้ามาเช็ค/อัปเดตเองทุก 6-12 เดือน
    base = ["ADVANC","AOT","AWC","BANPU","BBL","BDMS","BEM","BGRIM","BH","BJC",
            "BTS","CBG","CENTEL","CK","CPALL","CPF","CPN","CRC","DELTA","EA",
            "EGCO","GULF","HANA","HMPRO","INTUCH","IVL","JMT","KBANK","KCE",
            "KKP","KTB","KTC","LH","MAKRO","MBK","MINT","MTC","OR","OSP",
            "PTT","PTTEP","PTTGC","RATCH","SCB","SCC","SCGP","SIRI","SPALI",
            "THAI","TISCO","TOP","TRUE","TU","VGI","WHAUP","WORK"]
    mai = ["2S","ACAP","AMA","BFC","BFIT","CRANE","CSP","DCC","EARTH","EPG",
           "GENCO","HAPPY","HOME","ITEL","JWD","LEO","MASTER","MFEC","KISS"]
    return sorted([f"{t}.BK" for t in base + mai])


@st.cache_data(ttl=86400)
def fetch_etfs():
    # ⚠️ v3.5: list นี้เป็นของแข็ง — ETF ใหม่ๆที่ออกมาทีหลังจะไม่ถูกรวมอัตโนมัติ
    return sorted(["XLK","XLV","XLF","XLE","XLI","XLB","XLP","XLU","XLRE","XLC","XLY",
        "QQQ","QQQM","SOXX","SMH","HACK","IGV","WCLD","IWM","IWO","MDY","IJR",
        "EEM","EWJ","EWZ","FXI","VEA","VWO","INDA","TUR","EWY","EWT",
        "ARKK","ARKQ","ARKG","ARKF","ARKW","BOTZ","ROBO","AIQ",
        "GLD","SLV","GDX","GDXJ","USO","COPX",
        "TQQQ","SOXL","SPXL","TLT","HYG","LQD","EMB",
        "VYM","SCHD","VIG","NOBL"])


@st.cache_data(ttl=86400)
def fetch_broad_us():
    sp = fetch_sp500()
    nd = fetch_nasdaq100()
    extra = ["AEHR","ALEC","AMBA","AMKR","APPF","ARWR","ATRC","AZEK","BILL",
             "BIRK","BLKB","BURL","CACC","CAKE","CALM","CARG","CELH","CENTA",
             "CHDN","CHEF","CHUY","CIVI","CLFD","COMP","COOP","CRDO","CROX",
             "CWST","DAKT","DDOG","DFIN","DKNG","DLTH","DOCN","DOCS","DOOR",
             "DRVN","DXCM","EDIT","EGHT","ENVA","EPAM","ESAB","EVGO","EWBC",
             "EXAS","EXEL","EXPI","FELE","FIGS","FIZZ","FOUR","FROG","GRND",
             "HIMS","HLIT","HUBS","HWKN","IART","IIPR","IMVT","INDB","INFA",
             "INST","IONS","IRTC","ITCI","JACK","JAMF","JOBY","KLIC","KNSL",
             "KTOS","KVYO","LBRT","LEVI","LGND","LMND","LOCO","LYFT","MATX",
             "MBLY","MEDP","MGNI","MLAB","MLKN","MMSI","MORN","MPWR","MRTN",
             "MTSI","NARI","NATI","NKTR","NOVT","NSIT","NTNX","OCGN","OMCL",
             "ONTO","OPCH","PACK","PCOR","PENN","PGNY","PLXS","PODD","POWL",
             "PRDO","PRGS","PRIM","PSMT","PSTG","PUMP","QDEL","QTWO","RAMP",
             "RARE","RCKT","RDNT","RGEN","RIOT","ROCK","RPRX","RYTM","SAGE",
             "SAIA","SATS","SHLS","SHOO","SILK","SITM","SKYW","SMCI","SMPL",
             "SOUN","SPSC","STAA","STNE","STRL","SUMO","SWAV","SWKS","TASK",
             "TDOC","TMDX","TRMK","TROW","TTGT","TTMI","TWST","UCTT","UDMY",
             "ULCC","UPST","USNA","VERA","VIAV","VIRT","VRNS","VRNT","VSEC",
             "WAFD","WERN","WEYS","WINA","WOLF","WSFS","WTFC","XPEL","YELP"]
    return sorted(set(sp + nd + extra))


SECTOR_MAP = {
    "Technology | เทคโนโลยี":     ["AAPL","MSFT","NVDA","GOOGL","META","AVGO","ORCL","AMD","QCOM","TXN","AMAT","MU","LRCX","KLAC","CDNS","SNPS","NXPI","MCHP","ADI","FTNT"],
    "Healthcare | สุขภาพ":        ["UNH","JNJ","LLY","ABBV","MRK","TMO","ABT","DHR","BMY","AMGN","ISRG","VRTX","REGN","GILD","CVS","CI","ELV","HCA","IDXX","DXCM"],
    "Financials | การเงิน":       ["JPM","BAC","WFC","GS","MS","BLK","SCHW","AXP","USB","PNC","COF","TFC","MCO","SPGI","ICE","CME","AON","MMC","CB","PGR"],
    "Consumer | สินค้าอุปโภค":    ["AMZN","TSLA","HD","MCD","NKE","SBUX","TGT","LOW","BKNG","MAR","HLT","YUM","DRI","ROST","TJX","ULTA","LULU","DKNG","WYNN","CZR"],
    "Industrials | อุตสาหกรรม":   ["GE","HON","RTX","LMT","BA","CAT","DE","UPS","FDX","UNP","CSX","NSC","EMR","ETN","PH","ROK","IR","XYL","CARR","OTIS"],
    "Energy | พลังงาน":           ["XOM","CVX","COP","EOG","SLB","MPC","PSX","VLO","PXD","FANG","HAL","BKR","DVN","HES","APA","CTRA","MRO","OXY","WMB","KMI"],
    "Comm Svcs | สื่อสาร":        ["NFLX","DIS","CMCSA","T","VZ","CHTR","TMUS","PARA","FOX","FOXA","WBD","EA","TTWO","RBLX","MTCH","IAC","ZG","ANGI","LYFT","UBER"],
    "Real Estate | อสังหาริมทรัพย์": ["AMT","PLD","CCI","EQIX","PSA","DLR","O","WELL","AVB","EQR","SPG","VTR","ARE","BXP","KIM","REG","NNN","WPC","COLD","IIPR"],
    "Utilities | สาธารณูปโภค":    ["NEE","DUK","SO","D","SRE","AEP","XEL","PCG","EIX","WEC","ES","ETR","FE","PPL","CMS","AES","NI","EVRG","CNP","LNT"],
    "Materials | วัสดุ":          ["LIN","APD","ECL","DD","PPG","NEM","FCX","NUE","VMC","MLM","ALB","BALL","IP","CF","MOS","FMC","CE","RPM","ATI","CMC"],
    "ETFs | กองทุน ETF":          ["SPY","QQQ","IWM","XLK","XLF","XLE","XLV","XLI","XLP","XLU","GLD","TLT","HYG","EEM","EWJ","ARKK","SOXL","TQQQ","VYM","SCHD"],
    "🚀 Space | อวกาศ":              ["RKLB","LMT","NOC","BA","RTX","ASTS","SPCE","LUNR","RDW","KTOS","IRDM","VSAT","MAXR","ASTR","PL","TDY"],
    "🤖 AI | ปัญญาประดิษฐ์":         ["NVDA","MSFT","GOOGL","META","AMD","PLTR","SMCI","AVGO","ARM","AI","SNOW","PATH","BBAI","SOUN","UPST","CRM"],
    "💊 Biotech/Pharma | ยา/ไบโอเทค": ["LLY","UNH","JNJ","MRK","ABBV","VRTX","REGN","GILD","AMGN","MRNA","BNTX","ISRG","BIIB","ALNY","SRPT","RARE"],
    "🏦 Banking | ธนาคาร":           ["JPM","BAC","WFC","C","GS","MS","USB","PNC","TFC","COF","SCHW","BK","STT","FITB","RF","KEY"],
    "⚡ EV/Battery | ไฟฟ้า/แบตเตอรี่": ["TSLA","RIVN","LCID","NIO","LI","XPEV","ALB","LTHM","ENVX","QS","FREY","CHPT","BLNK","PLUG","FCEL","STEM"],
    "🎮 Gaming/Streaming | เกม/สตรีมมิ่ง": ["NFLX","DIS","RBLX","EA","TTWO","NTDOY","SONY","SPOT","PARA","WBD","ATVI","U","RICK","GME","HUYA","DOYU"],
    "🔒 Crypto/Cyber | คริปโต/ไซเบอร์": ["COIN","MSTR","MARA","RIOT","HUT","CLSK","BITF","CRWD","PANW","ZS","FTNT","OKTA","S","NET","CYBR","TENB"],
    "🏠 REIT | กองทุนอสังหา":         ["O","PLD","AMT","EQIX","PSA","DLR","SPG","AVB","EQR","WELL","VTR","ARE","BXP","KIM","REG","IIPR"],
    "🖥️ AI Datacenter | ศูนย์ข้อมูล AI": ["VRT","CEG","VST","TLN","NRG","GEV","MOD","SMCI","PWR","ANET","CIEN","DELL","HPE","IRM","COHR","APLD"],
    # v3.44: เพิ่มตามที่ขอ (ควอนตัม) + อีก 2 หมวดที่เช็คแล้วว่าขาดไปจริง —
    # ทุกตัวตรวจสอบแล้วว่าไม่ซ้ำกับ 20 หมวดเดิม (308 ticker) เลยสักตัว
    "⚛️ Quantum Computing | ควอนตัมคอมพิวติ้ง": ["IONQ","RGTI","QBTS","QUBT","ARQQ","LAES"],
    "🛡️ Defense/Aerospace | ป้องกันประเทศ/อากาศยาน": ["GD","LHX","TXT","HII","BAH"],
    "☢️ Nuclear/SMR | นิวเคลียร์/เครื่องปฏิกรณ์ขนาดเล็ก": ["SMR","OKLO","BWXT","LEU","UEC","CCJ","UUUU"],
}

# v3.31 ข้อ 4: reverse lookup ticker → sector — ใช้บอกบริบทว่าหุ้นตัวนี้อยู่
# sector ไหน แล้วเทียบกับ Sector Heatmap ที่มีอยู่แล้วว่า sector นั้นกำลัง
# "ร้อน" (Bull % สูง) อยู่ไหม เป็นบริบทเพิ่มที่ไม่ต้องคำนวณคะแนนใหม่เลย แค่
# เอาข้อมูลที่มีอยู่แล้ว 2 ชุดมาโยงกัน — หุ้นที่ตัวเลข Support Quality
# เท่ากันเป๊ะ แต่อยู่คนละ sector (sector หนึ่งร้อนแรง อีก sector เย็นชืด)
# ควรรู้สึกต่างกัน แม้ Support จะเหมือนกันก็ตาม
TICKER_TO_SECTOR = {tk: sector for sector, tickers in SECTOR_MAP.items() for tk in tickers}

# v3.20: ย้อนกลับ v3.17 ตามที่ตัดสินใจ — ข้อมูลพื้นฐานของหุ้นเล็ก/ไมโครแคป
# จาก yfinance ไม่ครบเป็นเรื่องปกติ เสี่ยงให้ "Top 100" กลายเป็น "100 ตัวที่
# บังเอิญมีข้อมูลครบ" มากกว่า "100 ตัวที่ดีที่สุดจริง" — กลับไปใช้
# Russell 2000 Small Cap / US Broad Market แบบเดิม (รายชื่อคงที่ ไม่คัดกรอง
# อัตโนมัติ แต่อย่างน้อยไม่มี bias จากข้อมูลที่หายไม่เท่ากัน)
GITHUB_REPO = "bigpk2002/BIGPK"
RELEASE_TAG = "latest-data"

UNIVERSE_OPTIONS = {
    "S&P 500 (503)": fetch_sp500,
    "Nasdaq 100 (101)": fetch_nasdaq100,
    "Russell 2000 Small Cap": fetch_russell2000,
    "US Broad Market (~700)": fetch_broad_us,
    "หุ้นไทย SET/mai": fetch_set,
    "ETF Screener (70)": fetch_etfs,
    "Sector Focus | เลือกตามหมวด": None,
    "Custom Tickers": None,
}


def resolve_tickers(universe: str, sector_choice: list, custom_input: str) -> list:
    """single source of truth สำหรับ resolve รายชื่อ ticker (เหมือน v2.0)"""
    if universe == "Custom Tickers":
        return [t.strip().upper() for t in custom_input.split(",") if t.strip()]
    elif universe == "Sector Focus | เลือกตามหมวด":
        tickers_all = []
        for s in sector_choice:
            tickers_all += SECTOR_MAP.get(s, [])
        return sorted(set(tickers_all))
    else:
        fn = UNIVERSE_OPTIONS.get(universe)
        return fn() if fn else []


# ════════════════════════════════════════════════════════
# [merged from lib/indicators.py]
# ════════════════════════════════════════════════════════
# MODULE — MATH ENGINE
# ทุกฟังก์ชันเหมือน v2.0 เดิม ยกเว้น relative_strength() ที่แก้บั๊กการเทียบวันที่
# (ดู docstring ของฟังก์ชันนั้นสำหรับรายละเอียด)



def wilder_rsi(prices: pd.Series, period: int = 14) -> float:
    if len(prices) < period + 1:
        return np.nan
    d = prices.diff().dropna()
    g = d.clip(lower=0)
    l = (-d).clip(lower=0)
    ag = g.iloc[:period].mean()
    al = l.iloc[:period].mean()
    a = 1.0 / period
    for i in range(period, len(g)):
        ag = a * g.iloc[i] + (1 - a) * ag
        al = a * l.iloc[i] + (1 - a) * al
    return round(100 - 100 / (1 + ag / al), 2) if al > 0 else 100.0


def ema(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(span=n, adjust=False).mean()


def macd(prices: pd.Series):
    ml = ema(prices, 12) - ema(prices, 26)
    sig = ema(ml, 9)
    return round(ml.iloc[-1], 4), round(sig.iloc[-1], 4), round((ml - sig).iloc[-1], 4)


def candle_pattern(df: pd.DataFrame) -> str:
    if len(df) < 2:
        return "—"
    c, p = df.iloc[-1], df.iloc[-2]
    body = abs(c.Close - c.Open)
    rng = c.High - c.Low
    if rng == 0:
        return "—"
    lo_sh = min(c.Close, c.Open) - c.Low
    up_sh = c.High - max(c.Close, c.Open)
    if body / rng < 0.10:
        return "🕯 Doji"
    if lo_sh >= 2 * body and up_sh < body * 0.5:
        return "🔨 Hammer"
    if c.Close > c.Open and p.Close < p.Open and c.Open < p.Close and c.Close > p.Open:
        return "🟢 Engulfing"
    return "—"


def ema_pattern(price, e5, e10, e20, e50, e100, e200) -> tuple:
    vals = [price, e5, e10, e20, e50, e100, e200]
    if any(v is None or (isinstance(v, float) and np.isnan(v)) for v in vals):
        return "—", 0
    parts = []
    score = 0
    if price > e5 > e10 > e20 > e50 > e100 > e200:
        parts.append("🏆 Perfect Uptrend"); score = 5
    elif price > e5 > e10 > e20 > e50 > e200:
        parts.append("📈 Strong Uptrend"); score = 4
    elif e20 > e50 > e200 and price > e20:
        parts.append("✨ Golden Align"); score = 3
    sp = (max(e20, e50, e200) - min(e20, e50, e200)) / e200 * 100
    if sp < 2.5 and price > e200:
        parts.append("🔥 Squeeze"); score = max(score, 4)
    elif sp < 4.0 and price > e200:
        parts.append("⚡ Pre-Squeeze"); score = max(score, 2)
    if e200 < price < e50 and price > e20:
        parts.append("🌱 Early Break"); score = max(score, 3)
    fan = (e5 - e200) / e200 * 100 if e200 > 0 else 0
    if fan > 8 and price > e5 and e5 > e50:
        parts.append("🎯 EMA Fan"); score = max(score, 2)
    if not parts:
        return ("❌ Below EMA200", 0) if price < e200 else ("🔄 Mixed", 1)
    return " · ".join(parts), min(score, 5)


def squeeze_direction(closes: pd.Series) -> tuple:
    if len(closes) < 206:
        return "—", np.nan, np.nan
    e20, e50, e200 = ema(closes, 20), ema(closes, 50), ema(closes, 200)

    def bw(i):
        hi = max(e20.iloc[i], e50.iloc[i], e200.iloc[i])
        lo = min(e20.iloc[i], e50.iloc[i], e200.iloc[i])
        return (hi - lo) / e200.iloc[i] * 100 if e200.iloc[i] > 0 else np.nan

    bw0, bw5 = bw(-1), bw(-6)
    if np.isnan(bw0) or np.isnan(bw5):
        return "—", np.nan, np.nan
    delta = round(bw0 - bw5, 3)
    if delta < -0.4:
        lbl = "🔥 Squeezing"
    elif delta < 0:
        lbl = "⚡ Tightening"
    elif delta < 0.6:
        lbl = "🌱 Just Broke"
    else:
        lbl = "📈 Expanding"
    return lbl, round(bw0, 2), delta


def signal_age(closes: pd.Series) -> int:
    if len(closes) < 202:
        return -1
    e200 = ema(closes, 200)
    for i in range(1, min(31, len(closes) - 1)):
        if closes.iloc[-i - 1] < e200.iloc[-i - 1] and closes.iloc[-i] > e200.iloc[-i]:
            return i - 1
    return -1


def support_age(closes: pd.Series, level: float, band_pct: float = 4.0) -> int:
    """
    v3.31: นับว่าราคาอยู่ในระยะ "ที่แนวรับนี้" (ห่างไม่เกิน band_pct%) มาต่อเนื่อง
    กี่วันแล้ว จากวันนี้ย้อนกลับไป — ใช้ตอบคำถาม "แนวรับนี้สดใหม่ หรือนอนแช่มา
    นานแล้ว" ซึ่งเป็นบริบทเวลาที่ตารางเดิมไม่เคยบอกเลย (มีแต่ Quality Score
    ตัวเดียว ไม่รู้ว่า "ใหม่" หรือ "เก่า") ยึดวิธีเดียวกับ signal_age()/Trend
    Age ที่มีอยู่แล้ว (scan ย้อนหลังจากราคาปัจจุบัน) เพื่อความสม่ำเสมอ

    ⚠️ เป็นค่าประมาณ — ใช้ระดับแนวรับของ "วันนี้" ไล่ย้อนหลังไปเทียบกับราคา
    ในอดีต ไม่ได้คำนวณใหม่ทุกวันว่าระบบตอนนั้นจะบอกระดับเดียวกันหรือไม่
    (ระดับแนวรับเปลี่ยนช้าอยู่แล้วในทางปฏิบัติ จึงเป็นค่าประมาณที่สมเหตุสมผล)
    """
    if pd.isna(level) or level <= 0 or len(closes) < 2:
        return 0
    age = 0
    for i in range(len(closes) - 1, -1, -1):
        dist = abs(closes.iloc[i] - level) / level * 100
        if dist <= band_pct:
            age += 1
        else:
            break
    return max(age - 1, 0)  # วันนี้นับเป็น 0 ถ้าเพิ่งมาถึงวันแรก


def _find_swing_levels(df: pd.DataFrame, col: str, mode: str, lookback: int = 180,
                       swing_window: int = 5, min_bars: Optional[int] = None) -> list:
    """
    v3.15: ฟังก์ชันกลางสำหรับหา "จุดเปลี่ยนทิศ" ในราคา — ใช้ร่วมกันทั้งหา
    แนวรับ (swing low จากคอลัมน์ Low, mode="low") และแนวต้าน (swing high จาก
    คอลัมน์ High, mode="high") เพื่อไม่ให้โค้ด 2 ชุดที่ทำสิ่งเดียวกัน (แค่กลับ
    ทิศ) แยกออกจากกันแล้วแก้ไม่พร้อมกันทีหลัง (จุดที่เคยเป็นความเสี่ยงมาก่อน
    ตอนแก้ find_support_levels หลายรอบใน session นี้)

    สำหรับแต่ละระดับ เก็บ:
      - touch_count: ราคาเคยเข้ามาใกล้ระดับนี้กี่ครั้ง (ภายใน 1% ถือว่ากลุ่มเดียวกัน)
      - avg_bounce_volume_ratio: ปริมาณซื้อขายเฉลี่ยตอนเจอระดับนี้ เทียบค่าเฉลี่ย
      - zone_low/zone_high: ช่วงราคาจริงของกลุ่ม (ไม่ใช่แค่ค่าเฉลี่ยจุดเดียว)

    คืนค่า list ของ dict เรียงจากราคามากไปน้อย
    """
    if min_bars is None:
        min_bars = swing_window * 2 + 30
    if df is None or len(df) < min_bars:
        return []
    recent = df.iloc[-lookback:] if len(df) > lookback else df
    prices = recent[col].values
    vol = recent["Volume"].values
    avg_vol = vol.mean() if vol.mean() > 0 else 1
    n = len(prices)

    raw_swings = []
    for i in range(swing_window, n - swing_window):
        window = prices[i - swing_window:i + swing_window + 1]
        is_extreme = (prices[i] == window.min()) if mode == "low" else (prices[i] == window.max())
        if is_extreme:
            raw_swings.append((float(prices[i]), float(vol[i])))

    if not raw_swings:
        return []

    # รวมจุดที่ใกล้กัน (ภายใน 1%) เป็นระดับเดียวกัน + นับ touch + volume
    raw_swings.sort(key=lambda x: x[0], reverse=True)
    merged = []
    for px, v in raw_swings:
        placed = False
        for grp in merged:
            if abs(px - grp["level"]) / grp["level"] <= 0.01:
                grp["touches"].append(px)
                grp["volumes"].append(v)
                grp["level"] = sum(grp["touches"]) / len(grp["touches"])
                placed = True
                break
        if not placed:
            merged.append({"level": px, "touches": [px], "volumes": [v]})

    results = []
    for grp in merged:
        results.append({
            "level": round(grp["level"], 2),
            "zone_low": round(min(grp["touches"]), 2),
            "zone_high": round(max(grp["touches"]), 2),
            "touch_count": len(grp["touches"]),
            "avg_bounce_volume_ratio": round(float(np.mean(grp["volumes"])) / avg_vol, 2),
        })
    return sorted(results, key=lambda x: x["level"], reverse=True)


def find_support_levels(df: pd.DataFrame, lookback: int = 180, swing_window: int = 5,
                        min_bars: Optional[int] = None) -> list:
    """หาแนวรับจาก swing low (ดู _find_swing_levels สำหรับรายละเอียด logic)"""
    return _find_swing_levels(df, "Low", "low", lookback, swing_window, min_bars)


def find_resistance_levels(df: pd.DataFrame, lookback: int = 180, swing_window: int = 5,
                           min_bars: Optional[int] = None) -> list:
    """
    v3.15: หาแนวต้านจาก swing high — เดิมแอปมีแต่แนวรับ ไม่มีเป้าหมายขาย/
    เป้าหมายทำกำไรเลย ใช้ logic เดียวกับแนวรับทุกอย่าง แค่มองหาจุดสูงสุดแทน
    จุดต่ำสุด (ดู _find_swing_levels)
    """
    return _find_swing_levels(df, "High", "high", lookback, swing_window, min_bars)


def resample_weekly_ohlc(df: pd.DataFrame) -> pd.DataFrame:
    """
    v3.8: รวมแท่งรายวันเป็นรายสัปดาห์ (Open/High/Low/Close/Volume) — ใช้เป็น
    ฐานหาแนวรับแทนแท่งรายวัน ตามที่ขอ (ดูกราฟสัปดาห์เป็นหลักตอนบอกว่า "อยู่ที่
    แนวรับ/ใกล้แนวรับ") เหตุผล: สวิงโลว์รายสัปดาห์ "หนักแน่น" กว่าสวิงโลว์
    รายวันมาก — ราคาต้องหลุดจุดต่ำสุดของทั้งสัปดาห์ ไม่ใช่แค่ไส้เทียนวันเดียว
    แนวรับที่เจอจากกราฟสัปดาห์เลยมักเป็นระดับที่ตลาดจริงๆให้ความสำคัญ ไม่ใช่
    จุด noise รายวัน (ส่วน Signal/Trend/RSI หลักยังเป็นรายวันเหมือนเดิม —
    เปลี่ยนเฉพาะการหาแนวรับเท่านั้น)

    ตัดสัปดาห์ล่าสุดทิ้งถ้ายังไม่ปิดสัปดาห์จริง (เหมือน weekly_trend() — กัน
    แนวรับ "ขยับ" ไปมาทุกวันจนกว่าสัปดาห์จะจบ)
    """
    try:
        w = df.resample("W-FRI").agg({
            "Open": "first", "High": "max", "Low": "min",
            "Close": "last", "Volume": "sum",
        }).dropna()
        if len(w) and df.index[-1].dayofweek != 4:
            w = w.iloc[:-1]
        return w
    except Exception as e:
        log_err("resample_weekly_ohlc", e)
        return pd.DataFrame()


def support_status(price: float, df: pd.DataFrame, e50: float, e200: float, rs20: float = np.nan) -> dict:
    """
    v3.7 — อัปเกรดใหญ่จากเวอร์ชันเดิม: เดิมเลือกแนวรับที่ใกล้ราคาที่สุดเสมอ
    (อาจเป็นแนวรับอ่อนๆที่บังเอิญอยู่ใกล้) ตอนนี้ให้คะแนนความแข็งแกร่ง
    (Support Quality 0-10) จากหลายปัจจัยร่วมกัน แล้วเลือกแนวรับที่ "คุ้มจะดู
    ที่สุด" จริงๆ ไม่ใช่แค่ใกล้สุด

    ปัจจัยที่ให้คะแนน:
      1. Touch count — โดนทดสอบกี่ครั้ง (ยิ่งเยอะยิ่งน่าเชื่อ ปกป้องราคาซ้ำๆ)
      2. Volume confirmation — มีแรงซื้อจริงตอนเด้งกลับไหม
      3. Confluence — Swing Low ตรงกับ EMA50/EMA200/แนวต้านเก่าพอดีไหม
         (แนวรับซ้อนกันจากคนละวิธีคำนวณ มาบรรจบที่จุดเดียวกัน = หนักแน่นกว่า)
      4. ระยะห่างจากราคาปัจจุบัน — ต้องใกล้พอจะมีความหมายตอนนี้
      5. Relative Strength (v3.24) — หุ้นแข็งกว่าตลาดได้โบนัสเล็กน้อย

    v3.8: swing low หา "แนวรับ" (Swing Low) เปลี่ยนมาใช้แท่ง**รายสัปดาห์**
    แทนรายวัน (ตามที่ขอ) — resample เป็นรายสัปดาห์ก่อนหาสวิงโลว์ ยังไม่ยิง
    Yahoo เพิ่ม (resample จาก df รายวันที่มีอยู่แล้ว) EMA50/EMA200 ยังคงเป็น
    รายวันเหมือนเดิม (เป็นคนละส่วนกับการหาสวิงโลว์)

    v3.14: เพิ่ม zone_low/zone_high/zone_label — เดิมคืนแค่ "level" เป็นจุดราคา
    เดียวเป๊ะๆ (เช่น 62.40) ซึ่งไม่ตรงกับการใช้งานจริง เพราะแนวรับจริงๆมักเป็น
    "โซน" ราคา ไม่ใช่เส้นตรงเป๊ะ ตอนนี้ถ้าแนวรับมาจาก Swing Low ที่ถูกรวมกลุ่ม
    จากหลายจุด (touches) จะโชว์เป็นช่วง (เช่น "60.20–65.40") จากค่าต่ำสุด-สูงสุด
    ของกลุ่มนั้นจริงๆ ถ้ามาจาก EMA50/EMA200 (เป็นเส้นเดียว ไม่ใช่กลุ่ม) จะโชว์
    เป็นจุดเดียวตามเดิม

    v3.24: เพิ่ม 2 แหล่งข้อมูล/ปัจจัยจากการวิเคราะห์ทบทวนอัลกอริทึม:
      - **Polarity Principle** — จุดที่เคยเป็น "แนวต้าน" ในอดีต (swing high)
        แล้วราคาทะลุขึ้นไปแล้ว มักกลายเป็นแนวรับเมื่อราคาย่อกลับมา (หลักการ
        TA ที่มีการันตีจากตำราหลายเล่ม ไม่ใช่การเดา) — ใช้ find_resistance_levels()
        ที่มีอยู่แล้ว หาจุดที่เคยเป็นแนวต้านแต่ตอนนี้อยู่ต่ำกว่าราคาปัจจุบัน
        เพิ่มเป็นแหล่งแนวรับที่ 3 (นอกจาก Swing Low และ EMA)
      - **RS Modifier** — หุ้นที่แข็งกว่าตลาด (RS 20D เป็นบวก) แล้วมาอยู่ที่
        แนวรับ น่าเชื่อกว่าหุ้นที่อ่อนกว่าตลาดมาก เพราะถ้าตลาดรวมร่วง แนวรับ
        จะพังง่ายกว่าไม่ว่าเทคนิคัลจะดูดีแค่ไหน — ให้โบนัส/หักคะแนนเล็กน้อย
        (ไม่ให้มีน้ำหนักเกิน 1 แต้ม เพราะเป็นแค่ตัวปรับ ไม่ใช่ปัจจัยหลัก)

    คืนค่า dict: {status, level, distance_pct, quality_score, touch_count,
                  volume_confirmed, confluence, zone_low, zone_high, zone_label}
    """
    empty = {"status": "—", "level": np.nan, "distance_pct": np.nan,
             "quality_score": 0, "touch_count": 0, "volume_confirmed": False, "confluence": False,
             "zone_low": np.nan, "zone_high": np.nan, "zone_label": "—", "age_days": 0}

    weekly_df = resample_weekly_ohlc(df)
    swing_levels = find_support_levels(weekly_df, lookback=52, swing_window=2, min_bars=14)
    candidates = []
    for sw in swing_levels:
        if sw["level"] <= price:
            candidates.append({"source": "Swing Low", "level": sw["level"],
                               "zone_low": sw["zone_low"], "zone_high": sw["zone_high"],
                               "touch_count": sw["touch_count"],
                               "vol_ratio": sw["avg_bounce_volume_ratio"]})
    # v3.24 Polarity Principle: แนวต้านเก่าที่ราคาทะลุขึ้นไปแล้ว มักกลายเป็น
    # แนวรับ — หาจาก find_resistance_levels() ตัวเดิม กรองเอาเฉพาะจุดที่อยู่
    # ต่ำกว่าราคาปัจจุบันแล้ว (แปลว่าทะลุขึ้นมาแล้วจริง ไม่ใช่แนวต้านที่ยังไม่โดนทะลุ)
    old_resistance = find_resistance_levels(weekly_df, lookback=52, swing_window=2, min_bars=14)
    for r in old_resistance:
        if r["level"] <= price:
            candidates.append({"source": "Old Resistance (Polarity)", "level": r["level"],
                               "zone_low": r["zone_low"], "zone_high": r["zone_high"],
                               "touch_count": r["touch_count"],
                               "vol_ratio": r["avg_bounce_volume_ratio"]})
    if e50 > 0 and e50 <= price:
        candidates.append({"source": "EMA50", "level": e50, "zone_low": e50, "zone_high": e50,
                           "touch_count": 1, "vol_ratio": 1.0})
    if e200 > 0 and e200 <= price:
        candidates.append({"source": "EMA200", "level": e200, "zone_low": e200, "zone_high": e200,
                           "touch_count": 1, "vol_ratio": 1.0})

    if not candidates:
        return empty

    # คำนวณคะแนนความแข็งแกร่งของแนวรับแต่ละตัว แล้วเลือกตัวที่ดีที่สุด
    # (ไม่ใช่แค่ใกล้สุด — แนวรับใกล้แต่อ่อนอาจแพ้แนวรับไกลกว่านิดหน่อยแต่แข็งแรงกว่ามาก)
    rs_bonus = 0.0
    if pd.notna(rs20):
        if rs20 > 5:
            rs_bonus = 1.0
        elif rs20 > 0:
            rs_bonus = 0.5
        elif rs20 < -10:
            rs_bonus = -1.0
        elif rs20 < -5:
            rs_bonus = -0.5

    scored = []
    for c in candidates:
        dist = (price - c["level"]) / c["level"] * 100
        if dist > 6.0:  # ไกลเกินจะมีความหมาย ตัดทิ้งตั้งแต่ขั้นนี้
            continue
        # Confluence: มีแนวรับอื่นอยู่ใกล้กันภายใน 1.5% ไหม (ซ้อนกันจากคนละแหล่ง)
        confluence = any(
            other is not c and abs(other["level"] - c["level"]) / c["level"] <= 0.015
            for other in candidates
        )
        touch_score = min(c["touch_count"], 4) * 1.5          # สูงสุด 6 คะแนน
        volume_score = 2.0 if c["vol_ratio"] >= 1.3 else (1.0 if c["vol_ratio"] >= 1.0 else 0)
        confluence_score = 2.0 if confluence else 0
        proximity_score = max(0, 1.0 - dist / 6.0)              # ใกล้กว่า = คะแนนเพิ่มเล็กน้อย
        quality = round(touch_score + volume_score + confluence_score + proximity_score + rs_bonus, 1)
        scored.append({**c, "distance_pct": round(dist, 2), "confluence": confluence,
                       "quality_score": max(0, min(quality, 10.0))})

    if not scored:
        return empty

    best = max(scored, key=lambda x: x["quality_score"])

    if best["distance_pct"] <= 1.5:
        status = "🟢 อยู่ที่แนวรับ"
    elif best["distance_pct"] <= 4.0:
        status = "🟡 ใกล้แนวรับ"
    else:
        status = "—"

    age = support_age(df["Close"], best["level"]) if status != "—" else 0

    return {
        "status": status, "level": round(best["level"], 2), "distance_pct": best["distance_pct"],
        "quality_score": best["quality_score"], "touch_count": best["touch_count"],
        "volume_confirmed": best["vol_ratio"] >= 1.3, "confluence": best["confluence"],
        "zone_low": best["zone_low"], "zone_high": best["zone_high"],
        "zone_label": (f'{best["zone_low"]:.2f}–{best["zone_high"]:.2f}'
                      if best["zone_high"] > best["zone_low"] else f'{best["level"]:.2f}'),
        "age_days": age,
    }


def resistance_status(price: float, df: pd.DataFrame, e50: float, e200: float) -> dict:
    """
    v3.15: แนวต้าน (Resistance) — เดิมแอปมีแต่แนวรับ ไม่มี "เป้าหมายขาย/
    ทำกำไร" ให้เทียบเลย ใช้ logic เดียวกับ support_status() ทุกอย่างแค่กลับทิศ:
      - หา swing high จากกราฟรายสัปดาห์ (เหตุผลเดียวกับแนวรับ — swing high
        รายสัปดาห์หนักแน่นกว่ารายวัน)
      - มองหาเฉพาะระดับที่อยู่ "เหนือ" ราคาปัจจุบัน (level >= price)
      - ให้คะแนนความแข็งแกร่งจากปัจจัยเดียวกัน (touch count, volume,
        confluence, ระยะห่าง)
      - EMA50/EMA200 นับเป็นแนวต้านได้เฉพาะตอนที่อยู่เหนือราคาปัจจุบัน
        (ต่างจาก support ที่ต้องอยู่ใต้ราคา)

    คืนค่า dict โครงสร้างเดียวกับ support_status(): {status, level,
    distance_pct, quality_score, touch_count, volume_confirmed, confluence,
    zone_low, zone_high, zone_label}
    """
    empty = {"status": "—", "level": np.nan, "distance_pct": np.nan,
             "quality_score": 0, "touch_count": 0, "volume_confirmed": False, "confluence": False,
             "zone_low": np.nan, "zone_high": np.nan, "zone_label": "—"}

    weekly_df = resample_weekly_ohlc(df)
    swing_levels = find_resistance_levels(weekly_df, lookback=52, swing_window=2, min_bars=14)
    candidates = []
    for sw in swing_levels:
        if sw["level"] >= price:
            candidates.append({"source": "Swing High", "level": sw["level"],
                               "zone_low": sw["zone_low"], "zone_high": sw["zone_high"],
                               "touch_count": sw["touch_count"],
                               "vol_ratio": sw["avg_bounce_volume_ratio"]})
    if e50 > 0 and e50 >= price:
        candidates.append({"source": "EMA50", "level": e50, "zone_low": e50, "zone_high": e50,
                           "touch_count": 1, "vol_ratio": 1.0})
    if e200 > 0 and e200 >= price:
        candidates.append({"source": "EMA200", "level": e200, "zone_low": e200, "zone_high": e200,
                           "touch_count": 1, "vol_ratio": 1.0})

    if not candidates:
        return empty

    scored = []
    for c in candidates:
        dist = (c["level"] - price) / price * 100  # เป็นบวกเสมอเพราะแนวต้านอยู่เหนือราคา
        if dist > 6.0:
            continue
        confluence = any(
            other is not c and abs(other["level"] - c["level"]) / c["level"] <= 0.015
            for other in candidates
        )
        touch_score = min(c["touch_count"], 4) * 1.5
        volume_score = 2.0 if c["vol_ratio"] >= 1.3 else (1.0 if c["vol_ratio"] >= 1.0 else 0)
        confluence_score = 2.0 if confluence else 0
        proximity_score = max(0, 1.0 - dist / 6.0)
        quality = round(touch_score + volume_score + confluence_score + proximity_score, 1)
        scored.append({**c, "distance_pct": round(dist, 2), "confluence": confluence,
                       "quality_score": min(quality, 10.0)})

    if not scored:
        return empty

    # เลือกแนวต้านที่ "ใกล้ที่สุด" ก่อนเป็นหลัก (ต่างจาก support ที่เลือกจาก
    # quality_score ล้วนๆ) เพราะการใช้งานจริงของแนวต้านคือหาเป้าหมายขาย/ทำกำไร
    # ถัดไปที่ "จะเจอก่อน" ไม่ใช่แนวต้านที่แข็งแกร่งที่สุดแต่ไกลลิบ
    best = min(scored, key=lambda x: x["distance_pct"])

    if best["distance_pct"] <= 1.5:
        status = "🔴 อยู่ที่แนวต้าน"
    elif best["distance_pct"] <= 4.0:
        status = "🟠 ใกล้แนวต้าน"
    else:
        status = "—"

    return {
        "status": status, "level": round(best["level"], 2), "distance_pct": best["distance_pct"],
        "quality_score": best["quality_score"], "touch_count": best["touch_count"],
        "volume_confirmed": best["vol_ratio"] >= 1.3, "confluence": best["confluence"],
        "zone_low": best["zone_low"], "zone_high": best["zone_high"],
        "zone_label": (f'{best["zone_low"]:.2f}–{best["zone_high"]:.2f}'
                      if best["zone_high"] > best["zone_low"] else f'{best["level"]:.2f}'),
    }


def quiet_accumulation(volumes: pd.Series, closes: pd.Series, rsi: float, n: int = 10) -> tuple:
    if len(volumes) < n or len(closes) < n:
        return 0, "—"
    rv = volumes.iloc[-n:]
    rc = closes.iloc[-n:]
    slope = np.polyfit(range(n), rv.values, 1)[0] > 0
    ranges = [abs(rc.iloc[i] - rc.iloc[i - 1]) / rc.iloc[i - 1] * 100 for i in range(1, n)]
    low_v = np.mean(ranges) < 2.5
    rsi_ok = not np.isnan(rsi) and rsi < 62
    va = volumes.iloc[-30:].mean() if len(volumes) >= 30 else volumes.mean()
    vr = volumes.iloc[-1] / va if va > 0 else 0
    sweet = 1.05 < vr < 2.5
    e20s = np.polyfit(range(5), ema(closes, 20).iloc[-5:].values, 1)[0] > 0 if len(closes) >= 20 else False
    score = sum([slope, low_v, rsi_ok, sweet, e20s])
    lbl = {5: "🔬 Stealth Accum", 4: "📦 Quiet Accum", 3: "🔍 Possible Accum", 2: "👀 Watch", 1: "—", 0: "—"}[score]
    return score, lbl


def weekly_trend(df: pd.DataFrame) -> tuple:
    """
    v3.7: เทรนด์รายสัปดาห์ — เพิ่มเป็น "ตัวกรองเสริม" คู่กับ Trend รายวันเดิม
    ไม่ได้เปลี่ยนทั้งระบบไปเป็นรายสัปดาห์ เพราะ threshold เดิมทั้งหมด (RSI,
    MACD, Volume ฯลฯ ใน quiet_accumulation) tune ไว้บนพฤติกรรมแท่งรายวัน
    โดยเฉพาะ เปลี่ยนทั้งระบบจะทำให้ตัวชี้วัดเดิมเพี้ยนหมด

    ใช้ resample() จากข้อมูลรายวันที่ analyze() ดึงมาอยู่แล้ว — ไม่ยิง Yahoo
    เพิ่มอีก request ต่อ ticker

    ใช้ EMA10/EMA20 ของแท่งสัปดาห์ (ไม่ใช่ EMA200) เพราะข้อมูลที่มีอยู่คือ
    period="1y" รายวัน (~52 แท่งสัปดาห์) ไม่พอคำนวณ EMA200 สัปดาห์ให้แม่นยำ
    ถ้าจะทำ EMA200 สัปดาห์จริงต้องดึงย้อนหลัง 4-5 ปี ซึ่งเพิ่มภาระ/เวลาการ
    ดึงข้อมูลทั้งระบบไปอีกมาก จึงยังไม่ทำในเวอร์ชันนี้

    กัน repainting: ถ้าแท่งรายวันล่าสุดยังไม่ใช่วันศุกร์ (ตลาดปิดสัปดาห์)
    ตัดแท่งสัปดาห์ล่าสุด (ที่ยังไม่ปิดจริง) ทิ้งก่อนคำนวณ ไม่งั้นตัวเลขของ
    "สัปดาห์นี้" จะขยับไปมาทุกวันจนกว่าสัปดาห์จะจบ
    """
    try:
        w = df["Close"].resample("W-FRI").last().dropna()
        if len(w) and df.index[-1].dayofweek != 4:
            w = w.iloc[:-1]
        if len(w) < 21:
            return "—", np.nan
        e10w = ema(w, 10).iloc[-1]
        e20w = ema(w, 20).iloc[-1]
        pw = w.iloc[-1]
        chg = round((pw - e20w) / e20w * 100, 2) if e20w > 0 else np.nan
        if pw > e10w > e20w:
            return "🟢 Weekly Bull", chg
        if pw < e10w < e20w:
            return "🔴 Weekly Bear", chg
        return "🟡 Weekly Mixed", chg
    except Exception as e:
        log_err("weekly_trend", e)
        return "—", np.nan


def relative_strength(closes: pd.Series, bench: pd.Series, period: int = 20) -> float:
    """
    เทียบ % การเปลี่ยนแปลงของหุ้นกับ benchmark (เช่น SPY) ใน N แท่งล่าสุด

    FIX (v3.0) — เดิม v2.0 เทียบโดยใช้ "ตำแหน่ง" (closes.iloc[-period] vs
    spy.iloc[-period]) ตรงๆ ระหว่าง 2 ซีรีส์ ซึ่งถูกต้องเฉพาะกรณีทั้งคู่มี
    ปฏิทินวันเทรดเหมือนกันทุกวันเท่านั้น (เช่น หุ้นสหรัฐฯ เทียบกับ SPY ซึ่งใช้
    ปฏิทิน NYSE เหมือนกัน) แต่ผิดทันทีถ้าเทียบ "หุ้นไทย .BK" กับ SPY เพราะ
    วันหยุดตลาดไทยกับสหรัฐฯ ไม่ตรงกัน ทำให้ "20 แท่งที่แล้ว" ของหุ้นไทยกับของ
    SPY ไม่ใช่วันเดียวกันจริง — ค่า RS ที่ได้คลาดเคลื่อนโดยไม่มี error ใดๆ
    ขึ้นเตือนเลย (silent bug)

    ตอนนี้ join ทั้งสองซีรีส์ด้วย "วันที่จริง" ก่อนคำนวณ (ผ่าน
    utils.to_date_indexed) เพื่อให้แน่ใจว่าเทียบช่วงเวลาเดียวกันเสมอ ไม่ว่า
    หุ้นจะมาจากตลาดไหน
    """
    if closes is None or bench is None:
        return np.nan
    if len(closes) < 2 or len(bench) < 2:
        return np.nan
    try:
        s = to_date_indexed(closes).rename("s")
        b = to_date_indexed(bench).rename("b")
        aligned = pd.concat([s, b], axis=1).dropna()
        if len(aligned) < period + 1:
            return np.nan
        sr = (aligned["s"].iloc[-1] - aligned["s"].iloc[-period]) / aligned["s"].iloc[-period] * 100
        br = (aligned["b"].iloc[-1] - aligned["b"].iloc[-period]) / aligned["b"].iloc[-period] * 100
        return round(sr - br, 2)
    except Exception as e:
        log_err("relative_strength", e)
        return np.nan


def gem_score(pat_score, acc_score, vol20, rsi, drawdown, mktcap_b) -> tuple:
    s = min(pat_score, 4)
    s += min(acc_score, 3)
    if 1.1 <= vol20 <= 2.0:
        s += 1
    if 40 <= rsi <= 62:
        s += 1
    if isinstance(mktcap_b, float) and 0 < mktcap_b < 10:
        s += 1
    s = min(s, 10)
    lbl = "💎 Hidden Gem" if s >= 8 else "🔭 Emerging Gem" if s >= 6 else "👀 Watch" if s >= 4 else "—"
    return s, lbl


def conservative_stars(price, e200, rsi, vol20, drawdown) -> str:
    s = 0
    if e200 > 0 and abs((price - e200) / e200 * 100) <= 2:
        s += 1
    if rsi < 35:
        s += 1
    if vol20 > 2.0:
        s += 1
    if -15 <= drawdown <= -5:
        s += 1
    return "⭐" * s if s else "—"


# ════════════════════════════════════════════════════════
# [merged from lib/analyzer.py]
# ════════════════════════════════════════════════════════
# MODULE — SINGLE TICKER PIPELINE + BATCH PROCESSOR
# 
# เปลี่ยนจาก v2.0 (รายละเอียดอยู่ในแต่ละ docstring):
#   1. ดึง fundamentals (.info) แยก cache จากราคา/เทคนิคัล + ดึงรอบเดียว
#      (เดิมยิงทั้ง .fast_info และ .info แยกกัน = 2 network call ต่อ ticker
#      ต่อสแกน ทั้งที่ fundamentals ไม่ได้เปลี่ยนรายวัน)
#   2. dividendYield ใช้ guard ตาม magnitude แทนการ assume format คงที่
#      (Yahoo เคยเปลี่ยน format ของ field นี้มาแล้ว — เห็นได้จาก GitHub issues
#      หลายอันใน ranaroussi/yfinance — โค้ดเดิมคูณ 100 เสมอ ถ้า field เปลี่ยน
#      มาเป็น % อยู่แล้วจะได้ yield ผิดเพี้ยนไปมาก)
#   3. retry + exponential backoff ทุก network call (เดิมไม่มี retry เลย)
#   4. batch_scan ใช้ ThreadPoolExecutor ยิง concurrent (เดิม sequential
#      ทีละตัว + sleep คงที่ — ช้าและไม่จำเป็น เพราะงานนี้เป็น I/O-bound)
#   5. relative_strength เรียกด้วยซีรีส์ที่มี date index จริง (ดู indicators.py)
#      แทนการส่ง tuple ของค่าดิบที่ไม่มีวันที่กำกับ




@retry(times=3, base_delay=0.6)
def _download_history(ticker: str, period: str, interval: str) -> pd.DataFrame:
    return yf.Ticker(ticker).history(period=period, interval=interval, auto_adjust=True)


@st.cache_data(ttl=3600)
def _cached_history(ticker: str, period: str, interval: str) -> Optional[pd.DataFrame]:
    try:
        df = _download_history(ticker, period, interval)
        if df is None or df.empty:
            return None
        return df
    except Exception as e:
        log_err(f"history({ticker})", e)
        return None


def _normalize_dividend_yield(raw) -> float:
    """
    Yahoo เคยเปลี่ยน format ของ dividendYield ไปมา (ทศนิยมเช่น 0.024 บางช่วง
    เทียบเท่า 2.4% แต่บางเวอร์ชันคืนค่าเป็น % ตรงๆ คือ 2.4 อยู่แล้ว) เดิม
    v2.0 คูณ 100 เสมอ — ถ้า field เปลี่ยนมาเป็น % แล้วจะได้ yield ผิดเป็น
    240% ทันทีแบบไม่มี error เตือน

    Guard ตรงนี้ใช้ magnitude เป็นตัวตัดสิน: ถ้าค่าที่ได้น้อยกว่า 1 ถือว่า
    เป็นทศนิยม (คูณ 100) ถ้ามากกว่า 1 ถือว่าเป็น % อยู่แล้ว — robust กว่า
    การ assume format คงที่ ไม่ว่า yfinance/Yahoo จะเปลี่ยน field นี้อีกกี่ครั้ง
    """
    if raw is None:
        return np.nan
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return np.nan
    if v <= 0:
        return 0.0
    return round(v * 100, 2) if v < 1 else round(v, 2)


@retry(times=3, base_delay=0.6)
def _download_info(ticker: str) -> dict:
    return yf.Ticker(ticker).info or {}


def _safe_num(val, decimals=2):
    """แปลงค่าเป็น float อย่างปลอดภัย — เคยพบว่า field บางตัวจาก Yahoo (เช่น P/E
    ของ BILL) คืนมาเป็น string แทนตัวเลข ทำให้ round() พังทั้งฟังก์ชันและ field
    อื่นที่ดีอยู่แล้วก็พลอยหายไปด้วย (v3.3 แก้ — เช็คทีละ field แทน)"""
    if val is None:
        return np.nan
    try:
        return round(float(val), decimals)
    except (TypeError, ValueError):
        return np.nan


@st.cache_data(ttl=21600)  # 6 ชม. — fundamentals เปลี่ยนช้ากว่าราคามาก ไม่ต้องดึงซ้ำทุกสแกน
def _cached_fundamentals(ticker: str) -> dict:
    """
    v3.20: ย้อนกลับเป็นเวอร์ชันเดิมก่อน v3.17 — ตัด Fundamental Score /
    Analyst Rec / Upside% ออกทั้งหมด (ดู CHANGELOG v3.20 สำหรับเหตุผล: ตัดคู่
    กับการเอา Micro/Small Cap Value + แท็บ "หุ้นน่าติดตาม" ออก เพราะเป็นตัว
    เดียวที่ใช้ field พวกนี้ ทำให้กลายเป็นโค้ดที่ไม่มีใครเรียกใช้แล้ว)
    """
    try:
        info = _download_info(ticker)
        pe = info.get("trailingPE") or info.get("forwardPE")
        pb = info.get("priceToBook")
        mktcap = info.get("marketCap")
        mktcap_b = (mktcap / 1e9) if isinstance(mktcap, (int, float)) else np.nan
        return {
            "pe": _safe_num(pe),
            "pb": _safe_num(pb),
            "div": _normalize_dividend_yield(info.get("dividendYield")),
            "mktcap_b": _safe_num(mktcap_b),
        }
    except Exception as e:
        log_err(f"fundamentals({ticker})", e)
        return {"pe": np.nan, "pb": np.nan, "div": np.nan, "mktcap_b": np.nan}


@st.cache_data(ttl=3600)
def analyze(ticker: str, period: str = "1y", interval: str = "1d", bench_tuple=None) -> Optional[dict]:
    """
    bench_tuple: tuple ของ (date_iso_string, close) ของ benchmark (เช่น SPY)
    เปลี่ยนจาก v2.0 ที่ส่งเป็น tuple ค่าดิบไม่มีวันที่กำกับ — จำเป็นสำหรับ
    relative_strength() เวอร์ชันใหม่ที่ join ด้วยวันที่จริง
    """
    try:
        df = _cached_history(ticker, period, interval)
        if df is None or len(df) < 30:
            return None
        df = df.copy()
        df.index = pd.to_datetime(df.index)
        cl = df["Close"]
        vl = df["Volume"]
        px = cl.iloc[-1]

        # v3.5: Data validation — Yahoo บางครั้งส่งราคา 0/ติดลบ/NaN มา (ข้อมูล
        # เสีย ไม่ใช่ราคาจริง) ตัดทิ้งตรงนี้เลยก่อนจะเอาไปคำนวณต่อ ป้องกัน
        # ผลลัพธ์ผิดเพี้ยน (เช่น % เปลี่ยนแปลงเป็น inf) หลุดไปแสดงในตาราง
        if pd.isna(px) or px <= 0:
            log_err(f"analyze({ticker})", ValueError(f"ราคาผิดปกติจาก Yahoo: {px}"))
            return None

        ep = {n: ema(cl, n).iloc[-1] for n in [5, 10, 20, 50, 100, 200]}
        ed = {n: round((px - v) / v * 100, 2) if v > 0 else np.nan for n, v in ep.items()}

        rsi_val = wilder_rsi(cl)
        ml, ms, mh = macd(cl)
        v20a = vl.iloc[-20:].mean() if len(vl) >= 20 else vl.mean()
        v3ma = vl.iloc[-63:].mean() if len(vl) >= 63 else vl.mean()
        v6ma = vl.iloc[-126:].mean() if len(vl) >= 126 else vl.mean()
        vc = vl.iloc[-1]
        vm20 = round(vc / v20a, 2) if v20a > 0 else np.nan
        vm3m = round(vc / v3ma, 2) if v3ma > 0 else np.nan
        vm6m = round(vc / v6ma, 2) if v6ma > 0 else np.nan

        hi52 = cl.rolling(min(252, len(cl))).max().iloc[-1]
        draw = round((px - hi52) / hi52 * 100, 2) if hi52 > 0 else np.nan
        prev_c = round(cl.iloc[-2], 2) if len(cl) >= 2 else px

        ytd_start = cl[cl.index.year == datetime.date.today().year]
        base0 = ytd_start.iloc[0] if len(ytd_start) > 1 else cl.iloc[0]
        ytd_ret = round((px - base0) / base0 * 100, 2)

        trend = "🟢 Bull" if px > ep[200] else "🔴 Bear"
        patt = candle_pattern(df)
        stars = conservative_stars(px, ep[200], rsi_val, vm20 or 0, draw or 0)

        ep_lbl, ep_sc = ema_pattern(px, ep[5], ep[10], ep[20], ep[50], ep[100], ep[200])
        acc_sc, acc_lb = quiet_accumulation(vl, cl, rsi_val)
        sq_lbl, bw_now, bw_delta = squeeze_direction(cl)
        age = signal_age(cl)  # จำนวนวันตั้งแต่ราคาข้าม EMA200 ขึ้นมา (ไม่เกี่ยวกับระบบ Signal ที่ตัดออกแล้ว)
        wk_trend, wk_chg = weekly_trend(df)

        # v3.24: ย้าย RS มาคำนวณ "ก่อน" support_status()/resistance_status()
        # เพราะตอนนี้ support_status() รับ rs20 เข้าไปปรับคะแนน Support Quality
        # ด้วย (ดูเหตุผลใน docstring ของ support_status) — เดิมคำนวณทีหลัง
        rs20 = rs50 = np.nan
        if bench_tuple:
            dates, vals = zip(*bench_tuple)
            bench = pd.Series(vals, index=pd.to_datetime(dates))
            rs20 = relative_strength(cl, bench, 20)
            rs50 = relative_strength(cl, bench, 50)

        sup = support_status(px, df, ep[50], ep[200], rs20=rs20)
        res = resistance_status(px, df, ep[50], ep[200])
        fnd = _cached_fundamentals(ticker)
        gs, gl = gem_score(ep_sc, acc_sc, vm20 or 0, rsi_val, draw or 0, fnd["mktcap_b"])

        row_out = {
            "Ticker": ticker, "Price": round(px, 2), "ราคาปิด": prev_c,
            "Sector": TICKER_TO_SECTOR.get(ticker, "—"),
            "Trend": trend, "Phase": ep_lbl, "Stars": stars,
            "EMA5": round(ep[5], 2), "EMA10": round(ep[10], 2), "EMA20": round(ep[20], 2),
            "EMA50": round(ep[50], 2), "EMA100": round(ep[100], 2), "EMA200": round(ep[200], 2),
            "vs EMA5%": ed[5], "vs EMA10%": ed[10], "vs EMA20%": ed[20],
            "vs EMA50%": ed[50], "vs EMA100%": ed[100], "vs EMA200%": ed[200],
            "RSI": rsi_val, "MACD": ml, "Signal_L": ms, "MACD_H": mh,
            "Vol×20D": vm20, "Vol×3M": vm3m, "Vol×6M": vm6m,
            "YTD%": ytd_ret, "Drawdown%": draw, "High52W": round(hi52, 2),
            "vs52W%": round((px - hi52) / hi52 * 100, 2) if hi52 > 0 else np.nan,
            "Candle": patt, "EMA Pattern": ep_lbl, "Pat Score": ep_sc,
            "Accum": acc_lb, "Accum Score": acc_sc, "Gem Score": gs, "💎 Gem": gl,
            "Squeeze": sq_lbl, "BW%": bw_now, "BW Δ5d": bw_delta, "Trend Age": age,
            "Support": sup["status"], "Support Level": sup["level"], "Support Dist%": sup["distance_pct"],
            "Support Zone": sup["zone_label"],
            "Support Quality": sup["quality_score"], "Support Touches": sup["touch_count"],
            "Support Vol Confirmed": sup["volume_confirmed"], "Support Confluence": sup["confluence"],
            "Support Age": sup.get("age_days", 0),
            "Resistance": res["status"], "Resistance Zone": res["zone_label"],
            "Resistance Dist%": res["distance_pct"], "Resistance Quality": res["quality_score"],
            "Resistance Level": res["level"],
            "Weekly Trend": wk_trend, "Weekly vs EMA20w%": wk_chg,
            "RS 20D": rs20, "RS 50D": rs50,
            "P/E": fnd["pe"], "P/BV": fnd["pb"], "Div%": fnd["div"], "MktCap$B": fnd["mktcap_b"],
        }
        # v3.31 ข้อ 2: Risk:Reward — เอา Support (ความเสี่ยง) + Resistance
        # (เป้าหมาย) ที่คำนวณไว้แล้วทั้งคู่มารวมเป็นตัวเลขเดียวที่ตัดสินใจได้
        # ทันที ไม่ต้องเข้า Deep Dive ทีละตัวถึงจะเห็น — ไม่ใช่คะแนนใหม่ แค่
        # หารเลขที่ validate แล้วทั้งสองฝั่ง
        risk = px - sup["level"] if pd.notna(sup["level"]) else np.nan
        reward = res["level"] - px if pd.notna(res["level"]) else np.nan
        rr_ratio = np.nan
        if pd.notna(risk) and pd.notna(reward) and risk > 0:
            rr_ratio = round(reward / risk, 2)
        row_out["Risk:Reward"] = rr_ratio
        return row_out
    except Exception as e:
        log_err(f"analyze({ticker})", e)
        return None


def make_bench_tuple(bench_df: pd.DataFrame) -> tuple:
    """แปลง DataFrame ราคาของ benchmark (เช่น SPY) เป็น tuple ของ (date_iso, close)
    เพื่อให้ผ่าน st.cache_data ได้ (ต้อง hashable) พร้อมคงวันที่ไว้สำหรับ
    relative_strength() เวอร์ชันใหม่ — เดิม v2.0 ส่งแค่ tuple(values) ทำให้
    วันที่หายไปตั้งแต่จุดนี้"""
    idx = pd.to_datetime(bench_df.index)
    if getattr(idx, "tz", None) is not None:
        idx = idx.tz_localize(None)
    return tuple(zip(idx.strftime("%Y-%m-%d"), bench_df["Close"].values.tolist()))


def batch_scan(
    tickers: tuple,
    period: str = "1y",
    interval: str = "1d",
    bench_tuple=None,
    max_workers: int = 6,
    progress_cb: Optional[Callable[[int, int], None]] = None,
) -> pd.DataFrame:
    """
    เดิม v2.0 สแกนทีละตัว sequential (sleep 0.4 วินาทีทุกๆ 25 ตัว) — สแกน
    300 ตัวต้องรอ network round-trip ของตัวก่อนหน้าจบก่อนถึงจะเริ่มตัวต่อไป
    ตอนนี้ใช้ ThreadPoolExecutor ยิง concurrent เพราะงานนี้เป็น I/O-bound
    (รอ network) ไม่ใช่ CPU-bound — max_workers ถูกจำกัดไว้ไม่สูงเกินไป
    เพื่อลดความเสี่ยงโดน Yahoo rate-limit จาก request ที่ถี่เกินไป
    """
    results = []
    total = len(tickers)
    done = 0
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(analyze, tk, period, interval, bench_tuple): tk for tk in tickers}
        for fut in as_completed(futures):
            done += 1
            try:
                d = fut.result()
                if d:
                    results.append(d)
            except Exception as e:
                log_err(f"batch_scan({futures[fut]})", e)
            if progress_cb:
                progress_cb(done, total)
    return pd.DataFrame(results) if results else pd.DataFrame()


def fetch_live(ticker: str) -> dict:
    try:
        fi = yf.Ticker(ticker).fast_info
        px = getattr(fi, "last_price", None)
        pc = getattr(fi, "previous_close", None)
        chg = round((px - pc) / pc * 100, 2) if px and pc else None
        return {
            "price": round(px, 2) if px else "N/A",
            "change": chg,
            "high": round(getattr(fi, "day_high", 0) or 0, 2),
            "low": round(getattr(fi, "day_low", 0) or 0, 2),
            "vol": f"{int(getattr(fi, 'last_volume', 0) or 0):,}",
            "cap": f"${(getattr(fi, 'market_cap', 0) or 0) / 1e9:.1f}B",
        }
    except Exception as e:
        log_err(f"fetch_live({ticker})", e)
        return {}


# ════════════════════════════════════════════════════════
# [merged from lib/backtest.py]
# ════════════════════════════════════════════════════════
# MODULE — BACKTESTER
# 
# เปลี่ยนจาก v2.0:
#   1. เข้าซื้อที่ "ราคาเปิดของแท่งถัดไป" (i+1) ไม่ใช่ "ราคาปิดของแท่งที่เกิด
#      สัญญาณ" (i) — เดิมใช้ close ของแท่งเดียวกับที่คำนวณสัญญาณ ซึ่งในทาง
#      ปฏิบัติเทรดจริงทำไม่ได้ (รู้ว่าสัญญาณเกิดก็ต่อเมื่อแท่งนั้นปิดแล้ว)
#   2. เพิ่ม Buy & Hold ของหุ้นตัวเดียวกัน ช่วงเวลาเดียวกัน เป็น benchmark
#      เทียบ — เดิมดู win rate ลอยๆ ไม่รู้ว่ากลยุทธ์ดีกว่า "ถือเฉยๆ" จริงไหม
#   3. เพิ่ม Max Drawdown (จาก equity curve ของ trade ที่ compound ต่อกัน)
#      และ Sharpe ratio แบบประมาณการจาก distribution ของ trade returns
#   4. ระบุข้อจำกัดของ backtest นี้ตรงๆ ในผลลัพธ์ (ดู key "notes")
# 
# ข้อจำกัดที่ยังมีอยู่ (ไม่ได้ทำให้ backtest นี้สมบูรณ์แบบ บอกตรงๆ):
#   • ไม่หักค่าคอมมิชชั่น/สเปรด/สลิปเพจ
#   • ทดสอบบนหุ้นที่ "ยังอยู่ใน index วันนี้" เท่านั้น → survivorship bias
#   • Sharpe คำนวณจาก distribution ของ trade returns ไม่ใช่ daily returns
#     แบบเข้มงวด ถือเป็นค่าประมาณ ไม่ใช่ Sharpe ที่ใช้เทียบกับกองทุนจริงได้
#   • กลยุทธ์เดียว ผลย้อนหลังไม่ใช่การันตีผลในอนาคต ไม่ใช่คำแนะนำการลงทุน


BACKTEST_NOTES = (
    "ไม่หักค่าคอมมิชชั่น/สเปรด · ทดสอบบนหุ้นที่ยังอยู่ใน index วันนี้เท่านั้น "
    "(survivorship bias) · Sharpe เป็นค่าประมาณจาก trade returns ไม่ใช่ "
    "daily returns แบบเข้มงวด · ผลย้อนหลังไม่ใช่การันตีอนาคต ไม่ใช่คำแนะนำการลงทุน"
)


@retry(times=3, base_delay=0.6)
def _download_2y(ticker: str) -> pd.DataFrame:
    return yf.Ticker(ticker).history(period="2y", interval="1d", auto_adjust=True)


@st.cache_data(ttl=86400)
def backtest(ticker: str, hold_days: int = 20) -> dict:
    try:
        df = _download_2y(ticker)
        if df is None or len(df) < 220:
            return {"error": "ข้อมูลไม่พอ (ต้องการ 2 ปี)"}
        cl = df["Close"]
        op = df["Open"]
        e20, e50, e200 = ema(cl, 20), ema(cl, 50), ema(cl, 200)

        trades = []
        in_trade = False
        entry_price = 0.0
        entry_i = 0
        upper = len(cl) - hold_days - 2
        for i in range(200, max(200, upper)):
            hi = max(e20.iloc[i], e50.iloc[i], e200.iloc[i])
            lo = min(e20.iloc[i], e50.iloc[i], e200.iloc[i])
            bw = (hi - lo) / e200.iloc[i] * 100 if e200.iloc[i] > 0 else np.nan
            if not in_trade and bw < 3.0 and cl.iloc[i] > e200.iloc[i]:
                entry_price = op.iloc[i + 1]  # เข้าซื้อที่ open ของแท่งถัดไป ไม่ใช่ close วันนี้
                entry_i = i + 1
                in_trade = True
            elif in_trade and (i - entry_i) >= hold_days:
                exit_price = cl.iloc[i]
                trades.append({
                    "ret": round((exit_price - entry_price) / entry_price * 100, 2),
                    "entry_date": str(cl.index[entry_i].date()),
                    "exit_date": str(cl.index[i].date()),
                })
                in_trade = False

        bh_start = cl.iloc[200]
        bh_end = cl.iloc[-1]
        bh_ret = round((bh_end - bh_start) / bh_start * 100, 2)

        if not trades:
            return {
                "n": 0, "win_rate": 0, "avg": 0, "best": 0, "worst": 0, "trades": [],
                "buy_hold_ret": bh_ret, "max_drawdown": 0, "sharpe": None, "notes": BACKTEST_NOTES,
            }

        rets = [t["ret"] for t in trades]
        wins = [r for r in rets if r > 0]

        equity = [1.0]
        for r in rets:
            equity.append(equity[-1] * (1 + r / 100))
        equity = np.array(equity)
        running_max = np.maximum.accumulate(equity)
        drawdowns = (equity - running_max) / running_max * 100
        max_dd = round(float(drawdowns.min()), 2)

        ann_factor = 252 / hold_days if hold_days > 0 else 1
        mean_r, std_r = float(np.mean(rets)), float(np.std(rets))
        sharpe = round((mean_r / std_r) * np.sqrt(ann_factor), 2) if std_r > 0 else None
        total_compound_ret = round((equity[-1] - 1) * 100, 2)

        return {
            "n": len(trades), "win_rate": round(len(wins) / len(trades) * 100, 1),
            "avg": round(mean_r, 2), "median": round(float(np.median(rets)), 2),
            "best": round(max(rets), 2), "worst": round(min(rets), 2),
            "trades": rets, "trade_details": trades,
            "buy_hold_ret": bh_ret, "strategy_compound_ret": total_compound_ret,
            "max_drawdown": max_dd, "sharpe": sharpe, "notes": BACKTEST_NOTES,
        }
    except Exception as e:
        log_err(f"backtest({ticker})", e)
        return {"error": str(e)}


# ────────────────────────────────────────────────────────────
# SUPPORT ACCURACY BACKTEST (v3.5, ตัด Signal ออกใน v3.21)
# ตอบคำถาม "แนวรับแม่นแค่ไหนจริงๆ" ด้วยหลักฐานจริง ไม่ใช่แค่เชื่อ label
# วิธีทำ: ย้อนคำนวณว่าในแต่ละวันที่ผ่านมา หุ้นแต่ละตัว "เคยอยู่ที่แนวรับไหม"
# (ใช้ข้อมูลถึงวันนั้นเท่านั้น ไม่มี lookahead) แล้ววัดผลตอบแทนจริงในอีก
# 10/20 วันถัดไป สรุปเป็นค่าเฉลี่ย/win rate ต่อสถานะแนวรับประเภทนั้นๆ
# ────────────────────────────────────────────────────────────

SUPPORT_BACKTEST_SAMPLE = (
    # หุ้นใหญ่ (Large Cap) — เดิม
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "JPM", "BAC", "XOM",
    "JNJ", "UNH", "HD", "WMT", "PG", "KO", "DIS", "NFLX", "ADBE", "CRM",
    "CAT", "BA", "GE", "NEE", "LIN", "COST", "MCD", "NKE", "V", "MA",
    "PTT.BK", "CPALL.BK", "AOT.BK", "KBANK.BK", "ADVANC.BK",
    # v3.5: เพิ่มหุ้นเล็ก/กลาง (Small/Mid Cap) — เดิมมีแต่หุ้นใหญ่ ทั้งที่ของจริง
    # ที่ระบบสแกนเจอบ่อยจาก Russell2000/Hidden Gem ส่วนใหญ่เป็นหุ้นเล็ก/กลาง
    # พฤติกรรมราคาต่างจากหุ้นใหญ่มาก ผลทดสอบจากหุ้นใหญ่ล้วนๆอาจไม่สะท้อนของจริง
    "DKNG", "SMCI", "UPST", "RIOT", "PODD", "LYFT", "SNOW", "TDOC",
    "KTOS", "CALX", "LOCO", "FIZZ", "HALO", "RGEN", "SWAV",
)


# v3.45: ลบ _wilder_rsi_series() ทิ้ง — เป็นเศษที่เหลือจาก _signal_history_
# for_ticker() เวอร์ชันเก่า (ก่อน v3.21 ตัด Signal ออก) ที่เคยต้องคำนวณ RSI
# rolling ทั้งเส้นสำหรับ backtest สัญญาณ — พอเปลี่ยนเป็น _support_history_
# for_ticker() ที่ไม่ใช้ RSI เลย ฟังก์ชันนี้เลยไม่มีใครเรียกอีกต่อไป
def _support_history_for_ticker(ticker: str) -> pd.DataFrame:
    """
    v3.21: เดิมฟังก์ชันนี้ (_signal_history_for_ticker) คำนวณทั้ง Signal
    history และ Support history คู่กัน — ตอนนี้ตัดส่วน Signal ออกทั้งหมด
    (ตามที่ตัดสินใจเอา Signal ออกจากทั้งระบบ) เหลือแค่ Support ซึ่งเป็นส่วน
    ที่ตอบโจทย์ตรงๆว่า "แนวรับที่หาไว้ใช้ได้จริงแค่ไหน" — คำนวณ Support
    status ของทุกวันในอดีต (2 ปี) ของหุ้นตัวเดียว + ผลตอบแทนจริงในอีก 10/20
    วันถัดไปจากจุดนั้น (ใช้ข้อมูลถึงวันนั้นเท่านั้น ไม่มี lookahead)

    v3.24: เพิ่มบันทึก touch_count/volume_confirmed/confluence ต่อเหตุการณ์
    (ข้อ 1 จากการวิเคราะห์ทบทวนอัลกอริทึม) — เดิมน้ำหนักคะแนนใน
    support_status() (touch×1.5, volume 2, confluence 2, proximity 1) เป็น
    ตัวเลขที่ตั้งเอง ไม่เคยพิสูจน์ว่าปัจจัยไหนจริงๆทำนายการเด้งกลับได้ดีกว่า
    กัน — เก็บ field พวกนี้ไว้ให้ backtest_support_accuracy() เอาไปแยกดูทีละ
    ปัจจัยได้ว่าอันไหนมีผลจริง อันไหนไม่ค่อยมีผล (ดูตาราง breakdown ในแท็บ
    Backtester) หมายเหตุ: ไม่ได้ส่ง rs20 เข้ามาที่นี่ (ต้องมี benchmark series
    เพิ่ม จะทำให้ backtest ช้าลงอีกมาก) แปลว่า Support Quality ในการ backtest
    นี้เป็นเวอร์ชัน "ไม่รวม RS bonus" ต่างจากตอนใช้งานจริงในแอปเล็กน้อย
    """
    try:
        df = _download_2y(ticker)
        if df is None or len(df) < 230:
            return pd.DataFrame()
        cl = df["Close"]
        e50, e200 = ema(cl, 50), ema(cl, 200)

        rows, prev_sup, n = [], None, len(df)
        for i in range(200, n - 20):
            px = cl.iloc[i]
            # เช็คทุก 3 วัน (ไม่ใช่ทุกวัน) เพื่อลดเวลาคำนวณ เพราะ
            # find_support_levels() ทำงาน O(lookback) ต่อครั้ง ระดับแนวรับ
            # เปลี่ยนช้าอยู่แล้ว เช็คถี่ทุกวันไม่จำเป็นและไม่กระทบความแม่นยำ
            if i % 3 == 0:
                sup = support_status(px, df.iloc[:i + 1], e50.iloc[i], e200.iloc[i])
                sup_sig = sup["status"]
                if sup_sig != prev_sup and sup_sig != "—":
                    rows.append({
                        "ticker": ticker, "signal": sup_sig, "kind": "support",
                        "fwd10": round((cl.iloc[i + 10] - px) / px * 100, 2),
                        "fwd20": round((cl.iloc[i + 20] - px) / px * 100, 2),
                        "touch_count": sup.get("touch_count", 0),
                        "volume_confirmed": sup.get("volume_confirmed", False),
                        "confluence": sup.get("confluence", False),
                    })
                prev_sup = sup_sig
        return pd.DataFrame(rows)
    except Exception as e:
        log_err(f"support_history({ticker})", e)
        return pd.DataFrame()


def _confidence_flag(n: int) -> str:
    """v3.5: เตือนตรงๆว่าจำนวนครั้งน้อยเกินจะเชื่อทางสถิติได้ — เคยพบจริงตอน
    ทดสอบว่า signal บางแบบมีแค่ 1-2 ครั้งทั้ง sample แล้วโชว์ Win Rate 100%
    ซึ่งไม่มีความหมายทางสถิติเลย แต่หน้าตาตารางดูน่าเชื่อเท่าแถวที่มีร้อยครั้ง"""
    if n >= 20:
        return "✅ พอเชื่อได้"
    if n >= 10:
        return "🔸 น้อย ระวัง"
    return "⚠️ น้อยมาก ไม่ควรเชื่อ"


SUPPORT_BACKTEST_NOTES = (
    "ทดสอบจากหุ้นตัวอย่าง 50 ตัว ผสมหุ้นใหญ่+เล็ก/กลาง (ไม่ใช่ทุกหุ้นใน universe) "
    "ย้อนหลัง 2 ปี · นับเฉพาะจุดที่สถานะแนวรับเพิ่งเปลี่ยน ไม่นับวันต่อเนื่องซ้ำ แต่ "
    "หุ้นคนละตัวในช่วงเวลาเดียวกันอาจมีความเชื่อมโยงกัน (เช่น ตลาดรวมขึ้น) "
    "ทำให้ไม่ใช่ independent sample เต็มรูปแบบ · แถวที่ 'จำนวนครั้ง' น้อย "
    "(ดูคอลัมน์ความเชื่อมั่น) ตัวเลขยังไม่น่าเชื่อถือพอทางสถิติ · ไม่หักค่าคอมมิชชั่น/"
    "สเปรด · ผลย้อนหลังไม่ใช่การันตีอนาคต ไม่ใช่คำแนะนำการลงทุน"
)


@st.cache_data(ttl=86400)
def backtest_support_accuracy(sample: tuple = SUPPORT_BACKTEST_SAMPLE) -> dict:
    """
    v3.21: เดิมชื่อ backtest_signal_accuracy() รวม Signal+Support สองตาราง
    — ตัด Signal ออก เหลือแค่ Support table เดียว ตอบคำถามตรงๆว่า "อยู่ที่
    แนวรับ"/"ใกล้แนวรับ" ในอดีตเด้งกลับขึ้นจริงกี่ % ด้วยหลักฐานจริง
    """
    all_dfs = [d for tk in sample if not (d := _support_history_for_ticker(tk)).empty]
    if not all_dfs:
        return {"error": "ดึงข้อมูลไม่สำเร็จเลยสักตัว ลองใหม่อีกครั้ง"}
    full = pd.concat(all_dfs, ignore_index=True)

    def _aggregate(sub: pd.DataFrame) -> pd.DataFrame:
        # v3.24 BUG FIX (พบระหว่างทดสอบฟีเจอร์ใหม่): เดิม "จำนวนครั้ง=(...)"
        # เขียนเป็น bare keyword argument ตรงๆ — Python จะทำ NFKC normalization
        # กับ "ชื่อ keyword argument" ที่เป็นตัวอักษรถูกต้องตามหลัก identifier
        # โดยอัตโนมัติตอน parse (คนละเรื่องกับ string literal ธรรมดาที่ไม่ถูก
        # normalize) ทำให้ "ำ" (SARA AM ตัวเดียว U+0E33) ถูกแปลงเป็นรูป
        # decompose (นิคหิต+สระอา 2 ตัวอักษร) กลายเป็นคนละ string กับตอนใช้
        # "จำนวนครั้ง" เป็น string literal ธรรมดาตอน index `agg["จำนวนครั้ง"]"`
        # ด้านล่าง — เกิด KeyError จริง (เจอตอนทดสอบเพิ่ม factor_table ใหม่
        # ทั้งที่โค้ดจุดนี้ไม่ได้แก้มานานแล้ว แปลว่าบั๊กนี้อาจซ่อนอยู่ตั้งแต่
        # ตอนแรกที่เขียน แค่ไม่มีใครกดปุ่มนี้ด้วย pandas version ที่โดนบั๊กพอดี)
        # แก้โดยเปลี่ยนมาใช้ **{"...": (...)} แบบเดียวกับคอลัมน์อื่นทั้งหมด
        # (unpack จาก dict ตอน runtime ไม่ผ่าน identifier normalization เลย)
        agg = sub.groupby("signal").agg(
            **{"จำนวนครั้ง": ("signal", "count")},
            **{"ผลตอบแทนเฉลี่ย 10วัน%": ("fwd10", "mean")},
            **{"Win Rate 10วัน%": ("fwd10", lambda x: round((x > 0).mean() * 100, 1))},
            **{"ผลตอบแทนเฉลี่ย 20วัน%": ("fwd20", "mean")},
            **{"Win Rate 20วัน%": ("fwd20", lambda x: round((x > 0).mean() * 100, 1))},
        ).round(2).reset_index().rename(columns={"signal": "Support"})
        agg["ความเชื่อมั่น"] = agg["จำนวนครั้ง"].apply(_confidence_flag)
        return agg.sort_values("ผลตอบแทนเฉลี่ย 20วัน%", ascending=False)

    sup_table = _aggregate(full) if not full.empty else pd.DataFrame()

    # v3.24 ข้อ 1: แยกวิเคราะห์ทีละปัจจัย (touch count / volume / confluence)
    # ว่าอันไหนจริงๆทำนายการเด้งกลับได้ดีกว่ากัน — แทนที่จะเชื่อน้ำหนักที่ตั้ง
    # เองใน support_status() เฉยๆ ใช้ข้อมูลจริงจากหุ้นตัวอย่างมาตรวจสอบ
    def _factor_row(label: str, mask: pd.Series) -> dict:
        sub = full[mask]
        if sub.empty:
            return {"ปัจจัย": label, "จำนวนครั้ง": 0, "ผลตอบแทนเฉลี่ย 20วัน%": None,
                   "Win Rate 20วัน%": None, "ความเชื่อมั่น": "⚠️ ไม่มีข้อมูล"}
        return {
            "ปัจจัย": label, "จำนวนครั้ง": len(sub),
            "ผลตอบแทนเฉลี่ย 20วัน%": round(sub["fwd20"].mean(), 2),
            "Win Rate 20วัน%": round((sub["fwd20"] > 0).mean() * 100, 1),
            "ความเชื่อมั่น": _confidence_flag(len(sub)),
        }

    factor_rows = []
    if "touch_count" in full.columns:
        factor_rows.append(_factor_row("Touch Count ≥3", full["touch_count"] >= 3))
        factor_rows.append(_factor_row("Touch Count <3", full["touch_count"] < 3))
    if "volume_confirmed" in full.columns:
        factor_rows.append(_factor_row("Volume ยืนยัน", full["volume_confirmed"] == True))
        factor_rows.append(_factor_row("Volume ไม่ยืนยัน", full["volume_confirmed"] == False))
    if "confluence" in full.columns:
        factor_rows.append(_factor_row("มี Confluence", full["confluence"] == True))
        factor_rows.append(_factor_row("ไม่มี Confluence", full["confluence"] == False))
    factor_table = pd.DataFrame(factor_rows)

    # Buy & Hold เฉลี่ยของหุ้นตัวอย่างทั้งหมดในช่วงเดียวกัน เอาไว้เทียบบรรทัดฐาน
    bh_rets = []
    for tk in sample:
        try:
            d = _download_2y(tk)
            if d is not None and len(d) > 220:
                bh_rets.append((d["Close"].iloc[-1] - d["Close"].iloc[200]) / d["Close"].iloc[200] * 100)
        except Exception:
            pass
    bh_avg = round(float(np.mean(bh_rets)), 2) if bh_rets else None

    return {"support_table": sup_table, "factor_table": factor_table,
            "n_tickers": len(all_dfs), "n_support_events": len(full),
            "buy_hold_avg": bh_avg, "notes": SUPPORT_BACKTEST_NOTES}


# ════════════════════════════════════════════════════════
# [merged from lib/styles.py]
# ════════════════════════════════════════════════════════
# MODULE — STYLES & UI HELPERS
# ย้ายมาจาก v2.0 ตรงๆ (CSS theme, dataframe style functions, info_card)

CSS_BLOCK = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Chakra+Petch:wght@500;600;700&family=Sarabun:wght@400;500;600;700&family=Share+Tech+Mono&display=swap');

/* ════════════════════════════════════════════════════════════
   DESIGN TOKENS — "Trading terminal of the future"
   bg: deep space-blue void · panels: layered blue-black glass
   accent: cyan (active/primary) + violet (featured/special)
   semantic: neon green (bull) / electric red (bear) / amber (watch)
   type: Chakra Petch (HUD labels/headers) + Sarabun (Thai/body)
         + Share Tech Mono (tickers/prices/readouts)
   ════════════════════════════════════════════════════════════ */
:root {
    --bg:#060912; --panel:#0e1626; --panel2:#101c33; --line:#1c2b45; --line2:#22344f;
    --cyan:#2de2e6; --violet:#b66bff; --green:#34f5a4; --red:#ff3864; --amber:#ffc857; --gold:#ffd84d;
    --text:#e8f0ff; --text-mid:#93a8c9; --text-dim:#5b7299;
}

/* ── BASE ── */
html, body, [class*="css"] { font-family:'Sarabun','Chakra Petch',sans-serif; }
.stApp {
    background:
        radial-gradient(circle at 12% 18%, rgba(182,107,255,0.07), transparent 38%),
        radial-gradient(circle at 88% 78%, rgba(45,226,230,0.08), transparent 42%),
        repeating-linear-gradient(0deg, rgba(45,226,230,0.025) 0px, rgba(45,226,230,0.025) 1px, transparent 1px, transparent 40px),
        repeating-linear-gradient(90deg, rgba(45,226,230,0.025) 0px, rgba(45,226,230,0.025) 1px, transparent 1px, transparent 40px),
        var(--bg) !important;
}
.main .block-container { padding:1.2rem 2rem 2rem 2rem !important; max-width:100% !important; }
* { scrollbar-width:thin; scrollbar-color:var(--cyan) var(--panel); }
::-webkit-scrollbar { width:8px; height:8px; }
::-webkit-scrollbar-track { background:var(--panel); }
::-webkit-scrollbar-thumb { background:var(--line2); border-radius:4px; }
::-webkit-scrollbar-thumb:hover { background:var(--cyan); }
*:focus-visible { outline:2px solid var(--cyan) !important; outline-offset:2px !important; }

/* ── ALL TEXT defaults ── */
p, span, div, label, li, td, th { color:var(--text) !important; font-family:'Sarabun',sans-serif; }
h1,h2,h3,h4,h5,h6 {
    color:#ffffff !important; font-weight:700 !important; line-height:1.3 !important;
    font-family:'Chakra Petch',sans-serif !important; letter-spacing:0.01em;
}
strong, b { color:#ffffff !important; }
small, .stCaption p { color:var(--text-dim) !important; font-size:0.78rem !important; }
code {
    color:var(--cyan) !important; background:var(--panel2) !important; padding:1px 6px !important;
    border-radius:3px !important; font-family:'Share Tech Mono',monospace !important;
    border:1px solid var(--line) !important;
}
hr { border-color:var(--line) !important; margin:1rem 0 !important; }

/* ── HUD HEADER PANEL (bracketed terminal frame for the title) ── */
.hud-frame {
    position:relative; border:1px solid var(--line2); border-radius:6px;
    background:linear-gradient(180deg, rgba(45,226,230,0.05), rgba(182,107,255,0.03));
    padding:18px 24px; margin-bottom:14px; overflow:hidden;
}
.hud-frame::before {
    content:""; position:absolute; top:0; left:0; right:0; height:2px;
    background:linear-gradient(90deg, transparent, var(--cyan), var(--violet), transparent);
    animation:hud-scan 5s linear infinite;
}
@keyframes hud-scan { 0%{transform:translateX(-100%);} 100%{transform:translateX(100%);} }
.hud-corner { position:absolute; width:14px; height:14px; border-color:var(--cyan); opacity:0.8; }
.hud-corner.tl { top:6px; left:6px; border-top:2px solid; border-left:2px solid; }
.hud-corner.tr { top:6px; right:6px; border-top:2px solid; border-right:2px solid; }
.hud-corner.bl { bottom:6px; left:6px; border-bottom:2px solid; border-left:2px solid; }
.hud-corner.br { bottom:6px; right:6px; border-bottom:2px solid; border-right:2px solid; }

/* ── METRIC CARDS ── */
div[data-testid="metric-container"] {
    background:var(--panel) !important;
    border:1px solid var(--line2) !important;
    border-radius:6px !important;
    padding:14px 18px !important;
    box-shadow:0 0 0 1px rgba(45,226,230,0.04) inset !important;
}
[data-testid="stMetricLabel"] p,
[data-testid="stMetricLabel"] span,
[data-testid="stMetricLabel"] div {
    color:var(--text-dim) !important;
    font-size:0.7rem !important; font-weight:600 !important;
    text-transform:uppercase !important; letter-spacing:0.08em !important;
    font-family:'Chakra Petch',sans-serif !important;
}
[data-testid="stMetricValue"],
[data-testid="stMetricValue"] > div,
[data-testid="stMetricValue"] span {
    color:#ffffff !important; -webkit-text-fill-color:#ffffff !important;
    font-size:1.6rem !important; font-weight:700 !important; line-height:1.25 !important;
    font-family:'Share Tech Mono',monospace !important;
}

/* ── TABS ── */
.stTabs [data-baseweb="tab-list"] {
    background:var(--panel) !important; border:1px solid var(--line) !important;
    border-radius:6px !important; padding:4px !important; gap:2px !important;
}
.stTabs [data-baseweb="tab"] {
    color:var(--text-dim) !important; font-weight:600 !important; font-size:0.85rem !important;
    border-radius:4px !important; padding:7px 16px !important; background:transparent !important;
    font-family:'Chakra Petch',sans-serif !important;
}
.stTabs [aria-selected="true"] {
    background:linear-gradient(135deg, rgba(45,226,230,0.18), rgba(182,107,255,0.12)) !important;
    color:#ffffff !important; box-shadow:0 0 0 1px var(--cyan) inset !important;
}
.stTabs [data-baseweb="tab"]:hover { color:var(--text) !important; }

/* ── SIDEBAR ── */
section[data-testid="stSidebar"] {
    background:var(--panel) !important; border-right:1px solid var(--line) !important;
}
section[data-testid="stSidebar"] p,
section[data-testid="stSidebar"] span,
section[data-testid="stSidebar"] label,
section[data-testid="stSidebar"] div { color:var(--text) !important; }

/* ── INPUTS ── */
.stSelectbox [data-baseweb="select"] > div,
.stMultiSelect [data-baseweb="select"] > div {
    background:var(--panel2) !important; border-color:var(--line2) !important; border-radius:5px !important;
}
.stSelectbox [data-baseweb="select"]:focus-within > div,
.stMultiSelect [data-baseweb="select"]:focus-within > div { border-color:var(--cyan) !important; }
.stSelectbox span, .stMultiSelect span { color:var(--text) !important; }

/* ── v3.41: ปรับ "pill" ของตัวเลือกที่เลือกไว้ (เช่น หมวดหุ้นที่เลือก) ให้
   เข้าธีมไซเบอร์พังก์ของแอป (ไล่สีฟ้า-ม่วง + เรืองแสงบางๆ) แทนสีเขียว
   default ธรรมดาของ Streamlit — CSS ล้วนๆ ปลอดภัย ไม่กระทบฟังก์ชันใดๆ ──*/
[data-baseweb="tag"] {
    background:linear-gradient(90deg, rgba(45,226,230,0.22), rgba(182,107,255,0.22)) !important;
    border:1px solid var(--cyan) !important;
    border-radius:6px !important;
    box-shadow:0 0 6px rgba(45,226,230,0.25) !important;
}
[data-baseweb="tag"] span { color:var(--text) !important; font-weight:600 !important; }
[data-baseweb="tag"] svg { fill:var(--cyan) !important; }
.stTextArea textarea, .stTextInput input {
    background:var(--panel2) !important; color:var(--text) !important; border-color:var(--line2) !important;
    border-radius:5px !important; font-family:'Share Tech Mono',monospace !important;
}
.stTextArea textarea:focus, .stTextInput input:focus { border-color:var(--cyan) !important; }
.stSlider [data-testid="stThumbValue"] span { color:#ffffff !important; }
.stSlider [data-baseweb="slider"] div[role="slider"] { background:var(--cyan) !important; box-shadow:0 0 8px var(--cyan) !important; }

/* ── BUTTONS ── */
.stButton > button {
    background:linear-gradient(135deg, var(--cyan), var(--violet)) !important;
    color:#04101a !important; border:none !important; border-radius:5px !important;
    font-weight:700 !important; font-size:0.88rem !important; padding:9px 18px !important;
    font-family:'Chakra Petch',sans-serif !important; letter-spacing:0.02em !important;
    transition:box-shadow 0.2s ease, transform 0.1s ease !important;
}
.stButton > button:hover {
    box-shadow:0 0 18px rgba(45,226,230,0.55), 0 0 4px rgba(182,107,255,0.5) !important;
    transform:translateY(-1px) !important;
}

/* ── EXPANDER ── */
details { background:var(--panel) !important; border:1px solid var(--line) !important; border-radius:6px !important; }
details summary { color:var(--text-mid) !important; font-weight:600 !important; padding:10px 14px !important; }
details summary:hover { color:var(--cyan) !important; }

/* ── DATAFRAME ── */
.stDataFrame { border-radius:6px !important; overflow:hidden !important; border:1px solid var(--line) !important; }

/* ── ALERTS ── */
.stAlert, [data-testid="stNotification"] {
    background:var(--panel2) !important; border-color:var(--line2) !important; border-radius:6px !important;
}
.stAlert p { color:var(--text) !important; }

/* ── SPINNER ── */
.stSpinner > div { border-top-color:var(--cyan) !important; }

/* ── PROGRESS BAR ── */
.stProgress > div > div { background:linear-gradient(90deg, var(--cyan), var(--violet)) !important; box-shadow:0 0 8px rgba(45,226,230,0.5) !important; }

/* ── HIDE CHROME ── */
#MainMenu, footer, .stDeployButton { display:none !important; }

/* ── v3.37: จอแคบ (มือถือ) — ขยายพื้นที่แตะให้ใหญ่ขึ้น กันกดพลาด ──
   ปรับแค่ปุ่ม/checkbox/แท็บ ไม่แตะโครงสร้างหลักเลย เป็น CSS ล้วนๆ
   ต่อให้ผิดพลาดก็แค่หน้าตาไม่เปลี่ยน ไม่มีทางทำฟังก์ชันพัง */
@media (max-width: 640px) {
  .stButton > button, .stCheckbox > label, .stRadio > label {
    min-height: 44px !important;
    font-size: 1rem !important;
  }
  .stTabs [data-baseweb="tab"] {
    min-height: 44px !important;
    padding: 8px 14px !important;
    font-size: 0.95rem !important;
  }
  .stTabs [data-baseweb="tab-list"] {
    gap: 4px !important;
  }
  .stCheckbox input, .stRadio input {
    width: 20px !important;
    height: 20px !important;
  }
  .stSelectbox > div, .stMultiSelect > div {
    min-height: 44px !important;
  }
}
</style>
"""


def inject_css() -> None:
    st.markdown(CSS_BLOCK, unsafe_allow_html=True)


def _sty_generic(v):
    v = str(v)
    if "Strong Buy" in v or "Hidden Gem" in v: return "color:#34f5a4;font-weight:800;"
    if "Breakout" in v or "เบรคเอาท์" in v:   return "color:#ffd76a;font-weight:700;"
    if "Uptrend"  in v or "ขาขึ้น" in v:       return "color:#34f5a4;font-weight:600;"
    if "Avoid" in v or "ขาลง" in v:             return "color:#ff3864;font-weight:700;"
    if "Watch" in v or "เฝ้าระวัง" in v:       return "color:#ffc857;font-weight:600;"
    if "Squeeze" in v:                           return "color:#b66bff;font-weight:700;"
    if "Accum" in v or "Stealth" in v:           return "color:#2de2e6;font-weight:700;"
    return "color:#e8f0ff;"


def _sty_weekly(v):
    v = str(v)
    if "Weekly Bull" in v: return "color:#34f5a4;font-weight:700;"
    if "Weekly Bear" in v: return "color:#ff3864;font-weight:700;"
    if "Weekly Mixed" in v: return "color:#ffc857;font-weight:600;"
    return "color:#e8f0ff;"


def _sty_rsi(v):
    try:
        f = float(v)
        if f < 35: return "color:#34f5a4;font-weight:700;"
        if f > 70: return "color:#ff3864;font-weight:700;"
        if f < 45: return "color:#5ee6ff;"
    except Exception:
        pass
    return "color:#e8f0ff;"


# v3.45: ลบ _sty_pct() ทิ้ง — ไม่มีคอลัมน์ไหนในตารางทั้งแอปเรียกใช้ style
# function นี้เลย (เศษที่เหลือจากคอลัมน์ % ที่ถูกตัด/เปลี่ยนไปในรอบก่อนๆ)

def _sty_gem(v):
    v = str(v)
    if "Hidden Gem" in v: return "color:#ffd84d;font-weight:800;"
    if "Emerging" in v:   return "color:#34f5a4;font-weight:700;"
    if "Watch" in v:      return "color:#ffc857;font-weight:600;"
    return "color:#5b7299;"


def _sty_squeeze(v):
    v = str(v)
    if "Squeezing" in v:  return "color:#b66bff;font-weight:800;"
    if "Tightening" in v: return "color:#5ee6ff;font-weight:700;"
    if "Just Broke" in v: return "color:#34f5a4;font-weight:700;"
    if "Expanding" in v:  return "color:#ffd76a;font-weight:600;"
    return "color:#5b7299;"


# v3.32: คำแปลไทยกำกับหัวคอลัมน์ที่เป็นศัพท์อังกฤษ — ใช้รูปแบบเดียวกับที่มี
# อยู่แล้วในแอป (เช่น filter label "Trend | แนวโน้ม") มาใช้กับหัวตารางด้วย
# ตั้งใจไม่แปลทุกคำ — คำที่เป็นมาตรฐานสากลอยู่แล้ว (RSI, P/E) ถ้าแปลจะยิ่ง
# งงกว่าเดิม เพราะไม่มีใครเรียกชื่อไทยของมันจริงๆในวงการ
COLUMN_LABEL_TH = {
    "Ticker": "Ticker | หุ้น",
    "Price": "Price | ราคา",
    "ราคาปิด": "ราคาปิด | Prev Close",
    "Sector": "Sector | หมวดธุรกิจ",
    "Trend": "Trend | แนวโน้ม",
    "Support": "Support | แนวรับ",
    "Support Zone": "Support Zone | ช่วงแนวรับ",
    "Support Dist%": "Support Dist% | ห่างแนวรับ",
    "Support Age": "Support Age | แนวรับมา(วัน)",
    "Support Quality": "Support Quality | คุณภาพแนวรับ",
    "Support Touches": "Support Touches | แตะกี่ครั้ง",
    "Resistance": "Resistance | แนวต้าน",
    "Resistance Zone": "Resistance Zone | ช่วงแนวต้าน",
    "Resistance Dist%": "Resistance Dist% | ห่างแนวต้าน",
    "Risk:Reward": "Risk:Reward | เสี่ยง:ผลตอบแทน",
    "Trend Age": "Trend Age | แนวโน้มมา(วัน)",
    "💎 Gem": "💎 Gem | หุ้นซ่อนเร้น",
    "Accum": "Accum | การสะสม",
    "Sector Bull%": "Sector Bull% | หมวดขาขึ้น%",
    "MktCap$B": "MktCap$B | มูลค่าบริษัท($B)",
    "Stars": "Stars | ดาว",
    # v3.33: เพิ่มคำแปลสำหรับตารางอื่นๆ ทั่วแอป (ไม่ใช่แค่ Dashboard) — Hidden
    # Gems, Watchlist, Backtester, Sector Map
    "Support Vol Confirmed": "Support Vol Confirmed | Volume ยืนยัน",
    "Support Confluence": "Support Confluence | แนวรับซ้อนกัน",
    "Trade #": "Trade # | ไม้ที่",
    "Return %": "Return % | ผลตอบแทน%",
    "Entry": "Entry | ราคาเข้า",
    "Exit": "Exit | ราคาออก",
    "Result": "Result | ผลลัพธ์",
    "Avg Gem Score": "Avg Gem Score | คะแนน Gem เฉลี่ย",
    "Avg Accum": "Avg Accum | คะแนนสะสมเฉลี่ย",
    "Bull %": "Bull % | ขาขึ้น%",
    "Weekly Trend": "Weekly Trend | แนวโน้มรายสัปดาห์",
    "EMA Pattern": "EMA Pattern | รูปแบบเส้น EMA",
    "Squeeze": "Squeeze | การหดตัว",
    "RS 20D": "RS 20D | ความแข็งแกร่งเทียบตลาด",
    "YTD%": "YTD% | ผลตอบแทนปีนี้%",
    "Drawdown%": "Drawdown% | ลดลงจากจุดสูงสุด%",
    "Trades": "Trades | จำนวนไม้",
    "Win%": "Win% | อัตราชนะ%",
    "Avg Ret%": "Avg Ret% | ผลตอบแทนเฉลี่ย%",
    "Best%": "Best% | ดีที่สุด%",
    "Worst%": "Worst% | แย่ที่สุด%",
    "vs Buy&Hold%": "vs Buy&Hold% | เทียบถือยาว%",
    "Gem Score": "Gem Score | คะแนนหุ้นซ่อนเร้น",
}


def apply_thai_labels(dfx: pd.DataFrame, style_map: dict = None):
    """
    v3.32: rename คอลัมน์เป็น "English | ไทย" สำหรับแสดงผล **เฉพาะตอนจะโชว์
    บนตารางเท่านั้น** — เรียกเป็นขั้นตอนสุดท้ายก่อน st.dataframe() เสมอ ห้าม
    เรียกก่อนหน้านั้น (ก่อน sort/filter/style) เพราะโค้ดส่วนอื่นทั้งหมดยังคง
    อ้างอิงชื่อคอลัมน์เดิม (ภาษาอังกฤษล้วน) อยู่ — ฟังก์ชันนี้แค่เปลี่ยนชื่อที่
    "เห็น" ตอนสุดท้าย ไม่กระทบ logic ใดๆก่อนหน้า

    คืนค่า (df ที่ rename แล้ว, style_map ที่ปรับ key ให้ตรงกับชื่อใหม่แล้ว)
    """
    dfx2 = dfx.rename(columns=COLUMN_LABEL_TH)
    style_map2 = None
    if style_map:
        style_map2 = {COLUMN_LABEL_TH.get(k, k): v for k, v in style_map.items()}
    return dfx2, style_map2


def support_tier(quality) -> str:
    """
    v3.31 ข้อ 3: จัดกลุ่มเป็นระดับ (🔥/👍/👀) แทนตัวเลขต่อเนื่องล้วนๆ — ไม่ใช่
    คะแนนใหม่ แค่แบ่งช่วงจาก Support Quality ที่มีอยู่แล้ว (เกณฑ์เดียวกับที่
    ใช้ใน Quick Pick sidebar: ≥8=สูงสุด, ≥6=น่าสนใจ) มนุษย์แยกแยะ "กลุ่ม"
    ได้ง่ายกว่าเทียบตัวเลขต่อเนื่องทีละคู่ — ช่วยแก้ปัญหา "หน้าตาเหมือนกันหมด"
    """
    if pd.isna(quality) or quality <= 0:
        return "—"
    if quality >= 8:
        return "🔥 สูงสุด"
    if quality >= 6:
        return "👍 น่าสนใจ"
    return "👀 เฝ้าดู"


def _sty_tier(v):
    v = str(v)
    if "สูงสุด" in v: return "color:#ff8a3d;font-weight:800;"
    if "น่าสนใจ" in v: return "color:#34f5a4;font-weight:700;"
    if "เฝ้าดู" in v: return "color:#5b7299;"
    return "color:#5b7299;"


def _sty_support(v):
    v = str(v)
    if "อยู่ที่แนวรับ" in v: return "color:#34f5a4;font-weight:800;"
    if "ใกล้แนวรับ" in v:   return "color:#ffc857;font-weight:700;"
    return "color:#5b7299;"


def _row_highlight_support(row):
    """
    v3.23: ไฮไลต์ทั้งแถวเป็นแถบสี (ไม่ใช่แค่ตัวหนังสือในคอลัมน์ Support
    คอลัมน์เดียว) ให้เห็นชัดเจนกว่าเดิมว่าหุ้นตัวไหน "เข้าเงื่อนไข" อยู่ที่
    แนวรับจริงๆ ใช้สีชุดเดียวกับที่ย้อม text คอลัมน์ Support อยู่แล้ว
    (เขียว/เหลือง — เข้ากับธีมเว็บ) แค่ทำให้จางลงมากเพื่อไม่ให้ตัวหนังสือ
    อ่านยาก (ความทึบ ~0.08-0.10 เท่านั้น)

    เงื่อนไข 2 ระดับ:
      🟢 อยู่ที่แนวรับ (ห่างราคา ≤1.5%) → แถบเขียวอ่อน
      🟡 ใกล้แนวรับ (ห่างราคา 1.5-4%)  → แถบเหลืองอ่อน
    ไม่เข้าเงื่อนไขทั้งสอง → ไม่มีแถบสี (พื้นหลังปกติ)

    v3.32: หา column "Support" แบบยืดหยุ่น (เผื่อถูก rename เป็นสองภาษา เช่น
    "Support | แนวรับ" ตอนแสดงผล) แทนที่จะเช็คชื่อ "Support" ตรงๆ อย่างเดียว
    ป้องกันฟีเจอร์แถบสีพังเงียบๆ ถ้ามีคนไป rename คอลัมน์ทีหลัง
    """
    sup_val = ""
    for col in row.index:
        if col == "Support" or str(col).startswith("Support |") or str(col).startswith("Support ("):
            sup_val = row[col]
            break
    sup = str(sup_val)
    if "อยู่ที่แนวรับ" in sup:
        return ["background-color: rgba(52,245,164,0.10);"] * len(row)
    if "ใกล้แนวรับ" in sup:
        return ["background-color: rgba(255,200,87,0.08);"] * len(row)
    return [""] * len(row)


def _sty_resistance(v):
    v = str(v)
    if "อยู่ที่แนวต้าน" in v: return "color:#ff3864;font-weight:800;"
    if "ใกล้แนวต้าน" in v:   return "color:#ffa857;font-weight:700;"
    return "color:#5b7299;"


def _sty_rs(v):
    try:
        f = float(v)
        if f > 5: return "color:#34f5a4;font-weight:700;"
        if f > 0: return "color:#5ee6ff;"
        if f < -5: return "color:#ff3864;font-weight:700;"
        return "color:#ffc857;"
    except Exception:
        return "color:#5b7299;"


def _sty_gs(v):
    try:
        n = int(v)
        if n >= 8: return "color:#ffd84d;font-weight:800;"
        if n >= 6: return "color:#34f5a4;font-weight:700;"
        if n >= 4: return "color:#2de2e6;"
    except Exception:
        pass
    return "color:#5b7299;"


def _sty_wr(v):
    try:
        f = float(v)
        if f >= 60: return "color:#34f5a4;font-weight:700;"
        if f >= 50: return "color:#5ee6ff;"
        return "color:#ff3864;"
    except Exception:
        return ""


def _sty_confidence(v):
    v = str(v)
    if "พอเชื่อได้" in v: return "color:#34f5a4;font-weight:600;"
    if "น้อย ระวัง" in v: return "color:#ffc857;font-weight:600;"
    if "น้อยมาก" in v: return "color:#ff3864;font-weight:700;"
    return "color:#5b7299;"


BASE_TBL = {
    "background-color": "#0e1626",
    "color": "#e8f0ff",
    "border": "1px solid #16213a",
    "font-size": "13px",
    "padding": "5px 10px",
}
HDR_TBL = [{"selector": "th", "props": [
    ("background-color", "#16213a"), ("color", "#ffffff"),
    ("font-weight", "700"), ("font-size", "11px"),
    ("padding", "8px 10px"), ("text-transform", "uppercase"),
    ("letter-spacing", "0.05em"),
]}]


def make_table(df, style_map: dict = None, row_style_fn=None) -> object:
    """Apply consistent dark styling + optional column-level styling + optional
    row-level highlight (v3.23: เพิ่ม row_style_fn — ย้อมทั้งแถวเป็นแถบสี
    แทนที่จะย้อมแค่ตัวหนังสือในคอลัมน์เดียว ดูง่ายกว่าเดิมมากว่าหุ้นตัวไหน
    'เข้าเงื่อนไข' จริงๆ)"""
    s = df.style.set_properties(**BASE_TBL).set_table_styles(HDR_TBL).hide(axis="index")
    if row_style_fn:
        s = s.apply(row_style_fn, axis=1)
    if style_map:
        for col, fn in style_map.items():
            if col in df.columns:
                s = s.map(fn, subset=[col])
    return s


def info_card(label: str, value: str, color="#ffffff", sub="") -> str:
    """Compact HTML metric card — terminal-readout style, guaranteed readable."""
    sub_html = f'<div style="color:#5b7299;font-size:0.75rem;margin-top:3px;">{sub}</div>' if sub else ""
    return (f'<div style="background:#0e1626;border:1px solid #22344f;border-left:3px solid {color};'
            f'border-radius:5px;padding:14px 16px;min-width:110px;">'
            f'<div style="color:#5b7299;font-size:0.68rem;font-weight:600;text-transform:uppercase;'
            f'letter-spacing:0.08em;margin-bottom:6px;font-family:\'Chakra Petch\',sans-serif;">{label}</div>'
            f'<div style="color:{color};font-size:1.45rem;font-weight:700;line-height:1.2;'
            f'font-family:\'Share Tech Mono\',monospace;">{value}</div>'
            f'{sub_html}'
            f'</div>')


def render_animated_metric_cards(cards: list, height: int = 180) -> None:
    """
    v3.9: การ์ดตัวเลขสรุปบน Dashboard เดิมโชว์ตัวเลขนิ่งๆ ทันที ตอนนี้ทำให้
    "นับวิ่งขึ้น" ทุกครั้งที่สลับ Universe หรือข้อมูลรีเฟรชใหม่ (เหมือนเลข
    ยอดวิ่งของเว็บสถิติทั่วไป) — ต้องใช้ st.components.v1.html() แทน
    st.markdown() ธรรมดา เพราะ st.markdown ไม่รัน <script> (โดน sanitize
    ออกด้วยเหตุผลด้านความปลอดภัย) components.html วาดใน iframe แยก ทำให้ต้อง
    ประกาศ CSS สีพื้นฐานซ้ำในนี้เอง (ไม่ได้สืบทอดจากธีมหลักของแอป)

    animate จาก 0 ทุกรอบ (ไม่ได้ต่อจากค่าก่อนหน้า) เพราะแต่ละ rerun ของ
    Streamlit สร้าง iframe ใหม่ ไม่มี state เดิมให้จำต่อ — ผลคือพอสลับ
    Universe/รีเฟรชข้อมูล ตัวเลขจะวิ่งขึ้นให้เห็นทุกครั้งตามที่ต้องการพอดี

    cards: list ของ dict {label, value, color, decimals(optional, default 0)}
    """
    cards_markup = ""
    for c in cards:
        val = c["value"] if pd.notna(c["value"]) else 0
        cards_markup += (
            f'<div class="ic" style="border-left-color:{c["color"]};">'
            f'<div class="lbl">{c["label"]}</div>'
            f'<div class="val count-num" data-target="{val}" '
            f'data-decimals="{c.get("decimals", 0)}" style="color:{c["color"]};">0</div>'
            f'</div>'
        )

    html = f"""
    <style>
      body {{ margin:0; background:transparent; }}
      .wrap {{ display:flex; gap:10px; flex-wrap:wrap; font-family:'Share Tech Mono',monospace; }}
      .ic {{ background:#0e1626; border:1px solid #22344f; border-left:3px solid #fff;
             border-radius:5px; padding:14px 16px; min-width:110px; box-sizing:border-box; }}
      .lbl {{ color:#5b7299; font-size:0.68rem; font-weight:600; text-transform:uppercase;
              letter-spacing:0.08em; margin-bottom:6px; font-family:'Chakra Petch',sans-serif; }}
      .val {{ font-size:1.45rem; font-weight:700; line-height:1.2; }}
    </style>
    <div class="wrap">{cards_markup}</div>
    <script>
      document.querySelectorAll('.count-num').forEach(function(el) {{
        var target = parseFloat(el.getAttribute('data-target')) || 0;
        var decimals = parseInt(el.getAttribute('data-decimals')) || 0;
        var duration = 700;
        var start = performance.now();
        function tick(now) {{
          var p = Math.min((now - start) / duration, 1);
          var eased = 1 - Math.pow(1 - p, 3);
          var cur = target * eased;
          el.textContent = decimals > 0 ? cur.toFixed(decimals) : Math.round(cur).toLocaleString();
          if (p < 1) {{ requestAnimationFrame(tick); }}
          else {{ el.textContent = decimals > 0 ? target.toFixed(decimals) : Math.round(target).toLocaleString(); }}
        }}
        requestAnimationFrame(tick);
      }});
    </script>
    """
    components.html(html, height=height)


# ════════════════════════════════════════════════════════
# [merged from lib/tv_chart.py]
# ════════════════════════════════════════════════════════
# MODULE — TRADINGVIEW WIDGET (relocated unchanged from v2.0)


def tv_chart(ticker: str, height: int = 620, interval: str = "D") -> None:

    nyse = {"JPM", "JNJ", "V", "PG", "UNH", "HD", "MA", "DIS", "BAC", "XOM", "CVX", "WMT",
            "KO", "PFE", "MRK", "T", "VZ", "IBM", "GE", "GM", "F", "GS", "MS", "C", "WFC"}
    is_thai = ticker.endswith(".BK")
    sym = ticker.replace(".BK", "") if is_thai else ticker
    prefix = "SET" if is_thai else ("NYSE" if ticker in nyse else "NASDAQ")
    html = f"""
    <div style="border-radius:10px;overflow:hidden;border:1px solid #16213a;">
    <div class="tradingview-widget-container" style="height:{height}px;width:100%;">
    <div class="tradingview-widget-container__widget" style="height:{height}px;width:100%;"></div>
    <script type="text/javascript"
        src="https://s3.tradingview.com/external-embedding/embed-widget-advanced-chart.js" async>
    {{
        "autosize":true,"symbol":"{prefix}:{sym}","interval":"{interval}",
        "timezone":"Asia/Bangkok","theme":"dark","style":"1","locale":"th",
        "backgroundColor":"#060912","gridColor":"rgba(28,43,69,0.4)",
        "hide_top_toolbar":false,"hide_legend":false,"save_image":false,
        "studies":[
            {{"id":"MAExp@tv-basicstudies","inputs":{{"length":20}},"styles":{{"plot_0":{{"color":"#ffc857","linewidth":1}}}}}},
            {{"id":"MAExp@tv-basicstudies","inputs":{{"length":50}},"styles":{{"plot_0":{{"color":"#2de2e6","linewidth":1}}}}}},
            {{"id":"MAExp@tv-basicstudies","inputs":{{"length":200}},"styles":{{"plot_0":{{"color":"#ff3864","linewidth":2}}}}}},
            "RSI@tv-basicstudies","MACD@tv-basicstudies"
        ]
    }}
    </script></div></div>"""
    components.html(html, height=height + 10, scrolling=False)


# ════════════════════════════════════════════════════════
# [merged from lib/sector_view.py]
# ════════════════════════════════════════════════════════
# MODULE — SECTOR HEATMAP (เหมือน v2.0 logic เดิม ย้ายมาไว้แยกไฟล์)



@st.cache_data(ttl=3600)
def sector_heatmap_data_live() -> pd.DataFrame:
    """
    v3.8: ไม่ได้ถูกเรียกจาก UI แล้ว (Tab 5 อ่านจาก
    load_prefetched_sector_heatmap() เพียงทางเดียว ไม่มีปุ่มสแกนสดอีกต่อไป
    ตามที่ขอ) เก็บฟังก์ชันนี้ไว้เผื่อรันทดสอบ/debug เองนอก UI เท่านั้น

    v3.12: ใช้ ticker ทั้งหมดต่อ sector แทน tickers[:5] เดิม ให้ตรงกับ
    compute_sector_heatmap() ใน fetch_data.py (ดูเหตุผลที่นั่น) — หมายเหตุ:
    ถ้าไม่มี bundle เลย (branch "else" ด้านล่าง) จะยิง Yahoo สดต่อ ticker
    เยอะขึ้นกว่าเดิมมาก (~18 ตัว/sector แทน 5 ตัว) แต่เพราะฟังก์ชันนี้ใช้
    debug นอก UI เท่านั้น ไม่กระทบผู้ใช้จริง จึงเลือกความแม่นยำมากกว่าความไว
    """
    _, bundle_df, _ = load_prefetched_bundle()
    use_bundle = bundle_df is not None and not bundle_df.empty and "Ticker" in bundle_df.columns

    rows = []
    for sector, tickers in SECTOR_MAP.items():
        scores = []
        if use_bundle:
            sub = bundle_df[bundle_df["Ticker"].isin(tickers)]
            for _, d in sub.iterrows():
                scores.append({
                    "gem": d.get("Gem Score", 0) or 0,
                    "accum": d.get("Accum Score", 0) or 0,
                    "rs20": d.get("RS 20D", 0) or 0,
                    "bull": 1 if "Bull" in str(d.get("Trend", "")) else 0,
                })
        else:
            for tk in tickers:
                d = analyze(tk)
                if d:
                    scores.append({
                        "gem": d.get("Gem Score", 0) or 0,
                        "accum": d.get("Accum Score", 0) or 0,
                        "rs20": d.get("RS 20D", 0) or 0,
                        "bull": 1 if "Bull" in str(d.get("Trend", "")) else 0,
                    })
        if scores:
            rows.append({
                "Sector": sector,
                "Avg Gem Score": round(np.mean([s["gem"] for s in scores]), 1),
                "Avg Accum": round(np.mean([s["accum"] for s in scores]), 1),
                "Avg RS 20D": round(np.mean([s["rs20"] for s in scores]), 1),
                "Bull %": round(np.mean([s["bull"] for s in scores]) * 100, 0),
                "Coverage": f"{len(scores)}/{len(tickers)}",
            })
    return pd.DataFrame(rows).sort_values("Avg Gem Score", ascending=False)


# ════════════════════════════════════════════════════════
# [เดิม merged from lib/alerts.py — ตัดออกทั้งหมดใน v3.21]
# ════════════════════════════════════════════════════════
# v3.21: เดิมมีระบบแจ้งเตือน "สัญญาณใหม่" 2 ชั้น (ในแอป + Telegram push) ผูก
# กับระบบ Signal (Strong Buy/Breakout) ที่ตัดออกทั้งระบบแล้วตามที่ตัดสินใจ
# โฟกัสกลยุทธ์ทั้งเว็บไปที่แนวรับ (Support) แทน — ตัดออกทั้งหมด ไม่มีการ
# แจ้งเตือน Telegram จากในแอปอีกต่อไป (Telegram แจ้งเตือน "Job ล้มเหลว" ใน
# prefetch.yml ยังอยู่เหมือนเดิม เป็นคนละระบบ ไม่เกี่ยวกัน)



# v3.10: จำกัดขนาดการสแกน "สด" (กดปุ่ม Run Screener) ไม่ให้ใหญ่เกินไป — เพราะ
# Streamlit Community Cloud (free tier) รันทุก session บนโปรเซสเดียวกัน สแกนสด
# ก้อนใหญ่ของคนนึงจะไปหน่วงคนอื่นที่เปิดแอปพร้อมกันด้วย การสแกนเต็ม Universe
# จริงๆ ให้เป็นหน้าที่ของ GitHub Action ตอนกลางคืนแทน (คนละโปรเซส ไม่กระทบกัน)
# v3.12: เดิมเลข version (เช่น "v3.8") เป็นแค่ข้อความ hardcode อยู่ใน HTML
# header เท่านั้น ไม่มีที่อื่นในโค้ดอ้างอิงถึงเลย ทำให้ไม่มีทางรู้อัตโนมัติว่า
# ข้อมูลที่ fetch_data.py เคยเซฟไว้ (latest_scan.json/snapshot) มาจากโค้ด
# version ไหน — เวลาจะทำ forward-test เทียบผลย้อนหลัง ถ้ามีการเปลี่ยน logic
# กลางทาง จะไม่มีทางแยกออกว่าข้อมูลไหน "ก่อน/หลัง" การเปลี่ยนนั้น ตอนนี้ทำให้
# เป็นค่าคงที่จริงในโค้ด แล้ว fetch_data.py stamp ค่านี้ลงไปในทุกไฟล์ JSON
# ที่เซฟ (ดู fetch_data.py) เพื่อให้ข้อมูลในอนาคตกรองตาม version ได้เอง
APP_VERSION = "3.45"

LIVE_SCAN_SAFETY_CAP = 100


# v3.5: เปลี่ยนจาก git commit ทุกวัน → เก็บไฟล์ที่ GitHub Release แทน
# (เดิม commit ไฟล์ ~800KB เข้า repo ทุกวัน จะกลายเป็น ~300MB/ปี ในระยะยาว
# repo จะบวมขึ้นเรื่อยๆ ไม่มีที่สิ้นสุด) แอปนี้อ่านจาก Release URL ตรงๆ
# (public URL ไม่ต้องมี API key) ไม่ต้องพึ่งไฟล์ใน git เลย
# (GITHUB_REPO/RELEASE_TAG ย้ายไปประกาศก่อน UNIVERSE_OPTIONS แล้ว — v3.17)
PREFETCH_URL = f"https://github.com/{GITHUB_REPO}/releases/download/{RELEASE_TAG}/latest_scan.json"
# v3.7: Sector Heatmap คำนวณไว้ล่วงหน้าตอน fetch_data.py รันแล้ว (ต่อจาก df
# ที่สแกนเสร็จอยู่แล้วในตัว ไม่ยิง Yahoo เพิ่ม) แอปแค่อ่านไฟล์นี้ตรงๆ
SECTOR_HEATMAP_URL = f"https://github.com/{GITHUB_REPO}/releases/download/{RELEASE_TAG}/sector_heatmap.json"

# ไฟล์ local ใช้เป็น fallback เฉพาะตอนรันทดสอบในเครื่องเอง (python fetch_data.py
# ตรงๆ โดยไม่ผ่าน GitHub Action) — ตอน deploy จริงบน Streamlit Cloud จะไม่มี
# ไฟล์นี้อยู่ในเครื่อง (เพราะไม่ได้ commit เข้า git แล้ว) จะใช้ทาง Release เสมอ
PREFETCH_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "latest_scan.json")
SECTOR_HEATMAP_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "sector_heatmap.json")


@st.cache_data(ttl=300)
def load_prefetched_bundle():
    """
    ดึงข้อมูลที่ GitHub Actions เตรียมไว้ล่วงหน้าทุกวันหลังตลาดปิด

    v3.5: เปลี่ยนจากอ่านไฟล์ local (data/latest_scan.json) เป็นดึงจาก
    GitHub Release URL ตรงๆ — เพราะไม่ commit ไฟล์เข้า git แล้ว (กัน repo
    บวม) ลองไฟล์ local ก่อนเผื่อรันทดสอบในเครื่องเอง ถ้าไม่มีค่อย fallback
    ไปดึงจาก Release

    v3.26: เพิ่มคืนค่า app_version ด้วย — fetch_data.py stamp field นี้ลงไป
    ในทุกไฟล์ JSON มาตั้งแต่ v3.12 แต่ไม่เคยมีที่ไหนในแอปดึงมาโชว์ให้เห็นเลย
    สักที่ (เจอตอนช่วยผู้ใช้ debug ปัญหา "คอลัมน์หาย" — เถียงกันไปมาว่าเป็น
    เพราะ deploy โค้ดใหม่ไม่ครบ หรือโค้ดมีบั๊ก ทั้งที่มีเครื่องมือเช็คได้ชัดๆ
    อยู่แล้วแต่ไม่เคยโชว์ให้ใครเห็น) ตอนนี้โชว์คู่กับเวลา "ดึงล่าสุด" ในแบนเนอร์
    เลย เทียบกับเลข APP_VERSION ของโค้ดที่รันอยู่ตอนนี้ได้ทันทีว่าตรงกันไหม

    คืนค่า (generated_at: str|None, df: pd.DataFrame, app_version: str|None)
    ถ้ายังไม่มีข้อมูลเลย (เช่น ก่อน Action รันรอบแรก) จะคืน (None, DataFrame
    ว่าง, None)
    """
    if os.path.exists(PREFETCH_PATH):
        try:
            with open(PREFETCH_PATH, "r", encoding="utf-8") as f:
                payload = json.load(f)
            return payload.get("generated_at"), pd.DataFrame(payload.get("data", [])), payload.get("app_version")
        except Exception as e:
            log_err("load_prefetched_bundle(local)", e)
    try:
        resp = requests.get(PREFETCH_URL, timeout=15)
        if resp.ok:
            payload = resp.json()
            return payload.get("generated_at"), pd.DataFrame(payload.get("data", [])), payload.get("app_version")
    except Exception as e:
        log_err("load_prefetched_bundle(release)", e)
    return None, pd.DataFrame(), None


@st.cache_data(ttl=3600)
def load_previous_snapshot(max_days_back: int = 5):
    """
    v3.18: โหลด snapshot ของวันก่อนหน้า (ย้อนหาได้สูงสุด max_days_back วัน
    เผื่อวันหยุด/สุดสัปดาห์ที่ไม่มี snapshot) ใช้เทียบ "วันนี้ vs เมื่อวาน"
    ในสรุปหน้า Dashboard (ดู fetch_data.py: เก็บ snapshot ทุกวันเป็น
    Release แยกชื่อ snapshot-YYYY-MM-DD อยู่แล้ว)

    คืนค่า (date_str หรือ None, DataFrame) — DataFrame ว่างถ้าหาไม่เจอเลย
    ในช่วง max_days_back วัน
    """
    try:
        today = datetime.date.today()
        for i in range(1, max_days_back + 1):
            d = (today - datetime.timedelta(days=i)).isoformat()
            url = f"https://github.com/{GITHUB_REPO}/releases/download/snapshot-{d}/snapshot_{d}.json"
            try:
                resp = requests.get(url, timeout=10)
                if resp.ok:
                    payload = resp.json()
                    return d, pd.DataFrame(payload.get("data", []))
            except Exception:
                continue
    except Exception as e:
        log_err("load_previous_snapshot", e)
    return None, pd.DataFrame()


@st.cache_data(ttl=300)
def load_prefetched_sector_heatmap():
    """
    v3.7: โหลด Sector Heatmap ที่ fetch_data.py คำนวณไว้ล่วงหน้าแล้วตอนดึง
    ข้อมูลหลักหลังตลาดปิด (ต่อจาก df ที่สแกนเสร็จอยู่แล้วในตัว ไม่ยิง Yahoo
    เพิ่ม) แทนที่จะต้องรอให้คนกดปุ่มคำนวณสดทุกครั้งที่เปิดแอป — คืนค่า
    (generated_at: str|None, DataFrame) เหมือน load_prefetched_bundle()
    """
    if os.path.exists(SECTOR_HEATMAP_PATH):
        try:
            with open(SECTOR_HEATMAP_PATH, "r", encoding="utf-8") as f:
                payload = json.load(f)
            return payload.get("generated_at"), pd.DataFrame(payload.get("data", []))
        except Exception as e:
            log_err("load_prefetched_sector_heatmap(local)", e)
    try:
        resp = requests.get(SECTOR_HEATMAP_URL, timeout=15)
        if resp.ok:
            payload = resp.json()
            return payload.get("generated_at"), pd.DataFrame(payload.get("data", []))
    except Exception as e:
        log_err("load_prefetched_sector_heatmap(release)", e)
    return None, pd.DataFrame()


def get_with_bundle_fallback(tickers: list, bundle_df: pd.DataFrame, max_live_fallback: int = 15) -> tuple:
    """ดึงข้อมูลของ tickers ที่ต้องการจาก bundle ที่ดึงไว้ล่วงหน้าก่อน ถ้ามีบาง
    ticker ไม่อยู่ใน bundle (เช่น พิมพ์ ticker แปลกๆใน Custom) ค่อย live fallback
    ทีละตัวสำหรับส่วนที่ขาดเท่านั้น (ใหม่ v3.2)

    v3.12: เดิมถ้าหา ticker ไม่เจอเกิน max_live_fallback ตัว (หรือ live fallback
    เองก็ยังหาไม่เจอ เช่น พิมพ์ผิด/delisted) จะถูกตัดทิ้งแบบเงียบๆ ไม่มีการ
    แจ้งอะไรเลย ผู้ใช้จะงงว่าทำไมตารางมีแถวน้อยกว่าที่คาด — ตอนนี้คืนค่าเป็น
    (DataFrame, dropped: list) ให้ผู้เรียกตัดสินใจแจ้งเตือนเอง (เช่น st.warning)
    """
    if bundle_df is None or bundle_df.empty or "Ticker" not in bundle_df.columns:
        have = pd.DataFrame()
        missing = list(tickers)
    else:
        have = bundle_df[bundle_df["Ticker"].isin(tickers)].copy()
        found = set(have["Ticker"].tolist())
        missing = [t for t in tickers if t not in found]

    dropped = []
    if missing:
        if len(missing) <= max_live_fallback:
            valid_extra = []
            for tk in missing:
                r = analyze(tk)
                if r:
                    valid_extra.append(r)
                else:
                    dropped.append(tk)  # หาไม่เจอแม้ลอง live fallback แล้ว (พิมพ์ผิด/delisted จริง)
            if valid_extra:
                have = pd.concat([have, pd.DataFrame(valid_extra)], ignore_index=True) if not have.empty else pd.DataFrame(valid_extra)
        else:
            dropped = missing  # เกิน threshold ไม่ลอง live fallback เลย (กันยิง Yahoo เยอะเกินไป)
    return have, dropped


st.set_page_config(
    page_title="Stock Screener Pro",
    page_icon="📊",
    layout="wide",
    # v3.37: เดิม "expanded" บังคับให้ sidebar เปิดค้างตอนโหลดหน้าแรกเสมอ ไม่ว่า
    # จอจะเล็กแค่ไหน — บนมือถือแปลว่า sidebar บังเต็มจอทันทีที่เข้าเว็บ ต้อง
    # กดปิดเองก่อนถึงจะเห็นอะไร (ตรงกับที่ user บอกว่า "sidebar บังหน้าจอเล็ก
    # เกินไป") เปลี่ยนเป็น "auto" ให้ Streamlit ตัดสินใจเองตามความกว้างจอ —
    # จอกว้าง (PC) ยังเปิดเหมือนเดิม จอแคบ (มือถือ) จะปิดให้อัตโนมัติ
    initial_sidebar_state="auto",
)
inject_css()

def main():
    st.markdown(f"""
    <div class="hud-frame" style="text-align:center;">
        <div class="hud-corner tl"></div><div class="hud-corner tr"></div>
        <div class="hud-corner bl"></div><div class="hud-corner br"></div>
        <h1 style="font-size:1.9rem;margin:0;letter-spacing:0.03em;">
            <span style="color:#ffffff;">INSTITUTIONAL STOCK SCREENER</span>
            <span style="font-size:0.85rem;color:var(--cyan);font-family:'Share Tech Mono',monospace;margin-left:8px;">v{APP_VERSION}</span>
        </h1>
        <p style="color:var(--text-dim);font-size:0.85rem;margin:6px 0 0 0;font-family:'Chakra Petch',sans-serif;letter-spacing:0.04em;">
            PRECISION MATH &nbsp;//&nbsp; MULTI-MARKET &nbsp;//&nbsp; HIDDEN GEM ENGINE &nbsp;//&nbsp; BACKTESTER
        </p>
    </div>
    """, unsafe_allow_html=True)

    # ── Session state init ──────────────────────────────────
    if "df" not in st.session_state: st.session_state.df = pd.DataFrame()
    if "watchlist" not in st.session_state:
        st.session_state.watchlist = load_watchlist()  # โหลดจาก disk แทนเริ่มเป็น [] เสมอ
    if "ran" not in st.session_state: st.session_state.ran = False

    # ── Sidebar ─────────────────────────────────────────────
    with st.sidebar:
        st.markdown("### ⚙️ ตั้งค่า")

        universe = st.selectbox("🌍 Universe | กลุ่มหุ้น", list(UNIVERSE_OPTIONS.keys()))

        sector_choice = []
        if universe == "Sector Focus | เลือกตามหมวด":
            sector_choice = st.multiselect("เลือก Sector | หมวดหุ้น", list(SECTOR_MAP.keys()),
                                           default=["Technology | เทคโนโลยี"])

        custom_input = ""
        if universe == "Custom Tickers":
            custom_input = st.text_area("Tickers (คั่นด้วย ,)", "AAPL,MSFT,NVDA,GOOGL", height=80)

        st.markdown("---")
        st.markdown("**🔬 Filters**")

        min_gem = st.slider("💎 Min Gem Score", 0, 10, 0)
        min_accum = st.slider("📦 Min Accum Score", 0, 5, 0)
        pat_filter = st.multiselect("EMA Pattern | รูปแบบเส้น EMA",
            ["🏆 Perfect Uptrend", "📈 Strong Uptrend", "✨ Golden Align",
             "🔥 Squeeze", "⚡ Pre-Squeeze", "🌱 Early Break", "🎯 EMA Fan"],
            default=[], placeholder="ทั้งหมด")

        st.markdown("---")
        with st.expander("📅 Timeframe"):
            period = st.selectbox("ช่วงเวลา | Period", ["1y", "2y", "6mo", "3mo"], index=0)
            interval = st.selectbox("Interval | ช่วงแท่งเทียน", ["1d", "1wk"], index=0)
            use_rs = st.checkbox("คำนวณ RS vs SPY", value=True,
                                  help="ช้าขึ้นเล็กน้อย แต่ได้ข้อมูลสำคัญ")

        max_tk = st.slider("Max Tickers | จำนวนหุ้นสูงสุด", 10, 300, 50, step=10,
                           help="v3.11: มีผลเฉพาะตอนกด 🚀 Run Screener (สแกนสด) เท่านั้น — "
                                "ข้อมูลที่ดึงไว้ล่วงหน้าอัตโนมัติ (ค่าเริ่มต้นตอนเปิดแอป ไม่ต้องกดอะไร) "
                                "จะแสดงครบทุกตัวใน Universe เสมอ ไม่ถูกจำกัดด้วยค่านี้")

        # v3.28: สวิตช์ "โหมดมือถือ" ให้ผู้ใช้กดเปิดเอง — ไม่ใช่การตรวจจับ
        # อุปกรณ์อัตโนมัติ (ทำแบบนั้นเสี่ยงกว่ามาก ทดสอบบนมือถือจริงในนี้
        # ไม่ได้เลย ถ้าบั๊กจะพังทั้งแอปสำหรับทุกคน) ให้ผู้ใช้กดเลือกเองปลอดภัย
        # กว่า ตารางจะเหลือแค่คอลัมน์ที่จำเป็นสุด ลดการเลื่อนซ้ายขวา
        mobile_mode = st.checkbox("📱 โหมดมือถือ (คอลัมน์น้อยลง อ่านง่ายบนจอเล็ก)", value=False,
                                  help="เหลือแค่ Ticker/Price/Support/Support Zone/MktCap ตัดคอลัมน์ที่ไม่จำเป็นออก "
                                       "กดเปิดเองตอนใช้บนมือถือ ปิดกลับได้ตลอดเวลา")

        # v3.34: "โหมดเรียบง่าย" สำหรับ Desktop — เปิดเป็นค่าเริ่มต้น (True)
        # ตามที่ตัดสินใจไว้ว่าปัญหาจริงคือ "การแสดงผลแน่นเกินไป" ไม่ใช่ตัวระบบ
        # ข้างในผิด — ทางแก้นี้แค่ลดคอลัมน์ที่โชว์ ไม่แตะ logic การคำนวณใดๆ
        # เลย ปลอดภัยกว่าการรื้อทำใหม่ทั้งหมดมาก ปิดเพื่อดูแบบละเอียดได้เสมอ
        simple_mode = st.checkbox("🎯 โหมดเรียบง่าย (แนะนำ — เห็นเฉพาะสิ่งจำเป็นต่อการตัดสินใจ)",
                                  value=True,
                                  help="เหลือแค่ Ticker/Price/Support/Support Zone/Risk:Reward/ระดับ "
                                       "ปิดโหมดนี้เพื่อดูคอลัมน์ละเอียดครบทุกตัว (Support Quality, Sector, "
                                       "Trend Age ฯลฯ) สำหรับคนที่อยากเจาะลึก")

        st.markdown("---")
        run_btn = st.button("🚀 Run Screener | สแกนสดเดี๋ยวนี้", use_container_width=True,
                            help="ปกติไม่ต้องกดเลย — ข้อมูลมาจากรอบดึงอัตโนมัติทุกวันหลังตลาดปิด อยู่แล้ว "
                                 "กดปุ่มนี้เฉพาะตอนอยากได้ข้อมูลสดเดี๋ยวนี้ ไม่รอรอบถัดไป "
                                 f"(จำกัดสแกนสดไว้ไม่เกิน {LIVE_SCAN_SAFETY_CAP} หุ้น เพื่อไม่ให้กระทบ "
                                 "คนอื่นที่เปิดแอปพร้อมกัน — universe ใหญ่กว่านี้รอรอบอัตโนมัติแทน)")

        with st.expander("💾 Export | ส่งออกข้อมูล"):
            if not st.session_state.df.empty:
                csv = st.session_state.df.to_csv(index=False)
                st.download_button("⬇️ Download CSV", csv,
                    f"screener_{datetime.date.today()}.csv", "text/csv",
                    use_container_width=True)
            else:
                st.caption("รัน Screener ก่อน")

        with st.expander("🗑️ ล้าง Cache (เฉพาะของสแกนสด/manual)"):
            st.caption("ใช้ลบเฉพาะ cache ของการกด 'Run Screener' สแกนสดเอง "
                      "ไม่กระทบข้อมูล prefetch อัตโนมัติทุกวันหลังตลาดปิด (อันนั้นอัปเดตเองจาก GitHub Action)")
            if st.button("ล้าง Cache ของ Universe นี้", use_container_width=True):
                tickers_for_clear = resolve_tickers(universe, sector_choice, custom_input)[:max_tk][:LIVE_SCAN_SAFETY_CAP]
                if clear_cache_for(universe, tuple(tickers_for_clear), period, interval):
                    st.success("ล้างแล้ว — กด Run Screener เพื่อสแกนสดใหม่")
                else:
                    st.info("ยังไม่มี Cache สแกนสดสำหรับ Universe นี้")

        st.markdown("---")
        st.markdown(f"<p style='color:#44587f;font-size:0.72rem;'>Data: Yahoo Finance<br>"
                    f"ข้อมูลหลัก: ดึงอัตโนมัติทุกวันหลังตลาดปิด ผ่าน GitHub Action<br>"
                    f"Watchlist: {len(st.session_state.watchlist)} หุ้น (persist ข้าม session)</p>",
                    unsafe_allow_html=True)

    # ── Resolve tickers ──────────────────────────────────────
    tickers_all = resolve_tickers(universe, sector_choice, custom_input)
    tickers_use = tickers_all[:max_tk]

    # v3.10: ดูคอมเมนต์ที่ LIVE_SCAN_SAFETY_CAP (module-level ด้านบน) — จำกัด
    # ขนาดสแกนสดไว้ต่ำกว่า max_tk เสมอ กันไม่ให้กระทบผู้ใช้คนอื่นบน server เดียวกัน
    live_tickers_use = tickers_use[:LIVE_SCAN_SAFETY_CAP]

    auto_loaded = False
    bundle_gen_at = None
    bundle_app_version = None

    # ── Run screener (กดเอง = สแกนสดตอนนี้เลย ไม่รอรอบ prefetch ทุกวันหลังตลาดปิด) ──
    if run_btn:
        if len(tickers_use) > LIVE_SCAN_SAFETY_CAP:
            st.warning(f"⚠️ จำกัดสแกนสดไว้ที่ {LIVE_SCAN_SAFETY_CAP} หุ้นแรก (เลือกไว้ {len(tickers_use)} ตัว) "
                      f"เพื่อไม่ให้การสแกนสดของคุณไปทำให้คนอื่นที่เปิดแอปพร้อมกันหน่วง (แชร์ server "
                      f"เดียวกัน) ถ้าต้องการดูครบทุกตัวในกลุ่มนี้ รอรอบอัตโนมัติหลังตลาดปิดแทนได้เลย")
        bench_tuple = None
        if use_rs:
            with st.spinner("ดึงข้อมูล SPY เป็น benchmark…"):
                try:
                    spy_df = yf.Ticker("SPY").history(period="1y", interval="1d", auto_adjust=True)
                    bench_tuple = make_bench_tuple(spy_df)
                except Exception as e:
                    log_err("fetch SPY benchmark", e)
                    st.warning("ดึงข้อมูล SPY ไม่สำเร็จ — จะสแกนต่อโดยไม่มี Relative Strength")

        prog = st.progress(0.0, text=f"⚡ กำลังสแกน 0/{len(live_tickers_use)} หุ้น…")

        def _on_progress(done, total):
            prog.progress(done / total if total else 1.0, text=f"⚡ กำลังสแกน {done}/{total} หุ้น…")

        df = batch_scan(tuple(live_tickers_use), period, interval, bench_tuple, progress_cb=_on_progress)
        prog.empty()
        st.session_state.df = df
        st.session_state.ran = True
        save_disk_cache(universe, tuple(live_tickers_use), period, interval, df)

    # ── ดีฟอลต์ (ไม่กด Run): อ่านจากข้อมูลที่ดึงไว้ล่วงหน้าทุกวันหลังตลาดปิด (v3.2 ใหม่) ──
    # เปลี่ยนจาก v3.0/3.1 ที่ต้องรอให้มีคนกด Run ก่อนถึงจะมีข้อมูล — ตอนนี้แอป
    # ไม่ได้ไปคุยกับ Yahoo ตอนคนเข้าดูเลย แค่อ่านไฟล์ที่ fetch_data.py
    # (รันจาก GitHub Action ทุกวันหลังตลาดปิด) เตรียมไว้ให้แล้ว
    else:
        bundle_gen_at, bundle_df, bundle_app_version = load_prefetched_bundle()
        if bundle_gen_at:
            # v3.11: BUG FIX — เดิมกรอง bundle ด้วย tickers_use (ตัดตาม
            # max_tk แบบเรียงตัวอักษรก่อนแล้วค่อยกรอง) แปลว่าต่อให้ bundle มี
            # ข้อมูลครบทั้ง universe (503 ตัวของ S&P 500) อยู่แล้ว แอปก็จะโชว์
            # ให้เห็นแค่ "ตัวแรกตามตัวอักษร" ของ max_tk เสมอ (ไม่เกี่ยวกับมูลค่า
            # บริษัท/สัญญาณ/คุณภาพใดๆ) หุ้นที่น่าสนใจแต่ชื่อขึ้นต้นด้วยตัวอักษร
            # ท้ายๆจะไม่มีทางโผล่มาให้เห็นเลย ทั้งที่กรองจาก bundle ไม่มีต้นทุน
            # เพิ่มอะไรเลย (ข้อมูลอยู่ในหน่วยความจำแล้ว) — ตอนนี้ใช้ tickers_all
            # (universe เต็ม) แทน ไม่ตัดทิ้งอะไรก่อนกรองอีกต่อไป
            # v3.25 BUG FIX (พบจากที่ user รายงานว่าโหลดช้า): get_with_bundle_
            # fallback() ค่า default คือ max_live_fallback=15 — แปลว่าตอน
            # เปิดแอปปกติ (ไม่ได้กด Run Screener) ถ้ามี ticker เหลือไม่เจอใน
            # bundle ≤15 ตัว (เช่น Sector บางหมวดที่วันนั้นสแกนไม่ครบ) แอปจะ
            # แอบยิง Yahoo สดให้ "เงียบๆ" โดยไม่มีใครกดปุ่มอะไรเลย — ขัดกับ
            # หลักการที่วางไว้ทั้งระบบว่า "ไม่มีการยิง Yahoo สดถ้าไม่ได้กด Run
            # Screener" (คือที่มาของ LIVE_SCAN_SAFETY_CAP ที่ทำไว้ก่อนหน้า)
            # และเป็นสาเหตุที่หน้าเว็บโหลดช้าโดยไม่รู้ตัวว่าทำไม — Sector
            # Focus/Custom Tickers ที่ universe เล็กมีโอกาสโดนทางนี้บ่อยสุด
            # แก้โดยตั้ง max_live_fallback=0 สำหรับเส้นทาง auto-loaded นี้
            # โดยเฉพาะ (ticker ที่หาไม่เจอจะโชว์แค่ caption แจ้งเตือนแทน ไม่
            # พยายามดึงสดอีกต่อไป —ยังกด "🚀 Run Screener" เองได้เสมอถ้า
            # อยากได้ครบจริงๆ)
            have, dropped = get_with_bundle_fallback(tickers_all, bundle_df, max_live_fallback=0)
            st.session_state.df = have
            st.session_state.dropped_tickers = dropped
            st.session_state.ran = True
            auto_loaded = True
        elif not st.session_state.ran:
            st.session_state.df = pd.DataFrame()

    df = st.session_state.df

    # v3.22: รายการแนวรับแบบย่อในไซด์บาร์จริงๆ (ด้านซ้ายมือ ไม่ใช่แท็บ) ตามที่
    # ขอ — เรียงจากมูลค่าบริษัทมากไปน้อย ไม่ผูกกับ filter อื่นๆที่เลือกไว้ใน
    # ตารางหลัก (จะได้เป็นจุดอ้างอิงเร็วๆ ที่ไม่เปลี่ยนไปมาตามการปรับ filter)
    # แสดง Top 10 — เลือกเลขนี้เพราะไซด์บาร์แคบ เกิน 10 แถวต้องเลื่อนดูอยู่ดี
    # ไม่ต่างจากการดูในตารางหลักที่มีรายละเอียดครบกว่า จะได้กระชับจริงๆ
    if not df.empty and "Support" in df.columns and "MktCap$B" in df.columns:
        sup_df = df[df["Support"].isin(["🟢 อยู่ที่แนวรับ", "🟡 ใกล้แนวรับ"])].copy()
        if not sup_df.empty:
            sup_df = sup_df.sort_values("MktCap$B", ascending=False)
            top_n_side = 10
            top_side = sup_df.head(top_n_side)
            st.sidebar.markdown("---")
            st.sidebar.markdown(f"### 🟢 แนวรับ Top {len(top_side)}")
            st.sidebar.caption("เรียงมูลค่าบริษัทมาก→น้อย")
            rows_html = ""
            for _, r in top_side.iterrows():
                tier_icon = "🟢" if "อยู่ที่แนวรับ" in str(r["Support"]) else "🟡"
                mc = r.get("MktCap$B", 0) or 0
                px_r = r.get("Price", 0) or 0
                rows_html += (
                    f'<div style="display:flex;justify-content:space-between;align-items:center;'
                    f'padding:5px 2px;border-bottom:1px solid #22344f;font-size:0.82rem;">'
                    f'<span>{tier_icon} <b style="color:#e8f0ff;">{r["Ticker"]}</b> '
                    f'<span style="color:#5b7299;font-size:0.75rem;">${px_r:,.1f}</span></span>'
                    f'<span style="color:#93a8c9;">${mc:,.0f}B</span>'
                    f'</div>'
                )
            st.sidebar.markdown(f'<div style="margin-top:4px;">{rows_html}</div>', unsafe_allow_html=True)
            if len(sup_df) > top_n_side:
                st.sidebar.caption(f"อีก {len(sup_df) - top_n_side} ตัว — ดูครบในตารางหลัก")

    # v3.30: ย้ายมาจากตารางหลัก (checkbox "กรองด่วน" เดิม) มาไว้ในไซด์บาร์แทน
    # ตามที่ขอ — เป็นลิสต์กระชับใต้ "แนวรับ Top 10" (เรียงมูลค่า) อันบน แต่
    # เรียงตาม Support Quality แทน ใช้เกณฑ์เดียวกับเดิม: อยู่ที่แนวรับจริง
    # (ไม่ใช่แค่ใกล้) + Quality ≥6/10 — ยังคงไม่สร้างคะแนนรวมใหม่ ใช้ Support
    # Quality ตัวเดียวที่มีเครื่องมือพิสูจน์จริงอยู่แล้ว (Backtester → Support
    # Accuracy) ไม่ผูกกับ filter อื่นในตารางหลักเหมือนบล็อกแรก
    if not df.empty and "Support" in df.columns and "Support Quality" in df.columns:
        qual_df = df[(df["Support"] == "🟢 อยู่ที่แนวรับ") & (df["Support Quality"] >= 6)].copy()
        if not qual_df.empty:
            qual_df = qual_df.sort_values("Support Quality", ascending=False)
            top_n_qual = 10
            top_qual = qual_df.head(top_n_qual)
            st.sidebar.markdown("---")
            st.sidebar.markdown(f"### ⭐ แนวรับคุณภาพสูงสุด Top {len(top_qual)}")
            st.sidebar.caption("อยู่ที่แนวรับจริง + Quality ≥6/10 · เรียงคุณภาพมาก→น้อย")
            qual_rows_html = ""
            for _, r in top_qual.iterrows():
                px_r = r.get("Price", 0) or 0
                q = r.get("Support Quality", 0) or 0
                qual_rows_html += (
                    f'<div style="display:flex;justify-content:space-between;align-items:center;'
                    f'padding:5px 2px;border-bottom:1px solid #22344f;font-size:0.82rem;">'
                    f'<span>⭐ <b style="color:#e8f0ff;">{r["Ticker"]}</b> '
                    f'<span style="color:#5b7299;font-size:0.75rem;">${px_r:,.1f}</span></span>'
                    f'<span style="color:#ffd76a;font-weight:700;">{q:.1f}/10</span>'
                    f'</div>'
                )
            st.sidebar.markdown(f'<div style="margin-top:4px;">{qual_rows_html}</div>', unsafe_allow_html=True)
            if len(qual_df) > top_n_qual:
                st.sidebar.caption(f"อีก {len(qual_df) - top_n_qual} ตัว — ดูครบในตารางหลัก")
            st.sidebar.caption("⚠️ ไม่ใช่คำแนะนำการลงทุน แค่ช่วยลดตัวเลือกให้ดูง่ายขึ้น")
        else:
            st.sidebar.markdown("---")
            st.sidebar.caption("⭐ ยังไม่มีหุ้นที่เข้าเงื่อนไข Quality ≥6/10 ใน Universe นี้ตอนนี้")

    # ── แสดงสถานะ ──────────────────────────────────────
    if st.session_state.ran and not df.empty:
        if auto_loaded:
            try:
                gen_dt = datetime.datetime.fromisoformat(str(bundle_gen_at).replace("Z", "+00:00"))
                gen_lbl = gen_dt.astimezone(ZoneInfo("Asia/Bangkok")).strftime("%d/%m %H:%M น.")
            except Exception:
                gen_lbl = str(bundle_gen_at) or "—"

            # v3.26: โชว์ app_version ของ "ข้อมูล" เทียบกับ APP_VERSION ของ
            # "โค้ด" ที่รันอยู่ตอนนี้ — เดิมมีการ stamp version ไว้ในข้อมูลแล้ว
            # ตั้งแต่ v3.12 แต่ไม่เคยเอามาโชว์ให้ใครเห็นเลยสักที่ ทำให้ตอน
            # debug ปัญหา "คอลัมน์หาย" ต้องเดากันไปมาว่าเป็นเพราะ deploy ไม่
            # ครบหรือโค้ดมีบั๊ก ทั้งที่เช็คตรงนี้ที่เดียวก็รู้คำตอบทันที
            ver_html = ""
            if bundle_app_version and bundle_app_version != APP_VERSION:
                ver_html = (f' · <span style="color:#ffc857;">⚠️ ข้อมูล v{bundle_app_version} '
                           f'≠ โค้ด v{APP_VERSION} (สแกนด้วยโค้ดเก่ากว่า — รอ/สั่งรัน GitHub Action '
                           f'ใหม่เพื่อได้คอลัมน์ล่าสุดครบ)</span>')
            elif bundle_app_version:
                ver_html = f' · <span style="color:#5b7299;">data v{bundle_app_version}</span>'
            else:
                ver_html = ' · <span style="color:#ffc857;">⚠️ ข้อมูลนี้เก่ามาก (ก่อน v3.12 ที่เริ่ม stamp version)</span>'

            st.markdown(
                f'<div style="background:#101c33;border:1px solid #22344f;border-radius:8px;'
                f'padding:8px 14px;margin-bottom:10px;display:flex;align-items:center;gap:10px;flex-wrap:wrap;">'
                f'<span style="color:#34f5a4;font-size:0.85rem;">⚡ ข้อมูลล่วงหน้า — อัปเดตอัตโนมัติทุกวันหลังตลาดปิด</span>'
                f'<span style="color:#5b7299;font-size:0.8rem;">ดึงล่าสุด {gen_lbl} · {universe} · '
                f'{len(df)} หุ้น{ver_html}</span>'
                f'<span style="color:#44587f;font-size:0.75rem;">— ไม่ต้องรอ ไม่ต้องกด Run</span>'
                f'</div>', unsafe_allow_html=True)
            # v3.12: เดิม ticker ที่หาไม่เจอเลยใน bundle (delisted/rate-limit
            # ตอนสแกน/พิมพ์ผิดใน Custom) จะถูกตัดทิ้งแบบเงียบๆ ไม่บอกใครเลย
            # ตอนนี้บอกจำนวนให้รู้ตัว (ใช้ caption เบาๆ ไม่ใช่ warning สีแดง
            # เพราะสำหรับ universe ใหญ่ๆ มีหลุดไปบ้างเป็นเรื่องปกติ ไม่ใช่
            # ความผิดพลาด — ดูเหตุผลได้จาก fetch_data.py log ของแต่ละวัน)
            dropped = st.session_state.get("dropped_tickers", [])
            if dropped:
                dn = ", ".join(dropped[:15]) + (f" +{len(dropped)-15} ตัว" if len(dropped) > 15 else "")
                st.caption(f"ℹ️ {len(dropped)} ตัวไม่มีข้อมูล (delisted/rate-limit ชั่วคราว/พิมพ์ผิด): {dn}")
        else:
            age_lbl = cache_age_label(universe, tuple(live_tickers_use), period, interval)
            st.markdown(
                f'<div style="background:#101c33;border:1px solid #34f5a4;border-radius:8px;'
                f'padding:8px 14px;margin-bottom:10px;display:flex;align-items:center;gap:10px;">'
                f'<span style="color:#34f5a4;font-size:0.85rem;">✅ สแกนสดเสร็จแล้ว (manual)</span>'
                f'<span style="color:#5b7299;font-size:0.8rem;">{age_lbl} · {universe} · '
                f'{len(df)} หุ้น · บันทึกแล้ว</span>'
                f'</div>', unsafe_allow_html=True)
    elif st.session_state.ran and df.empty and bundle_gen_at:
        st.warning("⚠️ Universe นี้ยังไม่อยู่ในข้อมูลที่ดึงไว้ล่วงหน้า — กด 🚀 Run Screener "
                  "เพื่อดึงสดสำหรับ Universe นี้แทน")


    # ── TABS ────────────────────────────────────────────────
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📊 Dashboard | แดชบอร์ด",
        "💎 Hidden Gems | หุ้นซ่อนเร้น",
        "🔍 Deep Dive | เจาะลึกหุ้น",
        "📈 Backtester | ทดสอบย้อนหลัง",
        "🗺️ Sector Map | แผนผังกลุ่มหุ้น",
        "⭐ Watchlist | รายการเฝ้าดู",
    ])

    # ════════════════════════════════════════════════════════
    # TAB 1: DASHBOARD
    # ════════════════════════════════════════════════════════
    with tab1:
        if not st.session_state.ran:
            st.markdown("""
            <div style="text-align:center;padding:80px 0;color:#5b7299;">
                <div style="font-size:3rem;">📊</div>
                <h3 style="color:#93a8c9;">ยังไม่มีข้อมูลล่วงหน้าสำหรับ Universe นี้</h3>
                <p>ปกติข้อมูลจะโผล่ขึ้นอัตโนมัติ (ดึงทุกวันหลังตลาดปิด) — ถ้ายังไม่เห็น ลองกด
                🚀 Run Screener เพื่อดึงสดเองครั้งนี้</p>
            </div>""", unsafe_allow_html=True)
        elif df.empty:
            # v3.40 BUG FIX: เดิมข้อความนี้ขึ้นซ้ำกับ warning ด้านบนสุด (ก่อน
            # tabs ทั้งหมด ที่เช็ค "bundle_gen_at" อยู่แล้ว) เวลา df ว่างเปล่า
            # เพราะเงื่อนไข "ran และ df.empty" ตรงกันทั้งคู่ ทำให้เห็นข้อความ
            # เตือนซ้อนกัน 2 อันพร้อมกัน (เจอจากภาพที่ user ส่งมา) — ถ้า
            # bundle_gen_at มีอยู่แล้ว (แปลว่า warning บนสุดขึ้นไปแล้ว พร้อม
            # บริบทที่เจาะจงกว่า) ข้ามข้อความนี้ไปเลย ไม่ต้องซ้ำ
            if not bundle_gen_at:
                st.error("⚠️ ไม่พบข้อมูล — ลองกด 🚀 Run Screener เพื่อดึงสด หรือตรวจสอบ Ticker/อินเทอร์เน็ต")
        else:
            total = len(df)
            bulls = len(df[df["Trend"].str.contains("Bull", na=False)])
            gems = len(df[df["💎 Gem"].str.contains("Gem", na=False)]) if "💎 Gem" in df else 0
            at_support = len(df[df["Support"].str.contains("อยู่ที่แนวรับ", na=False)]) if "Support" in df else 0
            near_support = len(df[df["Support"].str.contains("ใกล้แนวรับ", na=False)]) if "Support" in df else 0
            avg_rsi = df["RSI"].mean() if "RSI" in df else 0

            # v3.21: ตัดการ์ด Strong Buy/Breakout ออก (มาจากระบบ Signal ที่
            # ตัดทิ้งทั้งระบบแล้ว) เพิ่ม "ใกล้แนวรับ" แทน เพราะแนวรับเป็น
            # แกนหลักของแอปนี้แล้ว
            cards = [
                {"label": "สแกน", "value": total, "color": "#ffffff"},
                {"label": "Bull Trend", "value": bulls, "color": "#34f5a4"},
                {"label": "อยู่ที่แนวรับ", "value": at_support, "color": "#34f5a4"},
                {"label": "ใกล้แนวรับ", "value": near_support, "color": "#ffc857"},
                {"label": "Hidden Gem", "value": gems, "color": "#ffd84d"},
                {"label": "Avg RSI", "value": round(float(avg_rsi), 1) if pd.notna(avg_rsi) else 0,
                 "color": "#5ee6ff", "decimals": 1},
            ]
            render_animated_metric_cards(cards)

            # v3.21: สรุป "วันนี้ทำอะไรดี" — เดิมอิงจำนวน Strong Buy/สัญญาณใหม่
            # (ระบบ Signal) ตอนนี้เปลี่ยนมาอิงแนวรับล้วนๆ ตามที่ตัดสินใจโฟกัส
            # กลยุทธ์ทั้งเว็บไปที่แนวรับ — เทียบกับเมื่อวานด้วยสถานะแนวรับแทน
            # สถานะ Signal เดิม
            summary_bits = []
            if at_support > 0:
                summary_bits.append(f"🟢 {at_support} ตัวอยู่ที่แนวรับตอนนี้")
            prev_date, prev_df = load_previous_snapshot()
            if (prev_date and not prev_df.empty and "Ticker" in prev_df.columns
                    and "Support" in prev_df.columns and "Support" in df.columns):
                today_sup = df.set_index("Ticker")["Support"]
                prev_sup = prev_df.set_index("Ticker")["Support"]
                common = today_sup.index.intersection(prev_sup.index)
                if len(common) > 0:
                    newly_in = int(((today_sup.loc[common] == "🟢 อยู่ที่แนวรับ") &
                                    (prev_sup.loc[common] != "🟢 อยู่ที่แนวรับ")).sum())
                    newly_out = int(((today_sup.loc[common] != "🟢 อยู่ที่แนวรับ") &
                                     (prev_sup.loc[common] == "🟢 อยู่ที่แนวรับ")).sum())
                    summary_bits.append(f"📊 เทียบกับ {prev_date}: {newly_in} ตัวเพิ่งเข้าแนวรับใหม่, "
                                        f"{newly_out} ตัวหลุดแนวรับไปแล้ว")
            if summary_bits:
                st.info("  ·  ".join(summary_bits))

            st.caption("⚠️ Support / 💎 Gem / Accum เป็นการให้คะแนนตามเงื่อนไขเทคนิคัลที่ตั้งไว้เอง "
                      "(RSI, Volume, MACD, EMA) **ยังไม่ผ่านการพิสูจน์ทางสถิติว่าทำนายผลตอบแทนได้จริง** "
                      "ใช้เป็นจุดเริ่มต้นไปวิเคราะห์ต่อ ไม่ใช่คำแนะนำซื้อขาย")

            with st.expander("📖 แนวรับคำนวณจากอะไร"):
                st.markdown("""
**🟢 Support (แนวรับ)** หาจาก 2 แหล่ง แล้วให้คะแนนความแข็งแกร่ง (Support Quality 0-10) จาก 4 ปัจจัย ก่อนเลือกแนวรับที่ "คุ้มจะดูที่สุด" — ไม่ใช่แค่ตัวที่ใกล้ราคาที่สุด:
- **Swing Low** — จุดต่ำสุดในอดีต (จากกราฟรายสัปดาห์) ที่ราคาเคยเด้งกลับขึ้นมาแล้วจริง
- **EMA50 / EMA200** — เส้นค่าเฉลี่ยที่ราคามักเด้งกลับเมื่อแตะ

**ปัจจัยให้คะแนน Support Quality:**
1. **Touch Count** — แนวรับนี้เคยโดนทดสอบ (ราคาเข้ามาใกล้แล้วเด้งกลับ) กี่ครั้ง ยิ่งเยอะยิ่งน่าเชื่อ
2. **Volume Confirmation** — ตอนเด้งกลับมี volume สูงกว่าปกติไหม (มีแรงซื้อจริงรองรับ)
3. **Confluence** — Swing Low บังเอิญตรงกับ EMA50/200 พอดีไหม (แนวรับจากคนละวิธีมาบรรจบกัน = หนักแน่นกว่า)
4. **ระยะห่างจากราคาปัจจุบัน** — ต้องใกล้พอจะมีความหมายตอนนี้ (ตัดทิ้งถ้าไกลเกิน 6%)

แบ่งสถานะตามระยะห่างจากแนวรับ: **🟢 อยู่ที่แนวรับ** (ห่าง ≤1.5%) / **🟡 ใกล้แนวรับ** (ห่าง 1.5-4%) / ไม่แสดงถ้าไกลกว่านั้น

**Support Quality ≥6/10** ถือว่าน่าสนใจเป็นพิเศษ (มีในส่วนขยายใต้ตารางหลัก) เพราะผ่านการทดสอบหลายปัจจัยพร้อมกัน

⚠️ **คำเตือนสำคัญ:** แนวรับในอดีตไม่ได้การันตีว่าจะหยุดราคาได้อีกในอนาคต แม้ Quality Score จะสูงก็ตาม ถ้าหลุดแนวรับลงไปมักลงต่อแรง ควรมีจุดตัดขาดทุนเสมอ — ไปที่แท็บ **Backtester → Support Accuracy** เพื่อดูหลักฐานจริงว่า "อยู่ที่แนวรับ" ในอดีตเด้งกลับขึ้นจริงกี่ % เทียบกับ Buy & Hold ก่อนตัดสินใจเชื่อ
                """)

            fc1, fc2, fc3 = st.columns(3)
            with fc1:
                trend_filter = st.multiselect("Trend | แนวโน้ม", ["🟢 Bull", "🔴 Bear"],
                                              default=[], key="d_tr", placeholder="ทั้งหมด")
                wk_filter = st.multiselect("Weekly Trend | แนวโน้มรายสัปดาห์ (ตัวกรองเสริม)",
                                           ["🟢 Weekly Bull", "🔴 Weekly Bear", "🟡 Weekly Mixed"],
                                           default=[], key="d_wk", placeholder="ทั้งหมด")
            with fc2:
                sq_filter = st.multiselect("Squeeze | การหดตัว", df["Squeeze"].unique().tolist() if "Squeeze" in df else [],
                                           default=[], key="d_sq", placeholder="ทั้งหมด")
            with fc3:
                # v3.21: pre-select "อยู่ที่แนวรับ"/"ใกล้แนวรับ" เป็นค่าเริ่มต้น
                # — แนวรับเป็นแกนกลยุทธ์หลักของทั้งเว็บแล้ว ทำให้เป็นมุมมอง
                # default ตอนเปิดแอป ยังเลือกดูทั้งหมดได้ถ้าอยากปิด filter นี้
                sup_filter = st.multiselect("Support | แนวรับ",
                                            ["🟢 อยู่ที่แนวรับ", "🟡 ใกล้แนวรับ"],
                                            default=["🟢 อยู่ที่แนวรับ", "🟡 ใกล้แนวรับ"],
                                            key="d_sup", placeholder="ทั้งหมด")
                res_filter = st.multiselect("Resistance | แนวต้าน",
                                            ["🔴 อยู่ที่แนวต้าน", "🟠 ใกล้แนวต้าน"],
                                            default=[], key="d_res", placeholder="ทั้งหมด")

            # v3.27: ตัด Weekly Trend/EMA Pattern/Squeeze/RS 20D ออกตามที่ขอ
            # (ลดความรก) — RS 20D ถูกใช้ปรับ Support Quality อยู่เบื้องหลัง
            # แล้ว (v3.24) ไม่เสียข้อมูลจริงแม้ตัดคอลัมน์แสดงผลออก
            #
            # v3.28: โหมดมือถือ — เหลือแค่คอลัมน์ที่จำเป็นสุดสำหรับตัดสินใจซื้อ
            # (Ticker/Price/Support/Support Zone/Support Dist%/MktCap) กดเปิด
            # เองจากไซด์บาร์ ไม่ใช่ auto-detect
            # v3.31: เพิ่ม 4 ตัวช่วยแยกแยะหุ้น (ตามที่ขอ — "หน้าตาเหมือนกันหมด")
            # ทั้งหมดใช้ข้อมูลที่ validate แล้วหรือแค่จัดรูปแบบใหม่ ไม่สร้าง
            # คะแนนรวมใหม่แบบที่เคยตัดทิ้งไปในแท็บ "หุ้นน่าติดตาม"
            df = df.copy()
            if "Support Quality" in df.columns:
                df["ระดับ"] = df["Support Quality"].apply(support_tier)
            # ข้อ 4: Sector Bull% — โยง Sector Heatmap ที่มีอยู่แล้วเข้ากับหุ้น
            # แต่ละตัวผ่านคอลัมน์ Sector (v3.31 ใหม่ใน analyze())
            _, sector_hm_df = load_prefetched_sector_heatmap()
            if sector_hm_df is not None and not sector_hm_df.empty and "Sector" in df.columns:
                bull_map = dict(zip(sector_hm_df["Sector"], sector_hm_df.get("Bull %", pd.Series(dtype=float))))
                df["Sector Bull%"] = df["Sector"].map(bull_map)

            if mobile_mode:
                # v3.37: ตัดให้เหลือน้อยที่สุดเท่าที่จะน้อยได้ ตามที่ขอตรงๆ
                # (ราคาปิด/เปิด, โซนแนวรับ, เงื่อนไข) — เดิมยังมี Support Dist%/
                # MktCap$B ซึ่งเกินความจำเป็นสำหรับโหมดนี้
                show_cols = [c for c in ["Ticker", "Price", "ราคาปิด", "Support", "Support Zone"]
                             if c in df.columns]
            elif simple_mode:
                # v3.36: แยกงาน "สแกนกว้าง" (Dashboard/Hidden Gems) ออกจากงาน
                # "หาจุดเข้า" (Deep Dive) อย่างชัดเจนตามที่ตกลงกัน — Dashboard
                # ควรเบาที่สุด แค่บอกว่า "ตัวไหนน่าคลิกเข้าไปดูต่อ" เท่านั้น
                # ตัด Risk:Reward ออกจากที่นี่ (เป็นข้อมูลระดับ "ตัดสินใจ" ไม่ใช่
                # "ค้นหา") ย้ายไปอยู่ที่ Deep Dive อย่างเดียว (ดู sup_badge ด้านล่าง
                # ที่เพิ่ม Risk:Reward/Support Age/Sector เข้าไปแล้ว)
                show_cols = [c for c in ["Ticker", "Price", "Support", "Support Zone", "ระดับ"]
                             if c in df.columns]
            else:
                show_cols = [c for c in ["Ticker", "Price", "ราคาปิด", "Sector", "Trend", "RSI",
                                         "Support", "Support Zone", "Support Dist%", "Support Age",
                                         "Support Quality", "ระดับ", "Support Touches",
                                         "Resistance", "Resistance Zone", "Resistance Dist%", "Risk:Reward",
                                         "Trend Age", "💎 Gem", "Accum", "Sector Bull%", "MktCap$B", "Stars"]
                             if c in df.columns]
            dfv = df[show_cols].copy()
            if "Support Dist%" in dfv.columns:
                dfv["Support Dist%"] = dfv["Support Dist%"].apply(
                    lambda x: f"+{x:.1f}%" if pd.notna(x) else "—")
            if "Support Quality" in dfv.columns:
                dfv["Support Quality"] = dfv["Support Quality"].apply(
                    lambda x: f"{x:.1f}/10" if pd.notna(x) and x > 0 else "—")
            if "Support Touches" in dfv.columns:
                dfv["Support Touches"] = dfv["Support Touches"].apply(
                    lambda x: f"{int(x)}x" if pd.notna(x) and x > 0 else "—")
            if "Support Age" in dfv.columns:
                dfv["Support Age"] = dfv["Support Age"].apply(
                    lambda x: f"{int(x)}d" if pd.notna(x) and x > 0 else ("ใหม่" if pd.notna(x) else "—"))
            if "Resistance Dist%" in dfv.columns:
                dfv["Resistance Dist%"] = dfv["Resistance Dist%"].apply(
                    lambda x: f"+{x:.1f}%" if pd.notna(x) else "—")
            if "Risk:Reward" in dfv.columns:
                dfv["Risk:Reward"] = dfv["Risk:Reward"].apply(
                    lambda x: f"1:{x:.1f}" if pd.notna(x) and x > 0 else "—")
            if "Sector Bull%" in dfv.columns:
                dfv["Sector Bull%"] = dfv["Sector Bull%"].apply(
                    lambda x: f"{x:.0f}%" if pd.notna(x) else "—")

            if "Trend Age" in dfv.columns:
                dfv["Trend Age"] = dfv["Trend Age"].apply(
                    lambda x: f"{int(x)}d ago" if isinstance(x, (int, float)) and x >= 0 else "—")

            mask = pd.Series(True, index=dfv.index)
            if trend_filter: mask &= df["Trend"].apply(lambda x: any(t in str(x) for t in trend_filter))
            if wk_filter and "Weekly Trend" in df.columns:
                mask &= df["Weekly Trend"].isin(wk_filter)
            if sq_filter: mask &= df["Squeeze"].isin(sq_filter)
            if sup_filter and "Support" in df.columns: mask &= df["Support"].isin(sup_filter)
            if res_filter and "Resistance" in df.columns: mask &= df["Resistance"].isin(res_filter)
            if min_gem > 0 and "Gem Score" in df.columns: mask &= df["Gem Score"] >= min_gem
            if min_accum > 0 and "Accum Score" in df.columns: mask &= df["Accum Score"] >= min_accum
            if pat_filter and "EMA Pattern" in df.columns:
                mask &= df["EMA Pattern"].apply(lambda x: any(p in str(x) for p in pat_filter))
            dfv = dfv[mask]

            # v3.21: เดิมเรียงตาม Signal priority ก่อน (Strong Buy/Breakout/...)
            # แล้วค่อยเรียงตามแนวรับเป็นตัวรอง — ตอนนี้ตัด Signal ออกทั้งระบบ
            # แล้ว เปลี่ยนมาเรียงตาม "อยู่ที่แนวรับก่อน ใกล้แนวรับรองลงมา"
            # แล้วเรียงตาม "มูลค่าบริษัทมากไปน้อย" ตามที่ขอ (บริษัทใหญ่ก่อน
            # ภายในกลุ่มแนวรับเดียวกัน) ใช้ค่าดิบจาก df ผ่าน index เพราะ dfv
            # ถูกแปลงเป็น string แสดงผลไปแล้วบางคอลัมน์
            sup_tier_map = {"🟢 อยู่ที่แนวรับ": 0, "🟡 ใกล้แนวรับ": 1}
            if "Support" in df.columns:
                dfv["_st"] = df.loc[dfv.index, "Support"].map(sup_tier_map).fillna(2)
            else:
                dfv["_st"] = 2
            dfv["_mc"] = -df.loc[dfv.index, "MktCap$B"].fillna(0) if "MktCap$B" in df.columns else 0
            dfv["_sq"] = -df.loc[dfv.index, "Support Quality"] if "Support Quality" in df.columns else 0

            dfv = dfv.sort_values(["_st", "_mc", "_sq"]).drop(columns=["_st", "_mc", "_sq"])

            # ════════════════════════════════════════════════════════
            # v3.39: การ์ดสรุป "ตัวเด่นวันนี้" ก่อน — ตามที่ตัดสินใจ (ทางเลือก A)
            # ย้ายเนื้อหาจาก expander "แนวรับคุณภาพสูง" (เดิมอยู่ใต้ตารางเต็ม)
            # มาไว้บนสุดแทน ในรูปแบบการ์ดที่เห็นง่ายใน 3 วินาที ไม่ต้องเลื่อน
            # หาในตารางยาวๆอีกต่อไป — ใช้เกณฑ์เดิมเป๊ะ (อยู่ที่แนวรับจริง +
            # Quality ≥6/10) ไม่สร้างคะแนนใหม่ ตารางเต็มพับเก็บไว้ใน expander
            # ด้านล่างสำหรับคนที่อยากดูครบทุกตัวจริงๆ
            # ════════════════════════════════════════════════════════
            st.markdown("### 🎯 ตัวเด่นวันนี้")
            if "Support Quality" in df.columns:
                top_pick_df = df[(df["Support"] == "🟢 อยู่ที่แนวรับ") & (df["Support Quality"] >= 6)].copy()
                top_pick_df = top_pick_df.sort_values("Support Quality", ascending=False).head(8)
            else:
                top_pick_df = pd.DataFrame()

            if top_pick_df.empty:
                st.info("ยังไม่มีหุ้นที่เข้าเงื่อนไขคุณภาพสูง (อยู่ที่แนวรับจริง + Quality ≥6/10) ใน Universe นี้ตอนนี้ "
                        "— ลองดูตารางทั้งหมดด้านล่าง หรือเปลี่ยน Universe/Sector")
            else:
                n_cols = 1 if mobile_mode else 4
                rows_needed = (len(top_pick_df) + n_cols - 1) // n_cols
                pick_idx = 0
                for _r in range(rows_needed):
                    cols = st.columns(n_cols)
                    for c in cols:
                        if pick_idx >= len(top_pick_df):
                            break
                        prow = top_pick_df.iloc[pick_idx]
                        with c:
                            with st.container(border=True):
                                tier_lbl = support_tier(prow["Support Quality"])
                                zone_lbl = prow.get("Support Zone", "—") or "—"
                                st.markdown(f"**{prow['Ticker']}** · ${prow['Price']:,.2f}")
                                st.caption(f"โซนแนวรับ: {zone_lbl}")
                                st.markdown(tier_lbl)
                                if st.button("ดูรายละเอียด →", key=f"card_btn_{prow['Ticker']}_{pick_idx}",
                                            use_container_width=True):
                                    st.session_state["dd_sel"] = prow["Ticker"]
                                    st.info(f"✅ เลือก **{prow['Ticker']}** แล้ว — แตะแท็บ 🔍 "
                                           f"**เจาะลึกหุ้น** ด้านบนเพื่อดูรายละเอียด")
                        pick_idx += 1

            st.markdown("---")
            with st.expander(f"📊 ดูหุ้นทั้งหมด ({len(dfv)} ตัว)", expanded=False):
                # v3.31: เพิ่ม style ระดับ (Tier) เข้าไปด้วย
                smap = {"💎 Gem": _sty_gem,
                        "Support": _sty_support, "Resistance": _sty_resistance, "ระดับ": _sty_tier}
                # v3.32: rename เป็น "English | ไทย" ตอนแสดงผลจริงเท่านั้น (ขั้นตอน
                # สุดท้ายก่อน dataframe เสมอ — sort/filter/style ด้านบนทั้งหมดใช้
                # ชื่อคอลัมน์เดิมตามปกติ ไม่ถูกกระทบ)
                dfv_th, smap_th = apply_thai_labels(dfv, smap)
                # v3.35: ลดความสูงตารางลงตอนโหมดมือถือ
                tbl_height = 320 if mobile_mode else 520
                # v3.37: กดแตะแถวในตารางได้เลย — เลือกได้ทีละแถว แตะแถวไหน →
                # เซฟ ticker นั้นไว้ให้แท็บ "เจาะลึกหุ้น" หยิบไปใช้ทันที
                ev = st.dataframe(make_table(dfv_th, smap_th, row_style_fn=_row_highlight_support),
                                  use_container_width=True, height=tbl_height,
                                  on_select="rerun", selection_mode="single-row", key="dash_tbl_select")
                try:
                    sel_rows = ev.selection.rows if hasattr(ev, "selection") else ev["selection"]["rows"]
                except Exception:
                    sel_rows = []
                if sel_rows:
                    clicked_ticker = dfv.iloc[sel_rows[0]]["Ticker"]
                    st.session_state["dd_sel"] = clicked_ticker
                    st.info(f"✅ เลือก **{clicked_ticker}** แล้ว — แตะแท็บ 🔍 **เจาะลึกหุ้น** ด้านบนเพื่อดูรายละเอียด")

            st.markdown("---")
            wl_col1, wl_col2 = st.columns([3, 1])
            with wl_col1:
                add_tk = st.text_input("➕ เพิ่มในรายการเฝ้าดู", placeholder="AAPL", key="wl_add")
            with wl_col2:
                st.markdown("<br>", unsafe_allow_html=True)
                if st.button("เพิ่ม Watchlist | เพิ่มรายการเฝ้าดู") and add_tk.strip():
                    tk = add_tk.strip().upper()
                    if tk not in st.session_state.watchlist:
                        st.session_state.watchlist.append(tk)
                        save_watchlist(st.session_state.watchlist)  # persist ทันที (ใหม่ v3.0)
                        st.session_state.pop("wl_df", None)  # v3.13: กัน watchlist tab โชว์ผลค้างเก่า
                        st.session_state.pop("wl_dropped", None)
                        st.success(f"เพิ่ม {tk} แล้ว")

    # ════════════════════════════════════════════════════════
    # TAB 2: HIDDEN GEMS
    # ════════════════════════════════════════════════════════
    with tab2:
        st.markdown("### 💎 Hidden Gem Finder")
        st.caption("หุ้นที่ EMA สวย + Volume สะสมเงียบๆ + ตลาดยังไม่สนใจ")

        if df.empty:
            st.info("รัน Screener ก่อนครับ")
        else:
            g_cols = st.columns(4)
            keywords = [("💎 Hidden Gem", "Hidden", "#ffd84d"),
                        ("🔭 Emerging Gem", "Emerging", "#34f5a4"),
                        ("🔬 Stealth Accum", "Stealth", "#b66bff"),
                        ("🔥 Squeeze", "Squeeze", "#ff3864")]
            for i, (lbl, kw, clr) in enumerate(keywords):
                cnt = df.apply(lambda r, kw=kw: kw in str(r.get("💎 Gem", "")) or
                               kw in str(r.get("EMA Pattern", "")) or
                               kw in str(r.get("Accum", "")), axis=1).sum()
                g_cols[i].metric(lbl, int(cnt))

            st.markdown("---")

            if "EMA Pattern" in df.columns:
                pat_vc = df["EMA Pattern"].value_counts().head(8)
                with st.expander("📊 EMA Pattern ที่พบ", expanded=True):
                    pc = st.columns(4)
                    for i, (pat, cnt) in enumerate(pat_vc.items()):
                        pc[i % 4].markdown(
                            f'<div style="background:#101c33;border:1px solid #22344f;border-radius:8px;'
                            f'padding:10px 14px;margin:3px 0;">'
                            f'<div style="font-size:0.85rem;font-weight:700;color:#e8f0ff;">{pat}</div>'
                            f'<div style="color:#5b7299;font-size:0.75rem;">{cnt} หุ้น</div></div>',
                            unsafe_allow_html=True)

            st.markdown("---")

            gf1, gf2 = st.columns(2)
            with gf1:
                gem_f = st.multiselect("💎 Gem Label | ระดับหุ้นซ่อนเร้น",
                    ["💎 Hidden Gem", "🔭 Emerging Gem", "👀 Watch"],
                    default=[], key="gf1", placeholder="ทั้งหมด")
            with gf2:
                acc_f = st.multiselect("📦 Accumulation | การสะสมหุ้น",
                    ["🔬 Stealth Accum", "📦 Quiet Accum", "🔍 Possible Accum", "👀 Watch"],
                    default=[], key="gf2", placeholder="ทั้งหมด")

            # v3.35: Hidden Gems ไม่เคยเช็ค mobile_mode/simple_mode มาก่อนเลย
            # ทั้งที่ Dashboard เช็คแล้วตั้งแต่ v3.28/3.34 — เป็นสาเหตุที่
            # กดโหมดมือถือแล้วแท็บนี้ยังกว้างเหมือนเดิม (เจอจาก user ส่งภาพ
            # มาให้ดู) แก้ให้เช็คเหมือนกันทุกแท็บที่มีตารางหลัก
            if mobile_mode:
                gem_show = [c for c in ["Ticker", "Price", "💎 Gem", "Support", "Support Zone"]
                           if c in df.columns]
            elif simple_mode:
                # v3.36: เหมือน Dashboard — ตัดรายละเอียดเชิงตัดสินใจ (Gem
                # Score ตัวเลข, Accum) ออก เหลือแค่ label ที่บอกว่า "น่าคลิก
                # เข้าไปดูต่อไหม" พอ
                gem_show = [c for c in ["Ticker", "Price", "💎 Gem", "Support"]
                           if c in df.columns]
            else:
                gem_show = [c for c in ["Ticker", "Price", "ราคาปิด", "💎 Gem", "Gem Score",
                                        "EMA Pattern", "Squeeze", "Accum", "Accum Score",
                                        "Support", "Support Dist%",
                                        "RSI", "Vol×20D", "RS 20D", "MktCap$B"] if c in df.columns]
            dfg = df[gem_show].copy()
            if "Support Dist%" in dfg.columns:
                dfg["Support Dist%"] = dfg["Support Dist%"].apply(
                    lambda x: f"+{x:.1f}%" if pd.notna(x) else "—")

            gm = pd.Series(True, index=dfg.index)
            if gem_f: gm &= df["💎 Gem"].isin(gem_f)
            if acc_f: gm &= df["Accum"].isin(acc_f)
            if min_gem > 0: gm &= df["Gem Score"] >= min_gem
            if min_accum > 0: gm &= df["Accum Score"] >= min_accum
            if pat_filter: gm &= df["EMA Pattern"].apply(lambda x: any(p in str(x) for p in pat_filter))
            dfg = dfg[gm]
            if "Gem Score" in dfg.columns:
                dfg = dfg.sort_values("Gem Score", ascending=False)

            # v3.9: ลดคอลัมน์ที่ style เหมือนกับ Dashboard (เหตุผลเดียวกัน)
            gsmap = {"💎 Gem": _sty_gem, "Gem Score": _sty_gs, "Support": _sty_support}
            st.markdown(f"**{len(dfg)} หุ้น**")
            dfg_th, gsmap_th = apply_thai_labels(dfg, gsmap)
            gem_tbl_height = 320 if mobile_mode else 540
            st.dataframe(make_table(dfg_th, gsmap_th, row_style_fn=_row_highlight_support),
                         use_container_width=True, height=gem_tbl_height)

            with st.expander("📖 อ่านค่า"):
                st.markdown("""
**💎 Gem Score (0–10)**
- **8–10** `💎 Hidden Gem` — EMA สวย + สะสมเงียบ + cap เล็ก
- **6–7** `🔭 Emerging Gem` — สัญญาณดี ยังไม่ครบ
- **4–5** `👀 Watch` — ควรติดตาม

**EMA Pattern**
- `🏆 Perfect Uptrend` — price > EMA5>10>20>50>100>200
- `🔥 Squeeze` — EMA 20/50/200 ชิดกัน < 2.5% → กำลังจะเบรค
- `🌱 Early Break` — เพิ่งข้าม EMA200 ขึ้นมา

**Squeeze Direction**
- `🔥 Squeezing` — bandwidth แคบลง → **ยังไม่สาย**
- `🌱 Just Broke` — เพิ่งเบรค → **รีบตัดสินใจ**
- `📈 Expanding` — กางออกแล้ว → อาจช้าไปแล้ว

---
⚠️ **คะแนนทั้งหมดด้านบนเป็น heuristic** (ให้คะแนนตามเงื่อนไขที่ตั้งเอง จากหลักการ
วิเคราะห์เทคนิคัลทั่วไป) **ไม่ได้ผ่านการ backtest พิสูจน์ทางสถิติ** ว่าหุ้นที่ได้
คะแนนสูงจะให้ผลตอบแทนจริงดีกว่าหุ้นทั่วไปหรือสุ่มเลือก — ใช้เป็นจุดเริ่มต้น
ไปวิจัยเพิ่มเติมเอง ไม่ใช่คำแนะนำการลงทุน
                """)

    # ════════════════════════════════════════════════════════
    # TAB 3: DEEP DIVE
    # ════════════════════════════════════════════════════════
    with tab3:
        st.markdown("### 🔍 วิเคราะห์รายตัว")

        pick_list = df["Ticker"].tolist() if not df.empty else tickers_use[:50]
        # v3.37: กันพัง — ถ้า dd_sel ถูกตั้งไว้จากการแตะแถวใน Dashboard ตอน
        # Universe หนึ่ง แล้วผู้ใช้สลับ Universe ก่อนมาเปิดแท็บนี้ ticker เดิม
        # อาจไม่อยู่ใน pick_list ของ Universe ใหม่แล้ว — st.selectbox จะ error
        # ทันทีถ้า session_state ค้างค่าที่ไม่อยู่ใน options ล้างค่าทิ้งก่อน
        # ถ้าไม่อยู่ในลิสต์ปัจจุบัน กลับไปใช้ค่าเริ่มต้น (ตัวแรกในลิสต์) แทน
        if "dd_sel" in st.session_state and st.session_state["dd_sel"] not in pick_list:
            del st.session_state["dd_sel"]
        d1, d2, d3 = st.columns([3, 1, 1])
        with d1:
            sel = st.selectbox("เลือกหุ้น | Select Ticker", pick_list, key="dd_sel")
        with d2:
            ch_h = st.selectbox("ความสูงกราฟ | Chart Height", [620, 700, 800, 500], index=0, key="dd_h")
        with d3:
            ch_iv = st.selectbox("Timeframe | ช่วงเวลากราฟ", ["D", "W", "60", "15"], index=0, key="dd_iv",
                                 format_func=lambda x: {"D": "รายวัน", "W": "สัปดาห์", "60": "1H", "15": "15M"}[x])

        if sel:
            row = None
            if not df.empty and sel in df["Ticker"].values:
                row = df[df["Ticker"] == sel].iloc[0].to_dict()

            # v3.43 BUG FIX: เดิมถ้า row เป็น None (ticker ไม่ได้อยู่ในข้อมูลที่
            # วิเคราะห์ไว้แล้ว — พบบ่อยกับ Custom Tickers ที่ยังไม่เคยกด Run
            # Screener) หน้าจะเงียบหายไปเฉยๆ ไม่มีคำอธิบายเลยว่าทำไม Support/
            # Resistance/Position Sizing/Accumulation Plan/Technical Detail
            # ถึงไม่โผล่มา (เจอจาก user ส่งภาพมาให้ดูว่า "ดึงข้อมูลไม่ขึ้น
            # ไม่มีอะไรเลย") เหลือแค่กราฟ TradingView กับปุ่มดึงราคาสดที่ไม่ได้
            # พึ่ง row เลย ทำให้ดูเหมือนหน้าเว็บพังทั้งที่จริงๆแค่ยังไม่ได้
            # วิเคราะห์ ticker ตัวนี้ — เพิ่มข้อความอธิบายชัดเจน + ทางแก้ตรงๆ
            if row is None:
                st.warning(f"⚠️ ยังไม่มีข้อมูลวิเคราะห์ (Support/Resistance/Position Sizing ฯลฯ) "
                          f"สำหรับ **{sel}** เพราะ ticker นี้ไม่ได้อยู่ใน Universe ที่ดึงไว้ล่วงหน้า "
                          f"(พบบ่อยกับ Custom Tickers) — ยังดูกราฟราคา + ราคาสดด้านล่างได้ตามปกติ")
                # v3.43: แทนที่จะบอกให้ไปกด Run Screener (สแกนทั้ง Universe
                # ทั้งที่อยากดูแค่ตัวเดียว) — วิเคราะห์แค่ ticker นี้ตัวเดียว
                # ตรงนี้เลย เร็วกว่ามาก (ไม่ต้องรอสแกนหลายร้อยตัว)
                if st.button(f"🔍 วิเคราะห์ {sel} เดี๋ยวนี้ (ตัวเดียว เร็วกว่าสแกนทั้ง Universe)",
                            key="dd_analyze_now"):
                    with st.spinner(f"กำลังวิเคราะห์ {sel}…"):
                        try:
                            row = analyze(sel, bench_tuple=bench_tuple)
                        except Exception as e:
                            row = None
                            st.error(f"วิเคราะห์ {sel} ไม่สำเร็จ: {e} — ตรวจสอบว่าพิมพ์ ticker ถูกต้องไหม "
                                    f"(เช่น หุ้นไทยต้องมี .BK ต่อท้าย)")
                    if row:
                        st.success(f"✅ วิเคราะห์ {sel} สำเร็จ! (ผลนี้ยังไม่ถูกบันทึกลง Universe หลัก "
                                  f"— แค่แสดงผลชั่วคราวสำหรับหน้านี้เท่านั้น)")

            if row:
                px_now = row.get("Price", 0)
                pc_now = row.get("ราคาปิด", 0)
                chg_pct = round((px_now - pc_now) / pc_now * 100, 2) if pc_now else 0
                chg_col = "#34f5a4" if chg_pct >= 0 else "#ff3864"
                chg_arr = "▲" if chg_pct >= 0 else "▼"
                sq_now = row.get("Squeeze", "—")
                age_now = row.get("Trend Age", -1)
                age_str = f"{age_now}d ago" if isinstance(age_now, (int, float)) and age_now >= 0 else "—"
                rs20_now = row.get("RS 20D", np.nan)
                sup_now = row.get("Support", "—")
                sup_level_now = row.get("Support Level", np.nan)
                sup_dist_now = row.get("Support Dist%", np.nan)
                sup_quality_now = row.get("Support Quality", 0)
                sup_touches_now = row.get("Support Touches", 0)
                sup_vol_now = row.get("Support Vol Confirmed", False)
                sup_conf_now = row.get("Support Confluence", False)

                sup_zone_now = row.get("Support Zone", None)
                sup_age_now = row.get("Support Age", 0)
                sup_badge = ""
                if sup_now != "—" and pd.notna(sup_level_now):
                    sup_col = "#34f5a4" if "อยู่ที่แนวรับ" in str(sup_now) else "#ffc857"
                    tags = []
                    if sup_touches_now and sup_touches_now > 1:
                        tags.append(f"แตะแล้ว {int(sup_touches_now)} ครั้ง")
                    if sup_vol_now:
                        tags.append("Volume ยืนยัน")
                    if sup_conf_now:
                        tags.append("แนวรับซ้อนกัน")
                    # v3.36: ย้าย Support Age มาไว้ที่นี่ (แทนที่จะโชว์ในตาราง
                    # Dashboard แบบเดิม) เพราะเป็นข้อมูลระดับ "ตัดสินใจ" ไม่ใช่
                    # "ค้นหา" ตามที่แบ่งงานกันไว้ใหม่
                    if sup_age_now and sup_age_now > 0:
                        tags.append(f"อยู่ที่นี่มา {int(sup_age_now)} วัน")
                    tag_str = f" · {' · '.join(tags)}" if tags else ""
                    # v3.14: โชว์เป็น "โซน" ราคา (เช่น $60.20–$65.40) แทนราคา
                    # เป๊ะๆจุดเดียว ถ้าแนวรับมาจากหลาย swing low ที่กลุ่มกัน —
                    # ตรงกับการใช้งานจริงมากกว่า (แนวรับคือโซน ไม่ใช่เส้นตรง)
                    price_label = f"${sup_zone_now}" if sup_zone_now and sup_zone_now != "—" else f"${sup_level_now:,.2f}"
                    sup_badge = (
                        f'<span style="background:rgba(52,245,164,0.08);border:1px solid {sup_col};'
                        f'border-radius:6px;padding:4px 12px;font-size:0.85rem;font-weight:700;'
                        f'color:{sup_col};">'
                        f'{sup_now} {price_label} ({sup_dist_now:+.1f}%) · '
                        f'คุณภาพ {sup_quality_now:.1f}/10{tag_str}</span>'
                    )

                # v3.15: badge แนวต้าน คู่กับแนวรับ — เดิมมีแต่แนวรับ ไม่มี
                # เป้าหมายขาย/ทำกำไรให้ดูเลย
                res_now = row.get("Resistance", "—")
                res_level_now = row.get("Resistance Level", np.nan)
                res_zone_now = row.get("Resistance Zone", None)
                res_dist_now = row.get("Resistance Dist%", np.nan)
                res_quality_now = row.get("Resistance Quality", 0)
                res_badge = ""
                if res_now != "—" and pd.notna(res_level_now):
                    res_col = "#ff3864" if "อยู่ที่แนวต้าน" in str(res_now) else "#ffa857"
                    res_price_label = f"${res_zone_now}" if res_zone_now and res_zone_now != "—" else f"${res_level_now:,.2f}"
                    res_badge = (
                        f'<span style="background:rgba(255,56,100,0.08);border:1px solid {res_col};'
                        f'border-radius:6px;padding:4px 12px;font-size:0.85rem;font-weight:700;'
                        f'color:{res_col};">'
                        f'{res_now} {res_price_label} (+{res_dist_now:.1f}%) · '
                        f'คุณภาพ {res_quality_now:.1f}/10</span>'
                    )

                # v3.36: Risk:Reward badge — ย้ายมาจาก Dashboard ตามที่แบ่งงาน
                # กันใหม่ (Dashboard = ค้นหา, Deep Dive = ตัดสินใจ) คำนวณจาก
                # Support/Resistance เดียวกับที่ analyze() ทำไว้แล้ว
                rr_now = row.get("Risk:Reward", np.nan)
                rr_badge = ""
                if pd.notna(rr_now) and rr_now > 0:
                    rr_col = "#34f5a4" if rr_now >= 2 else ("#ffc857" if rr_now >= 1 else "#ff3864")
                    rr_badge = (
                        f'<span style="background:rgba(255,215,118,0.08);border:1px solid {rr_col};'
                        f'border-radius:6px;padding:4px 12px;font-size:0.85rem;font-weight:700;'
                        f'color:{rr_col};">⚖️ Risk:Reward 1:{rr_now:.1f}</span>'
                    )

                st.markdown(
                    f'<div style="display:flex;align-items:center;gap:16px;'
                    f'padding:10px 0 6px 0;flex-wrap:wrap;">'
                    f'<span style="font-size:2rem;font-weight:800;color:#ffffff;">'
                    f'${px_now:,.2f}</span>'
                    f'<span style="color:{chg_col};font-size:1.1rem;font-weight:700;">'
                    f'{chg_arr} {chg_pct}%</span>'
                    f'<span style="color:#5b7299;font-size:0.82rem;">ปิด: '
                    f'<b style="color:#93a8c9;">${pc_now:,.2f}</b></span>'
                    f'<span style="color:#5b7299;font-size:0.82rem;">ยืนเหนือ EMA200 มา: '
                    f'<b style="color:#ffd76a;">{age_str}</b></span>'
                    f'<span style="color:#5b7299;font-size:0.82rem;">Squeeze: '
                    f'<b style="color:#b66bff;">{sq_now}</b></span>'
                    f'<span style="color:#5b7299;font-size:0.82rem;">RS 20D: '
                    f'<b style="color:{"#34f5a4" if (rs20_now or 0) > 0 else "#ff3864"};">'
                    f'{rs20_now:.1f}%</b></span>'
                    f'{sup_badge}'
                    f'{res_badge}'
                    f'{rr_badge}'
                    f'</div>', unsafe_allow_html=True)

                # v3.36: บริบท Sector — ย้ายมาจาก Dashboard เช่นกัน
                sector_now = row.get("Sector", "—")
                if sector_now and sector_now != "—":
                    _, sector_hm_now = load_prefetched_sector_heatmap()
                    bull_now = None
                    if sector_hm_now is not None and not sector_hm_now.empty:
                        match = sector_hm_now[sector_hm_now["Sector"] == sector_now]
                        if not match.empty:
                            bull_now = match.iloc[0].get("Bull %")
                    bull_txt = f" — {bull_now:.0f}% ของหมวดนี้เป็นขาขึ้นตอนนี้" if bull_now is not None else ""
                    st.caption(f"📂 หมวด: {sector_now}{bull_txt}")

                ema_info = [(5, "#93a8c9"), (10, "#93a8c9"), (20, "#ffd76a"),
                            (50, "#2de2e6"), (100, "#b66bff"), (200, "#ff3864")]
                bdg = '<div style="display:flex;flex-wrap:wrap;gap:6px;margin:6px 0 12px 0;">'
                for n, col in ema_info:
                    ev = row.get(f"EMA{n}", None)
                    dev = row.get(f"vs EMA{n}%", None)
                    if ev and dev is not None:
                        dc = "#34f5a4" if dev > 0 else "#ff3864"
                        sgn = "+" if dev > 0 else ""
                        bdg += (f'<div style="background:#101c33;border:1px solid {col}40;'
                                f'border-radius:8px;padding:8px 12px;min-width:88px;">'
                                f'<div style="color:{col};font-size:0.68rem;font-weight:700;'
                                f'letter-spacing:0.05em;">EMA {n}</div>'
                                f'<div style="color:#ffffff;font-size:0.95rem;font-weight:700;">'
                                f'${ev:,.2f}</div>'
                                f'<div style="color:{dc};font-size:0.75rem;font-weight:600;">'
                                f'{sgn}{dev:.2f}%</div></div>')
                bdg += '</div>'
                st.markdown(bdg, unsafe_allow_html=True)

                # ════════════════════════════════════════════════════
                # v3.15: POSITION SIZING + RISK/REWARD CALCULATOR
                # ════════════════════════════════════════════════════
                # เพิ่มตามที่ขอ — ใช้แนวรับเป็นจุดตัดขาดทุนแนะนำ + แนวต้าน
                # เป็นเป้าหมายทำกำไรแนะนำ แล้วคำนวณจำนวนหุ้นที่ควรซื้อจาก
                # เงินทุน + % ความเสี่ยงที่ยอมรับได้ต่อไม้ (หลักการบริหารความ
                # เสี่ยงมาตรฐาน ไม่ใช่การทำนายราคา — เป็นแค่คณิตศาสตร์จาก
                # ตัวเลขที่ผู้ใช้กรอกเอง) ใช้ key ผูกกับ ticker (sel) เพื่อให้
                # ค่าเริ่มต้นอัปเดตอัตโนมัติเมื่อสลับหุ้น แต่ยังจำค่าที่เคย
                # กรอกเองไว้ได้ถ้ากลับมาดูหุ้นตัวเดิมอีกครั้งในเซสชันเดียวกัน
                with st.expander("🎯 คำนวณการเข้าซื้อ (Position Sizing + Risk/Reward)", expanded=False):
                    st.caption("⚠️ เครื่องมือคำนวณคณิตศาสตร์จากตัวเลขที่กรอกเอง ไม่ใช่การทำนายราคาหรือคำแนะนำการลงทุน "
                              "แนวรับ/แนวต้านในอดีตไม่การันตีว่าจะใช้ได้อีกในอนาคต")

                    default_stop = sup_level_now if (sup_now != "—" and pd.notna(sup_level_now)) else round(px_now * 0.95, 2)
                    default_target = res_level_now if (res_now != "—" and pd.notna(res_level_now)) else round(px_now * 1.10, 2)

                    pc1, pc2 = st.columns(2)
                    with pc1:
                        entry_px = st.number_input("💵 ราคาเข้าซื้อ (Entry)", min_value=0.01,
                                                   value=float(round(px_now, 2)), step=0.5,
                                                   key=f"pos_entry_{sel}")
                        stop_px = st.number_input("🛑 จุดตัดขาดทุน (Stop Loss) — default จากแนวรับ",
                                                  min_value=0.01, value=float(default_stop), step=0.5,
                                                  key=f"pos_stop_{sel}")
                        target_px = st.number_input("🎯 เป้าหมายทำกำไร (Target) — default จากแนวต้าน",
                                                    min_value=0.01, value=float(default_target), step=0.5,
                                                    key=f"pos_target_{sel}")
                    with pc2:
                        account_size = st.number_input("💰 เงินทุนทั้งหมด", min_value=0.0,
                                                        value=100000.0, step=10000.0,
                                                        key=f"pos_account_{sel}")
                        risk_pct = st.slider("⚖️ ยอมรับความเสี่ยงต่อไม้ (% ของเงินทุน)",
                                             0.25, 5.0, 1.0, step=0.25, key=f"pos_riskpct_{sel}")

                    risk_per_share = entry_px - stop_px
                    reward_per_share = target_px - entry_px

                    if risk_per_share <= 0:
                        st.error("⚠️ Stop Loss ต้องต่ำกว่าราคาเข้าซื้อ — เช็คตัวเลขอีกครั้ง")
                    elif reward_per_share <= 0:
                        st.error("⚠️ Target ต้องสูงกว่าราคาเข้าซื้อ — เช็คตัวเลขอีกครั้ง")
                    else:
                        rr_ratio = reward_per_share / risk_per_share
                        risk_amount = account_size * risk_pct / 100
                        shares = int(risk_amount // risk_per_share) if risk_per_share > 0 else 0
                        position_value = shares * entry_px
                        pct_of_account = (position_value / account_size * 100) if account_size > 0 else 0

                        rc1, rc2, rc3, rc4 = st.columns(4)
                        rc1.metric("Risk : Reward", f"1 : {rr_ratio:.2f}")
                        rc2.metric("ความเสี่ยง/หุ้น", f"${risk_per_share:,.2f}")
                        rc3.metric("กำไรเป้าหมาย/หุ้น", f"${reward_per_share:,.2f}")
                        rc4.metric("จำนวนหุ้นที่ควรซื้อ", f"{shares:,} หุ้น")

                        st.markdown(
                            f'<div style="background:#101c33;border:1px solid #22344f;border-radius:8px;'
                            f'padding:10px 14px;margin-top:6px;">'
                            f'<span style="color:#5b7299;font-size:0.85rem;">มูลค่าที่ต้องใช้: '
                            f'<b style="color:#e8f0ff;">${position_value:,.2f}</b> '
                            f'({pct_of_account:.1f}% ของเงินทุน) · เสี่ยงสูงสุด: '
                            f'<b style="color:#ff3864;">${shares * risk_per_share:,.2f}</b> '
                            f'({risk_pct:.2f}% ของเงินทุน)</span></div>',
                            unsafe_allow_html=True)

                        if rr_ratio < 1.5:
                            st.warning(f"⚠️ Risk:Reward ต่ำ (1:{rr_ratio:.2f}) — นักเทรดส่วนใหญ่แนะนำอย่างน้อย 1:1.5-2 "
                                      "ขึ้นไป ถึงจะคุ้มความเสี่ยงในระยะยาว แม้ Win Rate จะไม่ถึง 50% ก็ตาม")
                        if pct_of_account > 100:
                            st.warning("⚠️ มูลค่าที่ต้องใช้เกินเงินทุนทั้งหมด — ลด % ความเสี่ยงต่อไม้ลง "
                                      "หรือหา Stop Loss ที่ใกล้ราคาเข้าซื้อกว่านี้")
                        elif pct_of_account > 30:
                            st.warning(f"⚠️ ไม้นี้ใช้เงินทุนถึง {pct_of_account:.0f}% ของพอร์ต — กระจุกตัวสูง "
                                      "พิจารณาลดขนาดไม้เพื่อกระจายความเสี่ยงไปหุ้นตัวอื่นด้วย")

                    st.markdown("---")
                    # v3.18 ข้อ 5: "จุดที่ทฤษฎีผิด" — แยกจาก Stop Loss เชิงเทคนิค
                    # โดยตั้งใจ เพราะบางทีสองจุดนี้ราคาไม่เท่ากัน (เช่น Stop Loss
                    # อาจตั้งใกล้ๆเพื่อจำกัดขาดทุน แต่ "ทฤษฎีการลงทุนจะผิดจริง"
                    # อาจอยู่ลึกกว่านั้นที่แนวรับสำคัญถัดไป) การแยก 2 จุดนี้ชัดเจน
                    # ช่วยไม่ให้สับสนระหว่าง "ตัดขาดทุนเพราะวินัย" กับ "เลิกเชื่อ
                    # ไอเดียนี้แล้วจริงๆ" ซึ่งเป็นคนละเหตุผลกัน
                    default_invalid = round(default_stop * 0.97, 2)
                    invalid_px = st.number_input(
                        "❌ จุดที่ \"ทฤษฎีการลงทุนผิด\" (Thesis Invalidation) — ต่างจาก Stop Loss",
                        min_value=0.01, value=float(default_invalid), step=0.5, key=f"pos_invalid_{sel}",
                        help="ถ้าราคาหลุดจุดนี้ แปลว่าเหตุผลที่เคยเชื่อว่าหุ้นนี้น่าสนใจไม่จริงอีกต่อไป "
                             "ควรหยุดถัวเฉลี่ยเพิ่ม ไม่ใช่ซื้อเพิ่มเพราะ 'ยังเชื่ออยู่'")
                    st.caption(f"📍 ถ้าราคาหลุด **${invalid_px:,.2f}** ให้ถือว่าไอเดียนี้ผิดแล้ว หยุดซื้อเพิ่ม "
                              "ไม่ว่าจะรู้สึกอยากถัวเฉลี่ยแค่ไหนก็ตาม")

                    st.markdown("---")
                    st.markdown("**✅ Checklist ก่อนกดซื้อจริง** (ข้อ 4)")
                    chk1 = st.checkbox("เช็ค Win Rate ใน Backtester tab แล้ว ไม่ใช่แค่ดู Signal เฉยๆ", key=f"chk1_{sel}")
                    chk2 = st.checkbox("ตั้ง Stop Loss + จุดทฤษฎีผิดแล้ว (ด้านบน)", key=f"chk2_{sel}")
                    chk3 = st.checkbox("คำนวณ Position Size แล้ว ไม่ได้ซื้อตามความรู้สึก", key=f"chk3_{sel}")
                    chk4 = st.checkbox("รับความเสี่ยงที่จะขาดทุนเต็มจำนวนที่คำนวณไว้ได้จริง", key=f"chk4_{sel}")
                    if all([chk1, chk2, chk3, chk4]):
                        st.success("✅ ผ่านครบ 4 ข้อ — อย่างน้อยก็ตัดสินใจอย่างมีระบบ ไม่ใช่ตามอารมณ์")

                # v3.18 ข้อ 10: แผนถัวเฉลี่ยแบบมีขอบเขต — ตอบโจทย์ที่คุยกันไว้
                # ก่อนหน้าเรื่อง "หลุดแนวรับก็ซื้อเพิ่มเรื่อยๆ เพราะยังเชื่ออยู่"
                # ต่างจากนั้นตรงที่มีเพดานงบชัดเจน + จุดยกเลิกแผนตายตัว ไม่ใช่
                # ไล่ซื้อไม่มีที่สิ้นสุด
                with st.expander("📐 แผนถัวเฉลี่ยแบบมีขอบเขต (Accumulation Plan)", expanded=False):
                    st.caption("แบ่งงบเป็นไม้ตามแนวรับจริงที่มี พร้อมจุดยกเลิกแผนชัดเจน — "
                              "กันการ 'ซื้อเพิ่มเรื่อยๆ เพราะยังเชื่ออยู่' แบบไม่มีขอบเขต")

                    ac1, ac2 = st.columns(2)
                    with ac1:
                        total_budget = st.number_input("💰 งบรวมสูงสุดสำหรับหุ้นตัวนี้", min_value=0.0,
                                                        value=50000.0, step=5000.0, key=f"acc_budget_{sel}")
                    with ac2:
                        n_tranches = st.slider("จำนวนไม้ที่อยากแบ่ง", 2, 5, 3, key=f"acc_n_{sel}")

                    hist_df = _cached_history(sel, period, interval)
                    all_supports_below = []
                    if hist_df is not None and not hist_df.empty:
                        hist_df = hist_df.copy()
                        hist_df.index = pd.to_datetime(hist_df.index)
                        weekly_hist = resample_weekly_ohlc(hist_df)
                        all_sw = find_support_levels(weekly_hist, lookback=52, swing_window=2, min_bars=14)
                        all_supports_below = sorted(
                            [s for s in all_sw if s["level"] <= px_now],
                            key=lambda x: x["level"], reverse=True)

                    if not all_supports_below:
                        st.info("ไม่พบแนวรับที่ต่ำกว่าราคาปัจจุบันมากพอจะแบ่งไม้ — ลองดูหุ้นที่มีประวัติราคายาวกว่านี้")
                    else:
                        # ไม้แรกที่ราคาปัจจุบันเสมอ ไม้ที่เหลือไล่ตามแนวรับที่ลึกลงเรื่อยๆ
                        tranche_prices = [px_now] + [s["level"] for s in all_supports_below[:n_tranches - 1]]
                        tranche_prices = tranche_prices[:n_tranches]
                        budget_per_tranche = total_budget / len(tranche_prices)

                        plan_rows = []
                        for i, tp in enumerate(tranche_prices):
                            shares_i = int(budget_per_tranche // tp) if tp > 0 else 0
                            plan_rows.append({
                                "ไม้ที่": i + 1,
                                "ราคา": f"${tp:,.2f}",
                                "งบ/ไม้": f"${budget_per_tranche:,.0f}",
                                "จำนวนหุ้น": f"{shares_i:,}",
                            })
                        st.dataframe(pd.DataFrame(plan_rows), use_container_width=True, hide_index=True)

                        deepest = tranche_prices[-1]
                        cancel_point = round(deepest * 0.97, 2)
                        st.error(f"🛑 จุดยกเลิกแผนทั้งหมด: ถ้าราคาหลุด **${cancel_point:,.2f}** "
                                f"(ต่ำกว่าแนวรับลึกสุดที่ใช้ในแผน) ให้หยุดซื้อไม้ที่เหลือทันที "
                                "ไม่ว่าจะเหลืองบอีกเท่าไหร่ก็ตาม — แปลว่าแผนนี้ผิดแล้ว ไม่ใช่แค่ 'ยังไม่ถึงเวลา'")
                        if len(tranche_prices) < n_tranches:
                            st.caption(f"⚠️ หาแนวรับได้แค่ {len(tranche_prices)} ระดับ (ขอไว้ {n_tranches} ไม้) "
                                      "— แบ่งงบตามที่มีจริงเท่านั้น ไม่ได้ยัดไม้เพิ่มที่ไม่มีแนวรับรองรับ")

                # v3.18 ข้อ 7+9: บันทึกการตัดสินใจ + เตือน cooldown ถ้าขาดทุนติดกัน
                with st.expander("📝 บันทึกการตัดสินใจของตัวเอง", expanded=False):
                    st.caption("บันทึกไว้เพื่อย้อนดูทีหลังว่า **ตัวเอง** ตัดสินใจแม่นแค่ไหน — คนละเรื่องกับว่าระบบแม่นไหม")

                    dlog = load_decision_log()
                    streak = check_losing_streak(dlog)
                    if streak >= 3:
                        st.error(f"🛑 ขาดทุนติดกัน {streak} ไม้ล่าสุด — พักคิดก่อนเข้าไม้ถัดไปสักครู่ "
                                "เช็คว่ากำลังไล่ตามทุนคืน (revenge trading) อยู่หรือเปล่า")

                    dc1, dc2 = st.columns(2)
                    with dc1:
                        decision = st.selectbox("การตัดสินใจ", ["ซื้อ", "ไม่ซื้อ", "ขาย"], key=f"dec_choice_{sel}")
                    with dc2:
                        dec_note = st.text_input("เหตุผล/หมายเหตุ (ถ้ามี)", key=f"dec_note_{sel}")
                    if st.button("💾 บันทึก", key=f"dec_save_{sel}"):
                        # v3.19: FIX จาก self-audit — เพิ่ม "id" ไม่ซ้ำกันเลย
                        # (เดิม match ตอนอัปเดตผลลัพธ์ด้วย ticker+date+price ซึ่ง
                        # ไม่ unique จริง ถ้าบันทึก 2 รายการวันเดียวกัน ราคา
                        # เดียวกัน จะโดนอัปเดตพร้อมกันทั้งคู่โดยไม่ตั้งใจ)
                        dlog.append({
                            "id": f"{sel}_{datetime.datetime.now().isoformat()}_{len(dlog)}",
                            "date": datetime.date.today().isoformat(), "ticker": sel,
                            "decision": decision, "price": float(px_now), "note": dec_note,
                            "outcome": None, "outcome_note": "",
                        })
                        save_decision_log(dlog)
                        st.success(f"บันทึกแล้ว: {decision} {sel} @ ${px_now:,.2f}")
                        st.rerun()

                    recent = [e for e in dlog if e.get("ticker") == sel][-5:]
                    if recent:
                        st.markdown("**ประวัติล่าสุดของหุ้นตัวนี้**")
                        for i, entry in enumerate(reversed(recent)):
                            ec1, ec2 = st.columns([3, 1])
                            with ec1:
                                out_lbl = {"win": "✅ กำไร", "loss": "❌ ขาดทุน", None: "⏳ ยังไม่ปิด"}.get(
                                    entry.get("outcome"), "⏳ ยังไม่ปิด")
                                st.caption(f"{entry['date']} · {entry['decision']} @ ${entry['price']:,.2f} "
                                          f"· {out_lbl} · {entry.get('note') or '—'}")
                            with ec2:
                                if entry.get("outcome") is None:
                                    outcome_key = f"outcome_{sel}_{entry.get('id', i)}"
                                    picked = st.selectbox("ผลลัพธ์", ["ยังไม่ปิด", "กำไร", "ขาดทุน"],
                                                          key=outcome_key, label_visibility="collapsed")
                                    if picked != "ยังไม่ปิด":
                                        # v3.19: FIX จาก self-audit — match ด้วย "id" ที่ unique จริง
                                        # แทน ticker+date+price เดิม (ไม่ unique ถ้าบันทึกซ้ำวัน/ราคา
                                        # เดียวกัน จะโดนอัปเดตผิดตัวได้) รายการเก่าที่ไม่มี id (บันทึก
                                        # ไว้ก่อน v3.19) จะ fallback ไปใช้ ticker+date+price เหมือนเดิม
                                        target_id = entry.get("id")
                                        for e2 in dlog:
                                            if target_id is not None:
                                                if e2.get("id") == target_id:
                                                    e2["outcome"] = "win" if picked == "กำไร" else "loss"
                                            elif (e2.get("ticker") == entry["ticker"] and
                                                  e2.get("date") == entry["date"] and e2.get("price") == entry["price"]):
                                                e2["outcome"] = "win" if picked == "กำไร" else "loss"
                                        save_decision_log(dlog)
                                        st.rerun()

            st.caption("📈 กราฟจาก TradingView · 🟡 EMA20 · 🔵 EMA50 · 🔴 EMA200 · RSI · MACD")
            tv_chart(sel, height=ch_h, interval=ch_iv)

            st.markdown("---")

            fetch_live_btn = st.button("⚡ ดึงข้อมูลสด (Real-Time)", key="dd_live")
            if fetch_live_btn:
                with st.spinner("กำลังดึงข้อมูลสด…"):
                    rt = fetch_live(sel)
                if rt:
                    chg = rt.get("change") or 0
                    cc = "#34f5a4" if chg >= 0 else "#ff3864"
                    arr = "▲" if chg >= 0 else "▼"
                    cols_rt = st.columns(6)
                    cols_rt[0].metric("💰 ราคาสด", str(rt["price"]))
                    cols_rt[1].metric("📈 เปลี่ยน", f"{arr} {chg}%")
                    cols_rt[2].metric("🔼 High วันนี้", str(rt["high"]))
                    cols_rt[3].metric("🔽 Low วันนี้", str(rt["low"]))
                    cols_rt[4].metric("📊 Volume", rt["vol"])
                    cols_rt[5].metric("🏢 Mkt Cap", rt["cap"])

            if row:
                st.markdown("---")
                st.markdown("**📐 Technical Detail**")
                tc1, tc2, tc3 = st.columns(3)
                with tc1:
                    st.markdown('<p style="color:#5b7299;font-size:0.75rem;font-weight:700;'
                                'text-transform:uppercase;letter-spacing:0.06em;">MOMENTUM</p>',
                                unsafe_allow_html=True)
                    st.metric("RSI (14)", row.get("RSI", "—"))
                    st.metric("MACD Line", row.get("MACD", "—"))
                    st.metric("MACD Histogram", row.get("MACD_H", "—"))
                    st.metric("Gem Score", row.get("Gem Score", "—"))
                with tc2:
                    st.markdown('<p style="color:#5b7299;font-size:0.75rem;font-weight:700;'
                                'text-transform:uppercase;letter-spacing:0.06em;">VOLUME</p>',
                                unsafe_allow_html=True)
                    st.metric("Vol ×20D", f'{row.get("Vol×20D", "—")}×')
                    st.metric("Vol ×3M", f'{row.get("Vol×3M", "—")}×')
                    st.metric("Accum", row.get("Accum", "—"))
                    st.metric("RS 20D", f'{row.get("RS 20D", "—")}%')
                with tc3:
                    st.markdown('<p style="color:#5b7299;font-size:0.75rem;font-weight:700;'
                                'text-transform:uppercase;letter-spacing:0.06em;">PERFORMANCE</p>',
                                unsafe_allow_html=True)
                    st.metric("YTD Return", f'{row.get("YTD%", "—")}%')
                    st.metric("52W Drawdown", f'{row.get("Drawdown%", "—")}%')
                    st.metric("P/E Ratio", row.get("P/E", "—"))
                    st.metric("Div Yield", f'{row.get("Div%", "—")}%')

    # ════════════════════════════════════════════════════════
    # TAB 4: BACKTESTER
    # ════════════════════════════════════════════════════════
    with tab4:
        st.markdown("### 📈 Backtester — EMA Squeeze Strategy")
        st.caption("ทดสอบย้อนหลัง 2 ปี: ซื้อตอน EMA Bandwidth < 3% + ราคาเหนือ EMA200 "
                   "(เข้าซื้อที่ open ของแท่งถัดไปหลังสัญญาณเกิด ไม่ใช่ close ของแท่งสัญญาณเอง)")

        b1, b2, b3 = st.columns([3, 1, 1])
        with b1:
            bt_ticker = st.text_input("Ticker | ชื่อหุ้น", value="AAPL", key="bt_tk").upper()
        with b2:
            hold_d = st.selectbox("ถือกี่วัน | Hold Days", [10, 15, 20, 30], index=2, key="bt_hold")
        with b3:
            st.markdown("<br>", unsafe_allow_html=True)
            run_bt = st.button("▶️ Run Backtest | เริ่มทดสอบ", key="bt_run")

        if run_bt and bt_ticker:
            with st.spinner(f"กำลัง Backtest {bt_ticker}…"):
                res = backtest(bt_ticker, hold_d)

            if "error" in res:
                st.error(f"❌ {res['error']}")
            elif res.get("n", 0) == 0:
                st.warning("ไม่พบ signal ใน 2 ปีที่ผ่านมา (ลองเปลี่ยน Ticker)")
            else:
                wc = "#34f5a4" if res["win_rate"] >= 55 else "#ffc857" if res["win_rate"] >= 45 else "#ff3864"
                ac = "#34f5a4" if res["avg"] > 0 else "#ff3864"
                cards = '<div style="display:flex;gap:10px;flex-wrap:wrap;margin:12px 0;">'
                cards += info_card("Trades", str(res["n"]))
                cards += info_card("Win Rate", f'{res["win_rate"]}%', wc)
                cards += info_card("Avg Return/Trade", f'{res["avg"]}%', ac)
                cards += info_card("Best", f'+{res["best"]}%', "#34f5a4")
                cards += info_card("Worst", f'{res["worst"]}%', "#ff3864")
                cards += '</div>'
                st.markdown(cards, unsafe_allow_html=True)

                # ── เปรียบเทียบกับ Buy & Hold + risk metrics (ใหม่ v3.0) ──
                strat_ret = res.get("strategy_compound_ret", 0)
                bh_ret = res.get("buy_hold_ret", 0)
                beat = strat_ret > bh_ret
                cmp_color = "#34f5a4" if beat else "#ff3864"
                cards2 = '<div style="display:flex;gap:10px;flex-wrap:wrap;margin:4px 0 16px 0;">'
                cards2 += info_card("กลยุทธ์ (Compound)", f'{strat_ret:+.1f}%', cmp_color,
                                    "ผลรวมทุก trade ทบต้นต่อกัน")
                cards2 += info_card("Buy & Hold ช่วงเดียวกัน", f'{bh_ret:+.1f}%', "#5ee6ff")
                cards2 += info_card("Max Drawdown", f'{res.get("max_drawdown", 0)}%', "#ff3864",
                                    "จาก equity curve ของ trades")
                sharpe_v = res.get("sharpe")
                cards2 += info_card("Sharpe (ประมาณ)", f'{sharpe_v}' if sharpe_v is not None else "—", "#b66bff")
                cards2 += '</div>'
                st.markdown(cards2, unsafe_allow_html=True)

                verdict = "✅ กลยุทธ์ทำได้ดีกว่าถือเฉยๆ ในช่วงที่ทดสอบ" if beat else \
                          "⚠️ ถือเฉยๆ (Buy & Hold) ทำผลตอบแทนได้ดีกว่ากลยุทธ์นี้ในช่วงที่ทดสอบ"
                st.info(verdict)

                with st.expander("⚠️ ข้อจำกัดของ Backtest นี้ (อ่านก่อนเชื่อตัวเลข)"):
                    st.caption(res.get("notes", ""))

                trades = res["trades"]
                df_bt = pd.DataFrame({"Return %": trades})
                bins = [-100, -40, -20, -10, -5, 0, 5, 10, 20, 40, 200]
                df_bt["bucket"] = pd.cut(df_bt["Return %"], bins=bins)
                vc = df_bt["bucket"].value_counts().sort_index()
                vc = vc[vc > 0]

                bars = ""
                mx = max(vc.values) if len(vc) else 1
                for interval_b, cnt in vc.items():
                    pct = cnt / mx * 100
                    is_positive = interval_b.right > 0
                    col = "#34f5a4" if is_positive else "#ff3864"
                    label = f"{interval_b.left:.0f}% to {interval_b.right:.0f}%"
                    bars += (f'<div style="display:flex;align-items:center;gap:8px;margin:3px 0;">'
                             f'<div style="color:#5b7299;font-size:0.75rem;width:120px;text-align:right;">{label}</div>'
                             f'<div style="background:{col};height:18px;width:{pct:.0f}%;border-radius:3px;min-width:2px;"></div>'
                             f'<div style="color:#e8f0ff;font-size:0.78rem;">{cnt}</div></div>')
                chart_html = (f'<div style="background:#0e1626;border:1px solid #22344f;'
                              f'border-radius:10px;padding:16px 20px;">'
                              f'<div style="color:#5b7299;font-size:0.78rem;margin-bottom:10px;">'
                              f'การกระจาย Return หลัง {hold_d} วัน</div>{bars}</div>')
                components.html(chart_html, height=max(len(vc) * 28 + 60, 200))

                with st.expander("ดู trades ทั้งหมด (พร้อมวันที่เข้า-ออก)"):
                    details = res.get("trade_details", [])
                    if details:
                        tdf = pd.DataFrame(details)
                        tdf.insert(0, "Trade #", range(1, len(tdf) + 1))
                        tdf["Result"] = tdf["ret"].apply(lambda x: "✅ Win" if x > 0 else "❌ Loss")
                        tdf = tdf.rename(columns={"ret": "Return %", "entry_date": "Entry", "exit_date": "Exit"})
                        st.dataframe(make_table(*apply_thai_labels(tdf)), use_container_width=True)
                    else:
                        tdf = pd.DataFrame({"Trade #": range(1, len(trades) + 1), "Return %": trades})
                        tdf["Result"] = tdf["Return %"].apply(lambda x: "✅ Win" if x > 0 else "❌ Loss")
                        st.dataframe(make_table(*apply_thai_labels(tdf)), use_container_width=True)

        st.markdown("---")
        st.markdown("### 📊 Support Accuracy — แนวรับแม่นแค่ไหนจริงๆ")
        st.caption("ย้อนดูประวัติหุ้นตัวอย่าง 50 ตัว (ผสมหุ้นใหญ่+เล็ก/กลาง) 2 ปี หาทุกจุดที่เคยเข้าเงื่อนไข "
                  "แนวรับ แล้ววัดผลตอบแทนจริงใน 10/20 วันถัดไป — ใช้แทนการเชื่อ label เฉยๆ")

        run_sup_bt = st.button("🔬 วิเคราะห์ Support Accuracy", key="sup_bt_run")
        if run_sup_bt:
            with st.spinner("กำลังย้อนวิเคราะห์แนวรับของหุ้นตัวอย่าง 50 ตัว (อาจใช้เวลา 2-3 นาที)…"):
                sup_res = backtest_support_accuracy()
            st.session_state["sup_bt_res"] = sup_res

        if "sup_bt_res" in st.session_state:
            sup_res = st.session_state["sup_bt_res"]
            if "error" in sup_res:
                st.error(f"❌ {sup_res['error']}")
            else:
                st.caption(f"วิเคราะห์จากหุ้น {sup_res['n_tickers']} ตัว · พบจุดที่เข้าเงื่อนไขแนวรับ "
                          f"{sup_res.get('n_support_events', 0)} ครั้ง · "
                          f"Buy & Hold เฉลี่ยของกลุ่มตัวอย่างช่วงเดียวกัน: "
                          f"{sup_res['buy_hold_avg']:+.1f}%" if sup_res.get("buy_hold_avg") is not None else "")

                sup_table = sup_res.get("support_table", pd.DataFrame())
                if not sup_table.empty:
                    sup_smap = {"Support": _sty_support, "ผลตอบแทนเฉลี่ย 10วัน%": _sty_rs,
                               "ผลตอบแทนเฉลี่ย 20วัน%": _sty_rs, "Win Rate 10วัน%": _sty_wr,
                               "Win Rate 20วัน%": _sty_wr, "ความเชื่อมั่น": _sty_confidence}
                    sup_table_th, sup_smap_th = apply_thai_labels(sup_table, sup_smap)
                    st.dataframe(make_table(sup_table_th, sup_smap_th), use_container_width=True)
                    st.caption("ถ้า Win Rate 10/20 วันของ '🟢 อยู่ที่แนวรับ' สูงกว่า 50% และสูงกว่า Buy & Hold "
                              "เฉลี่ยด้านบนชัดเจน แปลว่าฟีเจอร์แนวรับมีหลักฐานสนับสนุนว่าใช้ได้จริง — ถ้าใกล้เคียง "
                              "หรือต่ำกว่า แปลว่ายังไม่ควรเชื่อมั่นมาก ควรใช้ร่วมกับการวิเคราะห์อื่นเสมอ")
                else:
                    st.info("ไม่พบข้อมูล Support ในช่วงทดสอบ — อาจเป็นเพราะหุ้นตัวอย่างไม่ค่อยมีจังหวะใกล้แนวรับในช่วงนี้")

                # v3.24 ข้อ 1: ตาราง breakdown แยกทีละปัจจัย (touch/volume/
                # confluence) — เดิมน้ำหนักคะแนนใน support_status() (touch×1.5,
                # volume 2, confluence 2) เป็นตัวเลขที่ตั้งเอง ไม่เคยพิสูจน์
                # ว่าปัจจัยไหนช่วยจริง ตารางนี้ให้ดูของจริงว่าปัจจัยไหน Win Rate
                # สูงกว่ากันชัดเจน จะได้รู้ว่าควรเชื่อปัจจัยไหนมากกว่ากัน
                factor_table = sup_res.get("factor_table", pd.DataFrame())
                if not factor_table.empty:
                    with st.expander("🔬 แยกทีละปัจจัย — อันไหนช่วยจริง (Touch/Volume/Confluence)", expanded=False):
                        st.caption("เทียบ Win Rate/ผลตอบแทนของแต่ละปัจจัยที่ใช้ให้คะแนน Support Quality "
                                  "แยกกัน — ถ้าคู่ไหน (เช่น 'มี Confluence' vs 'ไม่มี Confluence') ตัวเลขต่างกัน "
                                  "ชัดเจน แปลว่าปัจจัยนั้นมีผลจริง ถ้าใกล้เคียงกันมาก แปลว่าปัจจัยนั้นอาจไม่ค่อย "
                                  "สำคัญเท่าที่คิดไว้ตอนตั้งน้ำหนักคะแนน")
                        factor_smap = {"ผลตอบแทนเฉลี่ย 20วัน%": _sty_rs, "Win Rate 20วัน%": _sty_wr,
                                      "ความเชื่อมั่น": _sty_confidence}
                        st.dataframe(make_table(factor_table, factor_smap), use_container_width=True)

                with st.expander("⚠️ ข้อจำกัดของผลทดสอบนี้ (อ่านก่อนเชื่อตัวเลข)"):
                    st.caption(sup_res["notes"])

    # ════════════════════════════════════════════════════════
    # TAB 5: SECTOR MAP
    # ════════════════════════════════════════════════════════
    with tab5:
        st.markdown("### 🗺️ Sector Heatmap — Money Flow")
        st.caption("เฉลี่ยจากหุ้นทั้งหมดในแต่ละ Sector (16-20 ตัว/sector) เพื่อวัด momentum และ accumulation")

        # v3.8: ตัดปุ่ม "สแกนสด/Live Rescan" ออกตามที่ขอ — ข้อมูลมาจากรอบ
        # prefetch อัตโนมัติหลังตลาดปิดเพียงทางเดียวเท่านั้น (เหมือนข้อมูลหุ้น
        # หลักในแท็บอื่นๆ) ไม่มีการยิง Yahoo สดจาก tab นี้อีกต่อไป
        if "sec_df" not in st.session_state:
            gen_at, pre_sec_df = load_prefetched_sector_heatmap()
            st.session_state["sec_df"] = pre_sec_df
            st.session_state["sec_df_gen_at"] = gen_at

        sec_df = st.session_state.get("sec_df", pd.DataFrame())
        gen_at = st.session_state.get("sec_df_gen_at")

        if gen_at:
            st.caption(f"🕒 อัปเดตพร้อมกับสแกนหลักหลังตลาดปิดล่าสุด — {gen_at}")

        if sec_df is not None and not sec_df.empty:
            # v3.8: ลดคอลัมน์ที่แสดงตามที่ขอ — เหลือแค่ Gem Score / Accum / Bull %
            # (ตัด RS 20D และรายชื่อหุ้นตัวอย่างออกจากมุมมองหลัก เพราะไม่ใช่
            # ตัวเลขหลักที่ใช้ตัดสินใจว่า sector ไหน "สะสม" อยู่)
            st.markdown("**📊 Gem Score ต่อ Sector (ยิ่งสูง = สัญญาณสะสมมากกว่า)**")
            mx_gem = sec_df["Avg Gem Score"].max() or 1
            for _, row in sec_df.iterrows():
                g_val = row["Avg Gem Score"]; a_val = row["Avg Accum"]; bl_val = row["Bull %"]
                g_pct = g_val / mx_gem * 100 if mx_gem > 0 else 0
                g_col = "#ffd84d" if g_val >= 7 else "#34f5a4" if g_val >= 5 else "#2de2e6" if g_val >= 3 else "#5b7299"
                st.markdown(
                    f'<div style="background:#0e1626;border:1px solid #16213a;border-radius:8px;'
                    f'padding:10px 14px;margin:4px 0;display:flex;align-items:center;gap:12px;flex-wrap:wrap;">'
                    f'<div style="color:#ffffff;font-weight:700;width:110px;font-size:0.88rem;">{row["Sector"]}</div>'
                    f'<div style="flex:1;min-width:100px;">'
                    f'<div style="background:{g_col};height:14px;width:{g_pct:.0f}%;border-radius:3px;min-width:3px;"></div></div>'
                    f'<div style="color:{g_col};font-weight:700;width:50px;font-size:0.85rem;">{g_val:.1f}</div>'
                    f'<div style="color:#5b7299;font-size:0.78rem;">Accum:<b style="color:#2de2e6;"> {a_val:.1f}</b></div>'
                    f'<div style="color:#5b7299;font-size:0.78rem;">Bull:<b style="color:#34f5a4;"> {bl_val:.0f}%</b></div>'
                    f'</div>', unsafe_allow_html=True)

            st.markdown("---")
            st.dataframe(make_table(*apply_thai_labels(sec_df[["Sector", "Avg Gem Score", "Avg Accum", "Bull %"]])),
                         use_container_width=True)
        else:
            st.markdown("""
            <div style="text-align:center;padding:60px;color:#5b7299;">
                <div style="font-size:2.5rem;">🗺️</div>
                <h3 style="color:#93a8c9;">ยังไม่มี Sector Heatmap</h3>
                <p>ข้อมูลจะโผล่ขึ้นอัตโนมัติหลัง GitHub Action รันรอบแรก (ทุกวันหลังตลาดปิด) —
                ไม่ต้องกดอะไรเพิ่ม</p>
            </div>""", unsafe_allow_html=True)

    # ════════════════════════════════════════════════════════
    # TAB 6: WATCHLIST
    # ════════════════════════════════════════════════════════
    with tab6:
        st.markdown("### ⭐ Watchlist")
        st.caption("รายการหุ้นที่คุณเฝ้าดู — บันทึกถาวรบน disk ของแอป (อยู่ข้าม session/refresh ปกติ "
                   "แต่จะถูกล้างถ้า redeploy ใหม่จาก git push)")

        # v3.18: เตือนถ้า Watchlist ใหญ่เกินจนดูแลไม่ทั่ว (ข้อ 8) — นักลงทุน
        # มือใหม่มักถือ/เฝ้าหุ้นเยอะเกินจนบริหารความเสี่ยงจริงไม่ไหว เกณฑ์ 20
        # ตัวเป็นเลขกลมๆที่พอเฝ้าดูรายวันได้จริง ไม่ใช่ hard limit (ยังใช้งาน
        # ได้ปกติแค่เตือนเฉยๆ)
        if len(st.session_state.watchlist) > 20:
            st.warning(f"⚠️ มี {len(st.session_state.watchlist)} ตัวใน Watchlist — เยอะเกินกว่าจะเฝ้าดูได้ทั่วถึงทุกวันไหม? "
                      "ลองพิจารณาตัดตัวที่ไม่ได้ติดตามจริงจังออก จะได้โฟกัสตัวที่สำคัญจริงๆ")

        wc1, wc2, wc3 = st.columns([3, 1, 1])
        with wc1:
            new_tk = st.text_input("ชื่อหุ้น", placeholder="เช่น AAPL หรือ PTT.BK", key="wl_new")
        with wc2:
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("➕ เพิ่ม", key="wl_add2") and new_tk.strip():
                tk = new_tk.strip().upper()
                if tk not in st.session_state.watchlist:
                    st.session_state.watchlist.append(tk)
                    save_watchlist(st.session_state.watchlist)
                    # v3.13: เจอระหว่าง self-audit — เดิมผลสแกน/warning ค้างเก่า
                    # (wl_df/wl_dropped) อยู่ต่อจนกว่าจะกด Scan All ใหม่ ทำให้
                    # ตารางไม่รวมหุ้นที่เพิ่งเพิ่ม และ warning อาจโชว์ชื่อหุ้นที่
                    # ลบไปแล้วค้างอยู่ — เคลียร์ทิ้งทันทีที่ list เปลี่ยน
                    st.session_state.pop("wl_df", None)
                    st.session_state.pop("wl_dropped", None)
        with wc3:
            st.markdown("<br>", unsafe_allow_html=True)
            rem_tk = st.selectbox("ลบออก | Remove", ["—"] + st.session_state.watchlist, key="wl_rem")
            if rem_tk != "—":
                if st.button("🗑️ ลบ", key="wl_del"):
                    st.session_state.watchlist.remove(rem_tk)
                    save_watchlist(st.session_state.watchlist)
                    st.session_state.pop("wl_df", None)
                    st.session_state.pop("wl_dropped", None)
                    st.rerun()

        if not st.session_state.watchlist:
            st.info("ยังไม่มีหุ้นใน Watchlist — เพิ่มจากตารางด้านบนหรือพิมพ์ชื่อหุ้นเข้ามา")
        else:
            st.markdown(f"**{len(st.session_state.watchlist)} หุ้น** — "
                        f"{', '.join(st.session_state.watchlist)}")
            st.markdown("---")

            scan_wl = st.button("🔄 Scan Watchlist ทั้งหมด | Scan All", key="wl_scan")
            if scan_wl:
                with st.spinner("กำลังวิเคราะห์ Watchlist…"):
                    _, bundle_df_wl, _ = load_prefetched_bundle()
                    wl_df_result, wl_dropped = get_with_bundle_fallback(
                        st.session_state.watchlist, bundle_df_wl, max_live_fallback=50)
                    st.session_state["wl_df"] = wl_df_result
                    st.session_state["wl_dropped"] = wl_dropped

            # v3.12: Watchlist เป็น list ที่คนพิมพ์เองทีละตัว ต่างจาก Universe
            # ใหญ่ๆ ตรงที่ทุกตัวที่หายไปมีความหมาย (พิมพ์ผิด/delisted/ชื่อไม่ตรง
            # ตลาด) เดิมหายไปแบบเงียบๆ ไม่รู้ตัวเลยว่าทำไมตารางน้อยกว่ารายชื่อ
            # ที่มี ตอนนี้เตือนชัดเจนกว่า Dashboard หลัก (ใช้ warning ไม่ใช่ caption)
            if st.session_state.get("wl_dropped"):
                st.warning(f"⚠️ หาไม่เจอ {len(st.session_state['wl_dropped'])} ตัว: "
                          f"{', '.join(st.session_state['wl_dropped'])} — เช็คว่าพิมพ์ชื่อถูกไหม "
                          f"(เช่นหุ้นไทยต้องมี .BK ต่อท้าย เช่น PTT.BK) หรือหุ้นอาจ delisted ไปแล้ว")

            if "wl_df" in st.session_state and not st.session_state["wl_df"].empty:
                wdf = st.session_state["wl_df"]
                if mobile_mode:
                    wl_show = [c for c in ["Ticker", "Price", "Support", "Support Zone"] if c in wdf.columns]
                elif simple_mode:
                    wl_show = [c for c in ["Ticker", "Price", "Trend", "RSI", "💎 Gem", "Accum"]
                              if c in wdf.columns]
                else:
                    wl_show = [c for c in ["Ticker", "Price", "Trend", "Weekly Trend", "RSI", "EMA Pattern", "Squeeze",
                                           "Trend Age", "💎 Gem", "Accum", "RS 20D",
                                           "YTD%", "Drawdown%"]
                               if c in wdf.columns]
                wdf = wdf.copy()
                if "Trend Age" in wdf.columns:
                    wdf["Trend Age"] = wdf["Trend Age"].apply(
                        lambda x: f"{int(x)}d ago" if isinstance(x, (int, float)) and x >= 0 else "—")
                wsmap = {"💎 Gem": _sty_gem, "RSI": _sty_rsi,
                         "Squeeze": _sty_squeeze, "RS 20D": _sty_rs, "Accum": _sty_generic,
                         "Weekly Trend": _sty_weekly}
                wdf_th, wsmap_th = apply_thai_labels(wdf[wl_show], wsmap)
                st.dataframe(make_table(wdf_th, wsmap_th),
                             use_container_width=True, height=400)

                with st.expander("📈 Backtest ทุกตัวใน Watchlist"):
                    bt_rows = []
                    for tk in st.session_state.watchlist:
                        with st.spinner(f"Backtest {tk}…"):
                            r = backtest(tk)
                        if "error" not in r and r.get("n", 0) > 0:
                            bt_rows.append({"Ticker": tk, "Trades": r["n"],
                                "Win%": r["win_rate"], "Avg Ret%": r["avg"],
                                "Best%": r["best"], "Worst%": r["worst"],
                                "vs Buy&Hold%": round(r.get("strategy_compound_ret", 0) - r.get("buy_hold_ret", 0), 2)})
                    if bt_rows:
                        bt_df = pd.DataFrame(bt_rows)
                        bt_df_th, bt_smap_th = apply_thai_labels(
                            bt_df, {"Win%": _sty_wr, "Avg Ret%": _sty_rs, "vs Buy&Hold%": _sty_rs})
                        st.dataframe(make_table(bt_df_th, bt_smap_th),
                                     use_container_width=True)


if __name__ == "__main__":
    main()
