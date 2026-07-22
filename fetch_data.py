"""
สคริปต์นี้ไม่ได้ถูกเรียกจากแอป Streamlit ตรงๆ — เป็นตัวที่ GitHub Actions
รันตามเวลา (ทุกวันหลังตลาดปิด ดู .github/workflows/prefetch.yml) เพื่อดึงข้อมูลหุ้น
ทั้งหมดล่วงหน้า แล้วเซฟเป็นไฟล์ data/latest_scan.json ให้แอป Streamlit
อ่านตรงๆ แทนการไปยิง Yahoo Finance สดตอนมีคนเข้าดูหน้าเว็บ

ทำไมต้องแยกเป็นไฟล์นี้ ไม่ยัดเข้า app.py:
  - app.py ต้องรันใน Streamlit runtime (ใช้ st.session_state, ปุ่ม, sidebar ฯลฯ)
  - ตัวนี้แค่ "import app" มาดึงฟังก์ชันการสแกน/วิเคราะห์มาใช้ตรงๆ
    (กัน logic เพี้ยน/ซ้ำซ้อนกันระหว่าง 2 ที่) แล้วรันแบบ headless ไม่มีหน้าเว็บ

วิธีรันด้วยตัวเอง (ทดสอบ): python fetch_data.py
"""
import datetime
import json
import os
import time

import numpy as np
import pandas as pd

import app  # ดึง analyze / batch_scan / resolve_tickers / UNIVERSE_OPTIONS / SECTOR_MAP
            # / make_bench_tuple มาใช้ตรงจาก app.py (ไม่ duplicate logic)
import yfinance as yf

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUT_PATH = os.path.join(BASE_DIR, "data", "latest_scan.json")
# v3.7: คำนวณ Sector Heatmap ไว้ล่วงหน้าที่นี่เลย (ต่อจาก df ที่สแกนเสร็จ
# อยู่แล้วในรอบนี้ — ticker ของทุก sector ถูกรวมเข้า all_tickers ไปแล้ว
# ไม่ต้องยิง Yahoo เพิ่มอีกรอบ) แทนที่จะให้แอปต้องรอคนกดปุ่มคำนวณสดเอง
SECTOR_HEATMAP_PATH = os.path.join(BASE_DIR, "data", "sector_heatmap.json")

# v3.5: GitHub Actions ตั้ง GITHUB_REPOSITORY ให้อัตโนมัติเป็น "owner/repo"
# ใช้ดึง URL ของ Release "latest-data" รอบก่อนหน้า (ก่อนรอบนี้จะเขียนทับ)
# เพราะตอนนี้ไม่ commit ไฟล์เข้า git แล้ว (ย้ายไปเก็บที่ Release กัน repo บวม)
# ทุกรอบที่ Action รันใหม่จะ checkout repo สดๆไม่มีไฟล์เก่าเหลือในเครื่องอีก
GITHUB_REPO_ENV = os.environ.get("GITHUB_REPOSITORY")
RELEASE_TAG = "latest-data"

# Universe ที่จะดึงล่วงหน้าให้ทั้งหมด (ไม่รวม Custom Tickers / Sector Focus
# เพราะ Sector Focus ใช้ ticker ที่อยู่ใน SECTOR_MAP ซึ่งรวมไว้แยกด้านล่างแล้ว)
#
# v3.20: ย้อนกลับ v3.17 ตามที่ตัดสินใจ (ข้อมูลพื้นฐานหุ้นเล็ก/ไมโครแคปจาก
# yfinance ไม่ครบเป็นเรื่องปกติ เสี่ยงคัด Top 100 ผิด) — "Russell 2000 Small
# Cap"/"US Broad Market (~700)" กลับมาเป็น universe ตรงๆ ผ่าน resolve_tickers()
# เหมือนเดิมก่อน v3.17 ไม่ต้องเรียก app.fetch_russell2000()/fetch_broad_us()
# แยกต่างหากอีกต่อไป
# v3.50: ตัดให้เหลือแค่ 2 universe ตรงๆ (S&P500/Nasdaq100) ตามที่ app.py
# ตัด UNIVERSE_OPTIONS เหลือแค่ 3 ตัวเลือก (S&P500/Nasdaq100/Sector Focus)
# — Sector Focus ไม่ต้องอยู่ในลิสต์นี้เพราะ ticker ของทุก sector ถูกรวมเข้า
# all_tickers แยกด้านล่างอยู่แล้ว (ดู SECTOR_MAP.values() loop) ตัด Russell
# 2000/US Broad Market/หุ้นไทย SET/mai/ETF Screener ออกทั้งหมดเพราะเว็บไม่มี
# ตัวเลือกเหล่านี้ให้เลือกแล้ว (ดึงมาก็ไม่มีใครใช้ เปลืองเวลาสแกนเปล่าๆ)
PREFETCH_UNIVERSES = [
    "S&P 500 (503)",
    "Nasdaq 100 (101)",
]


def compute_sector_heatmap(df: pd.DataFrame) -> list:
    """
    v3.7: คำนวณ Sector Heatmap ต่อจาก df ที่สแกนเสร็จแล้วในรอบ prefetch นี้
    เลย (เหมือน logic เดิมใน app.sector_heatmap_data_live() แต่สร้างจาก df
    ที่มีอยู่แล้วในมือ ไม่เรียก analyze() ซ้ำ/ไม่ยิง Yahoo เพิ่มอีกรอบ)
    คืนค่าเป็น list of dict พร้อมเซฟลง JSON ให้แอปอ่านตรงๆ

    v3.12: เดิมใช้แค่ tickers[:5] (5 ตัวแรกที่พิมพ์ไว้ใน SECTOR_MAP) เป็น
    "ตัวแทน" ของทั้ง sector ทั้งที่แต่ละ sector มี 16-20 ตัว — ทำให้ Avg
    Gem/Accum/Bull % สะท้อนแค่บริษัทใหญ่ๆไม่กี่ตัวหัวแถว ไม่ใช่ภาพรวมจริงของ
    sector ตอนนี้ใช้ ticker ทั้งหมดใน sector แทน เพราะ df ที่ได้มามีข้อมูล
    ครบทุกตัวอยู่แล้ว (SECTOR_MAP ถูกรวมเข้า all_tickers ตั้งแต่ต้นไฟล์นี้แล้ว)
    ไม่มีต้นทุนเพิ่มขึ้นเลยจากการใช้ทั้งหมดแทนแค่ 5 ตัว
    """
    rows = []
    if df is None or df.empty or "Ticker" not in df.columns:
        return rows
    for sector, tickers in app.SECTOR_MAP.items():
        sub = df[df["Ticker"].isin(tickers)]
        if sub.empty:
            continue
        scores = [{
            "gem": r.get("Gem Score", 0) or 0,
            "accum": r.get("Accum Score", 0) or 0,
            "rs20": r.get("RS 20D", 0) or 0,
            "bull": 1 if "Bull" in str(r.get("Trend", "")) else 0,
        } for _, r in sub.iterrows()]
        rows.append({
            "Sector": sector,
            "Avg Gem Score": round(float(np.mean([s["gem"] for s in scores])), 1),
            "Avg Accum": round(float(np.mean([s["accum"] for s in scores])), 1),
            "Avg RS 20D": round(float(np.mean([s["rs20"] for s in scores])), 1),
            "Bull %": round(float(np.mean([s["bull"] for s in scores])) * 100, 0),
            "Coverage": f"{len(sub)}/{len(tickers)}",
        })
    rows.sort(key=lambda r: r["Avg Gem Score"], reverse=True)
    return rows


def main():
    print(f"[{datetime.datetime.now().isoformat()}] เริ่มดึงข้อมูลล่วงหน้า...")

    print("รวบรวมรายชื่อหุ้นทั้งหมดจากทุก universe + sector...")
    all_tickers = set()
    for u in PREFETCH_UNIVERSES:
        try:
            ts = app.resolve_tickers(u, [], "")
            all_tickers.update(ts)
            print(f"  {u}: {len(ts)} ตัว")
        except Exception as e:
            print(f"  {u}: ดึงรายชื่อไม่สำเร็จ ({e}) — ข้าม universe นี้รอบนี้")
    for sector_tickers in app.SECTOR_MAP.values():
        all_tickers.update(sector_tickers)
    all_tickers = sorted(all_tickers)
    print(f"รวมหุ้น unique ทั้งหมดที่ต้องดึง: {len(all_tickers)} ตัว")

    bench_tuple = None
    try:
        spy_df = yf.Ticker("SPY").history(period="1y", interval="1d", auto_adjust=True)
        bench_tuple = app.make_bench_tuple(spy_df)
        print("ดึง SPY benchmark สำเร็จ")
    except Exception as e:
        print(f"ดึง SPY benchmark ไม่สำเร็จ ({e}) — จะสแกนต่อโดยไม่มี Relative Strength")

    # v3.3: เดิมยิงทั้งหมดทีเดียวด้วย max_workers=8 — จาก log การรันจริงพบว่า
    # Yahoo เริ่ม Rate-limit (YFRateLimitError) หลังยิงต่อเนื่องสักพัก ทำให้
    # หุ้นท้ายๆของ universe ใหญ่หลุดไปจำนวนมาก ตอนนี้แบ่งเป็น chunk เล็กลง +
    # ใช้ concurrency ต่อ chunk ต่ำลง + พักระหว่าง chunk ให้ Yahoo "หายใจ"
    # ช้าลงแต่ได้ข้อมูลครบกว่าเดิมมาก — งานนี้ไม่มีคนรอ ไม่ต้องรีบ
    CHUNK_SIZE = 60
    PAUSE_BETWEEN_CHUNKS = 5  # วินาที

    all_dfs = []
    total = len(all_tickers)
    for i in range(0, total, CHUNK_SIZE):
        chunk = all_tickers[i:i + CHUNK_SIZE]
        try:
            chunk_df = app.batch_scan(tuple(chunk), "1y", "1d", bench_tuple, max_workers=4)
            all_dfs.append(chunk_df)
            done = min(i + CHUNK_SIZE, total)
            print(f"  ...สแกนแล้ว {done}/{total} (chunk นี้ได้ {len(chunk_df)}/{len(chunk)} ตัว)")
        except Exception as e:
            # v3.5: เดิมถ้า chunk ไหนพังกลางทาง (error ระดับ ThreadPoolExecutor
            # เอง ไม่ใช่ error รายตัวหุ้นที่ analyze() ดักไว้แล้ว) สคริปต์ทั้งตัว
            # จะพังทันที เสียผลของ chunk ก่อนหน้าที่ทำสำเร็จไปแล้วทั้งหมดไปด้วย
            # ตอนนี้ดักไว้ ข้ามไป chunk ถัดไปแทน ไม่ให้ความพยายามที่ทำมาหายเปล่า
            done = min(i + CHUNK_SIZE, total)
            print(f"  ⚠️ chunk {i}-{done} พังกลางทาง ({type(e).__name__}: {e}) — ข้ามไป chunk ถัดไป")
        if done < total:
            time.sleep(PAUSE_BETWEEN_CHUNKS)

    df = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()

    # v3.5: Data validation ชั้นที่ 2 — analyze() กรองราคา ≤0/NaN ออกไปแล้ว
    # ชั้นหนึ่ง แต่เช็คซ้ำตรงนี้อีกรอบเผื่อมีช่องโหว่ทางอื่นหลุดเข้ามา (กันไว้)
    if not df.empty and "Price" in df.columns:
        before = len(df)
        df = df[df["Price"].notna() & (df["Price"] > 0)]
        if before != len(df):
            print(f"  ตัดทิ้ง {before - len(df)} แถวที่ราคาผิดปกติ (≤0 หรือไม่มีค่า)")

    print(f"สแกนสำเร็จ {len(df)} / {len(all_tickers)} ตัว (ที่เหลือคือดึงไม่สำเร็จ/delisted/rate-limit ชั่วคราว — รอบหน้าจะลองใหม่)")

    if df.empty:
        print("ผลลัพธ์ว่างเปล่าทั้งหมด — ไม่บันทึกทับไฟล์เดิม (กันข้อมูลเก่าหายเปล่าๆ)")
        return

    generated_at = datetime.datetime.now(datetime.timezone.utc).isoformat()

    # v3.12: stamp เลข version ของโค้ด (app.APP_VERSION) ลงไปในทุกไฟล์ที่เซฟ
    # เหตุผล: ถ้า logic การคำนวณแนวรับเปลี่ยนกลางทาง (เช่นรอบนี้ที่เปลี่ยน
    # แนวรับเป็นรายสัปดาห์ + แก้บั๊กตัดข้อมูลตามตัวอักษร) ข้อมูลเก่ากับใหม่จะ
    # เทียบกันตรงๆไม่ได้ — มี app_version ติดไปด้วยเสมอ ทำให้ตอนวิเคราะห์
    # ย้อนหลัง (forward-test) กรองแยก "ก่อน/หลัง" การเปลี่ยนแปลงได้อัตโนมัติ
    # ไม่ต้องจำเองว่าห้ามเอาผลก่อนวันที่เท่าไหร่มาเทียบ
    app_version = getattr(app, "APP_VERSION", "unknown")

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump({"generated_at": generated_at, "app_version": app_version,
                   "data": df.to_dict(orient="records")},
                   f, default=str, ensure_ascii=False)
    print(f"บันทึก {OUT_PATH} ({os.path.getsize(OUT_PATH) / 1024:.0f} KB) [app_version={app_version}]")

    # v3.7: คำนวณ + เซฟ Sector Heatmap พร้อมกันในรอบเดียวกัน (ต่อจาก df ที่มี
    # อยู่แล้ว ไม่ยิง Yahoo เพิ่ม) แอปจะโหลดไฟล์นี้แสดงอัตโนมัติโดยไม่ต้องกดปุ่ม
    try:
        sector_rows = compute_sector_heatmap(df)
        with open(SECTOR_HEATMAP_PATH, "w", encoding="utf-8") as f:
            json.dump({"generated_at": generated_at, "app_version": app_version,
                       "data": sector_rows}, f, default=str, ensure_ascii=False)
        print(f"บันทึก {SECTOR_HEATMAP_PATH} ({len(sector_rows)} sectors)")
    except Exception as e:
        print(f"คำนวณ/บันทึก Sector Heatmap ไม่สำเร็จ ({e}) — ข้าม (ไม่กระทบข้อมูลหลักด้านบน)")

    # v3.5: เก็บ snapshot รายวันแยกไฟล์ (ลงวันที่ในชื่อไฟล์) ไว้ย้อนดูภายหลัง
    # ว่าระบบแม่นแค่ไหนจริง — ไฟล์นี้จะถูก GitHub Action เอาไปสร้างเป็น release
    # แยกต่างหาก (ดู prefetch.yml) แล้วลบ release ที่เก่ากว่า 90 วันออกอัตโนมัติ
    # กันไม่ให้สะสมไม่จำกัด (ขัดกับเป้าหมายที่ลด storage bloat)
    snapshot_path = os.path.join(BASE_DIR, "data", f"snapshot_{datetime.date.today().isoformat()}.json")
    with open(snapshot_path, "w", encoding="utf-8") as f:
        json.dump({"generated_at": generated_at, "app_version": app_version,
                   "data": df.to_dict(orient="records")},
                   f, default=str, ensure_ascii=False)
    print(f"บันทึก snapshot: {snapshot_path}")

    print("เสร็จสิ้น")


if __name__ == "__main__":
    main()
