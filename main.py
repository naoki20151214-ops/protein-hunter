import os
import json
import math
import time
import base64
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, List, Tuple, Any, Optional

import requests
import gspread


# =========================
# Config (GitHub Secrets / Env)
# =========================
RAKUTEN_APP_ID = os.environ.get("RAKUTEN_APP_ID", "").strip()
RAKUTEN_ACCESS_KEY = os.environ.get("RAKUTEN_ACCESS_KEY", "").strip()
RAKUTEN_AFFILIATE_ID = os.environ.get("RAKUTEN_AFFILIATE_ID", "").strip()

SHEET_ID = os.environ.get("SHEET_ID", "").strip()

# Recommended: store Base64 of the service account JSON in GitHub Secrets
GSPREAD_SERVICE_ACCOUNT_JSON_B64 = os.environ.get("GSPREAD_SERVICE_ACCOUNT_JSON_B64", "").strip()

DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()

# Rakuten postageFlag (official): 0 = shipping included, 1 = shipping NOT included 
DEFAULT_SHIPPING_YEN = int(os.environ.get("DEFAULT_SHIPPING_YEN", "800"))

# Fetch more than we store, to avoid missing effective cheapest offers
FETCH_HITS = int(os.environ.get("FETCH_HITS", "100"))     # total offers fetched per canonical_id
STORE_HITS = int(os.environ.get("STORE_HITS", "20"))      # offers stored per canonical_id

REQUEST_SLEEP_SEC = float(os.environ.get("REQUEST_SLEEP_SEC", "1.0"))

# Optional extra point boost (Phase2). Example: 0.02 for +2%
EXTRA_POINT_RATE = float(os.environ.get("EXTRA_POINT_RATE", "0.0"))  # 0.0..1.0

# Filtering
EXCLUDE_KEYWORDS = [k.strip() for k in os.environ.get(
    "EXCLUDE_KEYWORDS",
    # Stronger default list (safe-side). Extend anytime.
    "シェイカー,シェーカー,ボトル,スプーン,計量スプーン,ミキサー,ブレンダー,"
    "お試し,試供品,サンプル,トライアル,小分け,個包装,少量,ミニ,"
    "訳あり,中古,アウトレット,福袋,セット,詰め合わせ,バラエティ,"
    "プロテインバー,バー,クッキー,チョコ,シリアル,グラノーラ,"
    "ゲイナー,増量,マスゲイナー,"
    "BCAA,EAA,クレアチン,アミノ酸,"
    "シェイク,ドリンク,飲料,缶,紙パック"
).split(",") if k.strip()]

# Capacity strict match is REQUIRED per your final spec
STRICT_CAPACITY_MATCH = True

# Rakuten endpoint (Ichiba Item Search)
RAKUTEN_ENDPOINT = "https://openapi.rakuten.co.jp/ichibams/api/IchibaItem/Search/20220601"


# =========================
# Data models
# =========================
@dataclass
class MasterItem:
    canonical_id: str
    search_keyword: str
    brand: str
    capacity_kg: float
    protein_ratio: float  # 0.70 for 70% etc


@dataclass
class OfferRow:
    date: str
    canonical_id: str
    item_code: str
    shop_name: str
    raw_price: int
    shipping_cost: int
    point_rate: float
    protein_cost: float
    item_url: str
    item_name: str


# =========================
# Helpers
# =========================
def jst_date() -> datetime.date:
    return datetime.now(ZoneInfo("Asia/Tokyo")).date()

def jst_today_str() -> str:
    return jst_date().isoformat()

def jst_now_iso() -> str:
    return datetime.now(ZoneInfo("Asia/Tokyo")).isoformat(timespec="seconds")

def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default

def safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return default

def discord_notify(title: str, lines: List[str]) -> None:
    if not DISCORD_WEBHOOK_URL:
        return
    content = f"**{title}**\n" + "\n".join(lines)
    content = content[:1800]
    requests.post(DISCORD_WEBHOOK_URL, json={"content": content}, timeout=20)


# =========================
# Google Sheets
# =========================
def load_service_account_dict_b64() -> dict:
    if not (SHEET_ID and GSPREAD_SERVICE_ACCOUNT_JSON_B64):
        raise RuntimeError("Missing SHEET_ID or GSPREAD_SERVICE_ACCOUNT_JSON_B64")
    raw = base64.b64decode(GSPREAD_SERVICE_ACCOUNT_JSON_B64.encode("utf-8")).decode("utf-8")
    return json.loads(raw)

def open_sheets():
    creds_dict = load_service_account_dict_b64()
    gc = gspread.service_account_from_dict(creds_dict)
    sh = gc.open_by_key(SHEET_ID)

    master_ws = sh.worksheet("Master_List")
    hist_ws = sh.worksheet("Price_History")

    # Min_Summary worksheet (create if missing)
    try:
        min_ws = sh.worksheet("Min_Summary")
    except gspread.exceptions.WorksheetNotFound:
        min_ws = sh.add_worksheet(title="Min_Summary", rows=2000, cols=10)
        min_ws.append_row(
            ["date", "canonical_id", "min_cost", "min_shop", "min_url", "updated_at"],
            value_input_option="RAW",
        )

    return master_ws, hist_ws, min_ws

def read_master(master_ws) -> List[MasterItem]:
    rows = master_ws.get_all_records()
    items: List[MasterItem] = []
    for r in rows:
        cid = str(r.get("canonical_id", "")).strip()
        kw = str(r.get("search_keyword", "")).strip()
        if not cid or not kw:
            continue
        items.append(
            MasterItem(
                canonical_id=cid,
                search_keyword=kw,
                brand=str(r.get("brand", "")).strip(),
                capacity_kg=safe_float(r.get("capacity_kg", 0)),
                protein_ratio=safe_float(r.get("protein_ratio", 0)),
            )
        )
    return items

def ensure_history_headers(hist_ws) -> None:
    existing = hist_ws.get_all_values()
    if existing:
        return
    hist_ws.append_row(
        [
            "date",
            "canonical_id",
            "item_code",
            "shop_name",
            "raw_price",
            "shipping_cost",
            "point_rate",
            "protein_cost",
            "item_url",
            "item_name",
        ],
        value_input_option="RAW",
    )

def append_history(hist_ws, offer_rows: List[OfferRow]) -> None:
    if not offer_rows:
        return
    ensure_history_headers(hist_ws)
    values = [
        [
            o.date,
            o.canonical_id,
            o.item_code,
            o.shop_name,
            o.raw_price,
            o.shipping_cost,
            round(o.point_rate, 6),
            round(o.protein_cost, 6),
            o.item_url,
            o.item_name,
        ]
        for o in offer_rows
    ]
    hist_ws.append_rows(values, value_input_option="RAW")

def read_min_summary(min_ws, target_date: str) -> Dict[str, Tuple[float, str, str]]:
    rows = min_ws.get_all_records()
    out: Dict[str, Tuple[float, str, str]] = {}
    for r in rows:
        if str(r.get("date", "")).strip() != target_date:
            continue
        cid = str(r.get("canonical_id", "")).strip()
        if not cid:
            continue
        out[cid] = (
            safe_float(r.get("min_cost", math.inf), math.inf),
            str(r.get("min_shop", "")).strip(),
            str(r.get("min_url", "")).strip(),
        )
    return out

def read_alltime_min(min_ws) -> Dict[str, Tuple[float, str, str]]:
    rows = min_ws.get_all_records()
    out: Dict[str, Tuple[float, str, str]] = {}
    for r in rows:
        cid = str(r.get("canonical_id", "")).strip()
        if not cid:
            continue
        cost = safe_float(r.get("min_cost", math.inf), math.inf)
        shop = str(r.get("min_shop", "")).strip()
        url = str(r.get("min_url", "")).strip()
        prev = out.get(cid)
        if prev is None or cost < prev[0]:
            out[cid] = (cost, shop, url)
    return out

def upsert_today_min(min_ws, date: str, cid: str, min_cost: float, min_shop: str, min_url: str) -> None:
    """
    Upsert by (date, canonical_id).
    Uses a simple scan; Min_Summary is small (20 items/day), so stays fast.
    """
    values = min_ws.get_all_values()
    target_row = None

    # header row = 1
    for row_idx in range(2, len(values) + 1):
        row = values[row_idx - 1]
        if len(row) >= 2 and row[0] == date and row[1] == cid:
            target_row = row_idx
            break

    updated_at = jst_now_iso()
    if target_row:
        min_ws.update(f"C{target_row}:F{target_row}", [[str(min_cost), min_shop, min_url, updated_at]])
    else:
        min_ws.append_row([date, cid, str(min_cost), min_shop, min_url, updated_at], value_input_option="RAW")


# =========================
# Rakuten API
# =========================
def rakuten_search_page(keyword: str, page: int, hits: int) -> List[Dict[str, Any]]:
    """
    Uses formatVersion=2 so items are returned as flat dicts.
    NOTE: hits max is 30 per page.
    """
    if not RAKUTEN_APP_ID:
        raise RuntimeError("Missing RAKUTEN_APP_ID")

    params = {
        "applicationId": RAKUTEN_APP_ID,
        "accessKey": RAKUTEN_ACCESS_KEY,  # ← 追加（必須）
        "keyword": keyword,
        "hits": max(1, min(30, hits)),
        "page": page,
        "sort": "+itemPrice",   # raw price sort; we re-sort by effective price later
        "format": "json",
        "formatVersion": 2,
        "elements": ",".join(
            [
                "itemCode",
                "itemName",
                "itemPrice",
                "itemUrl",
                "shopName",
                "postageFlag",
                "pointRate",
            ]
        ),
    }
    if RAKUTEN_AFFILIATE_ID:
        params["affiliateId"] = RAKUTEN_AFFILIATE_ID

    resp = requests.get(RAKUTEN_ENDPOINT, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data.get("items") or []

def rakuten_search_multi_pages(keyword: str, total_hits: int) -> List[Dict[str, Any]]:
    """
    Fetch up to total_hits by paging (max 30 per page).
    """
    all_items: List[Dict[str, Any]] = []
    remaining = total_hits
    page = 1

    while remaining > 0:
        hits = min(30, remaining)
        items = rakuten_search_page(keyword, page=page, hits=hits)
        if not items:
            break
        all_items.extend(items)
        remaining -= len(items)

        # If API returned fewer than requested, likely no more pages
        if len(items) < hits:
            break

        page += 1
        if page > 10:  # safety
            break

        # be gentle
        time.sleep(0.3)

    return all_items


# =========================
# Filtering / Compute
# =========================
def looks_like_garbage(item_name: str) -> bool:
    name = item_name or ""
    return any(k in name for k in EXCLUDE_KEYWORDS)

def capacity_strict_match(master: MasterItem, item_name: str) -> bool:
    """
    Strict capacity match per final spec.
    Rule:
      - If master.capacity_kg >= 1.0 -> require "<N>kg" token in itemName (N rounded to int if .0)
      - If 0 < master.capacity_kg < 1.0 -> require "<grams>g" token
    This is intentionally strict to reduce mismatches.
    """
    if not STRICT_CAPACITY_MATCH:
        return True
    if master.capacity_kg <= 0:
        return True

    name = item_name or ""
    kg = master.capacity_kg

    if kg >= 1.0:
        # support 2.5kg etc
        if abs(kg - round(kg)) < 1e-9:
            token = f"{int(round(kg))}kg"
            return token in name
        else:
            # 2.5kg token
            token = f"{kg:g}kg"
            return token in name

    grams = int(round(kg * 1000))
    token_g = f"{grams}g"
    return token_g in name

def compute_offer(master: MasterItem, item: Dict[str, Any]) -> Optional[OfferRow]:
    date = jst_today_str()
    item_code = str(item.get("itemCode", "")).strip()
    shop_name = str(item.get("shopName", "")).strip()
    item_url = str(item.get("itemUrl", "")).strip()
    item_name = str(item.get("itemName", "")).strip()

    raw_price = safe_int(item.get("itemPrice", 0), 0)
    if not item_code or not shop_name or raw_price <= 0:
        return None

    # Garbage filtering
    if looks_like_garbage(item_name):
        return None
    if not capacity_strict_match(master, item_name):
        return None

    # postageFlag: 0=shipping included, 1=shipping NOT included (add DEFAULT_SHIPPING_YEN) 
    postage_flag = safe_int(item.get("postageFlag", 0), 0)
    shipping = DEFAULT_SHIPPING_YEN if postage_flag == 1 else 0

    # pointRate is percent (e.g. 2 -> 2%). Not all campaigns are reflected; Phase1 uses what API returns.
    point_rate_percent = safe_float(item.get("pointRate", 0.0), 0.0)
    point_rate = max(0.0, min(1.0, point_rate_percent / 100.0))
    point_rate = max(0.0, min(1.0, point_rate + EXTRA_POINT_RATE))

    denom = master.capacity_kg * master.protein_ratio
    if denom <= 0:
        return None

    protein_cost = ((raw_price + shipping) * (1.0 - point_rate)) / denom

    return OfferRow(
        date=date,
        canonical_id=master.canonical_id,
        item_code=item_code,
        shop_name=shop_name,
        raw_price=raw_price,
        shipping_cost=shipping,
        point_rate=point_rate,
        protein_cost=protein_cost,
        item_url=item_url,
        item_name=item_name,
    )


# =========================
# Main
# =========================
def main():
    print("APP_ID:", os.environ.get("RAKUTEN_APP_ID", "")[:6], "len=", len(os.environ.get("RAKUTEN_APP_ID","")))
    print("ENDPOINT:", RAKUTEN_ENDPOINT)
    today = jst_today_str()
    yesterday = (jst_date() - timedelta(days=1)).isoformat()

    master_ws, hist_ws, min_ws = open_sheets()
    masters = read_master(master_ws)
    if not masters:
        raise RuntimeError("Master_List is empty or missing required columns.")

    # Read minima from Min_Summary only (fast)
    yday_min = read_min_summary(min_ws, yesterday)   # {cid: (cost, shop, url)}
    alltime_min = read_alltime_min(min_ws)          # {cid: (cost, shop, url)}

    all_offers: List[OfferRow] = []
    notify_payloads: List[Tuple[str, List[str]]] = []

    for m in masters:
        time.sleep(REQUEST_SLEEP_SEC)

        # Fetch many, then compute effective cost and keep best STORE_HITS
        items = rakuten_search_multi_pages(m.search_keyword, total_hits=FETCH_HITS)

        seen = set()  # (date,cid,item_code,shop_name)
        offers_for_this: List[OfferRow] = []

        for it in items:
            offer = compute_offer(m, it)
            if not offer:
                continue
            key = (offer.date, offer.canonical_id, offer.item_code, offer.shop_name)
            if key in seen:
                continue
            seen.add(key)
            offers_for_this.append(offer)

        # Sort by effective cost (protein_cost) and keep top STORE_HITS
        offers_for_this.sort(key=lambda x: x.protein_cost)
        offers_for_this = offers_for_this[:STORE_HITS]

        # Append to history buffer
        all_offers.extend(offers_for_this)

        # Determine today's best and upsert Min_Summary
        if offers_for_this:
            best = offers_for_this[0]
            upsert_today_min(min_ws, today, m.canonical_id, best.protein_cost, best.shop_name, best.item_url)

            y_best = yday_min.get(m.canonical_id)
            a_best = alltime_min.get(m.canonical_id)

            changed_shop = (y_best is not None) and (best.shop_name != y_best[1])
            new_alltime_low = (a_best is None) or (best.protein_cost < a_best[0])

            if changed_shop or new_alltime_low:
                top3 = offers_for_this[:3]
                lines = [
                    f"- canonical_id: `{m.canonical_id}` / keyword: {m.search_keyword}",
                    f"- 今日の最安: **{best.shop_name}** / 実質(タンパク1kgあたり): **{best.protein_cost:,.0f}円**",
                    f"- 価格: {best.raw_price:,}円 送料加算:{best.shipping_cost:,}円 pt:{best.point_rate*100:.1f}%",
                    f"- 商品: {best.item_name[:100]}",
                    f"- URL: {best.item_url}",
                ]
                if y_best:
                    lines.append(f"- 昨日の最安: {y_best[1]} / {y_best[0]:,.0f}円")
                if a_best:
                    lines.append(f"- 過去最安: {a_best[1]} / {a_best[0]:,.0f}円")

                lines.append("")
                lines.append("Top3:")
                for i, o in enumerate(top3, 1):
                    lines.append(
                        f"{i}. {o.shop_name} / {o.protein_cost:,.0f}円 (価格{o.raw_price:,}+送料{o.shipping_cost:,}, pt{o.point_rate*100:.1f}%)"
                    )

                title = "【過去最安更新】" if new_alltime_low else "【最安ショップ入れ替わり】"
                notify_payloads.append((f"{title} {m.canonical_id} ({today})", lines))

    # Write to Price_History
    append_history(hist_ws, all_offers)

    # Send notifications
    for title, lines in notify_payloads:
        discord_notify(title, lines)

    print(f"OK: appended {len(all_offers)} rows, notified {len(notify_payloads)} items.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        msg = "".join(traceback.format_exception(type(e), e, e.__traceback__))[-1800:]
        discord_notify("❌ Rakuten protein tracker failed", [f"```{msg}```"])
        raise
