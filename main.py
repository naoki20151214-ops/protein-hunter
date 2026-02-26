import os
import json
import math
import time
import base64
import re
import traceback
from xml.sax.saxutils import escape
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
RAKUTEN_AFFILIATE_ID = os.environ.get("RAKUTEN_AFFILIATE_ID", "").strip()

SHEET_ID = os.environ.get("SHEET_ID", "").strip()

# Recommended: store Base64 of the service account JSON in GitHub Secrets
GSPREAD_SERVICE_ACCOUNT_JSON_B64 = os.environ.get("GSPREAD_SERVICE_ACCOUNT_JSON_B64", "").strip()

DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()
HATENA_ID = os.environ.get("HATENA_ID", "").strip()
HATENA_API_KEY = os.environ.get("HATENA_API_KEY", "").strip()
HATENA_BLOG_ID = os.environ.get("HATENA_BLOG_ID", "").strip()

HATENA_API_BASE = "https://blog.hatena.ne.jp"

# Rakuten postageFlag (official): 0 = shipping included, 1 = shipping NOT included 
DEFAULT_SHIPPING_YEN = int(os.environ.get("DEFAULT_SHIPPING_YEN", "800"))

# Fetch more than we store, to avoid missing effective cheapest offers
FETCH_HITS = int(os.environ.get("FETCH_HITS", "100"))     # total offers fetched per canonical_id
STORE_HITS = int(os.environ.get("STORE_HITS", "20"))      # offers stored per canonical_id

REQUEST_SLEEP_SEC = float(os.environ.get("REQUEST_SLEEP_SEC", "1.0"))
STRICT_MODE = os.environ.get("STRICT_MODE", "false").strip().lower() in {"1", "true", "yes", "on"}

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
RAKUTEN_ENDPOINT = "https://app.rakuten.co.jp/services/api/IchibaItem/Search/20220601"


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
    try:
        resp = requests.post(DISCORD_WEBHOOK_URL, json={"content": content}, timeout=20)
        resp.raise_for_status()
    except Exception:
        print(f"ERROR discord: failed to send notification title={title[:80]}")
        traceback.print_exc()


@dataclass
class HatenaPostResult:
    ok: bool
    status_code: Optional[int]
    endpoint: str
    message: str


def build_hatena_service_endpoint() -> Optional[str]:
    if not HATENA_ID or not HATENA_BLOG_ID:
        return None
    return f"{HATENA_API_BASE}/{HATENA_ID}/{HATENA_BLOG_ID}/atom"


def build_hatena_entry_endpoint() -> Optional[str]:
    service_endpoint = build_hatena_service_endpoint()
    if not service_endpoint:
        return None
    return f"{service_endpoint}/entry"


def log_hatena_service_document(auth: Tuple[str, str], service_endpoint: str) -> None:
    try:
        resp = requests.get(service_endpoint, auth=auth, timeout=30)
        print(f"DEBUG hatena: service_document status={resp.status_code} endpoint={service_endpoint}")
        body_preview = (resp.text or "")[:500].replace("\n", " ").strip()
        if body_preview:
            print(f"DEBUG hatena: service_document body_preview={body_preview}")

        collection_hrefs = re.findall(r'<collection[^>]*href="([^"]+)"', resp.text or "")
        if collection_hrefs:
            print("DEBUG hatena: service_document collections=" + ", ".join(collection_hrefs))
        else:
            print("DEBUG hatena: service_document collections not found")
    except Exception:
        print("ERROR hatena: failed to fetch service document for diagnostics")
        traceback.print_exc()


def build_top3_markdown(best_offers: List[OfferRow]) -> str:
    lines = [
        f"## 🏆 今日のプロテイン価格ランキング – {jst_today_str()}",
        "",
        f"- 基準: タンパク質1kgあたり実質コスト（価格 + 送料 - ポイント）",
        "",
    ]

    if not best_offers:
        lines.extend([
            "### 本日のランキング結果",
            "- 該当なし（対象データが見つかりませんでした）",
        ])
        return "\n".join(lines)

    rank_icons = {1: "🥇", 2: "🥈", 3: "🥉"}
    for i, offer in enumerate(best_offers[:3], 1):
        rank_icon = rank_icons.get(i, "🏅")
        lines.extend(
            [
                f"### {rank_icon} 第{i}位：**{offer.item_name}**",
                f"- 実質コスト：{offer.protein_cost:,.0f}円 / タンパク質1kg",
                f"- 価格詳細：本体 {offer.raw_price:,}円 / 送料 {offer.shipping_cost:,}円 / ポイント {offer.point_rate * 100:.1f}%",
                f"- ショップ：{offer.shop_name}",
                f"- 🎯 リンク：👉 [楽天で商品を見る]({offer.item_url})",
                "",
            ]
        )

    lines.extend(["---", "", "※ このフォーマットははてなブログAtomPub投稿用です。"])

    return "\n".join(lines).strip()


def post_top3_to_hatena(markdown_body: str) -> HatenaPostResult:
    if not HATENA_ID or not HATENA_API_KEY or not HATENA_BLOG_ID:
        msg = "skipped post because HATENA_ID/HATENA_API_KEY/HATENA_BLOG_ID is missing"
        print(f"WARNING hatena: {msg}")
        return HatenaPostResult(ok=False, status_code=None, endpoint="", message=msg)

    entry_endpoint = build_hatena_entry_endpoint()
    service_endpoint = build_hatena_service_endpoint()
    if not entry_endpoint or not service_endpoint:
        msg = "skipped post because endpoint could not be built"
        print(f"WARNING hatena: {msg}")
        return HatenaPostResult(ok=False, status_code=None, endpoint="", message=msg)

    title = f"【プロテイン価格ランキング】{jst_today_str()}"
    atom_xml = f"""<?xml version=\"1.0\" encoding=\"utf-8\"?>
<entry xmlns=\"http://www.w3.org/2005/Atom\" xmlns:app=\"http://www.w3.org/2007/app\">
  <title>{escape(title)}</title>
  <author><name>{escape(HATENA_ID)}</name></author>
  <content type=\"text/x-markdown\">{escape(markdown_body)}</content>
  <app:control>
    <app:draft>yes</app:draft>
  </app:control>
</entry>
"""

    try:
        print(f"INFO hatena: posting draft endpoint={entry_endpoint}")
        resp = requests.post(
            entry_endpoint,
            data=atom_xml.encode("utf-8"),
            auth=(HATENA_ID, HATENA_API_KEY),
            headers={"Content-Type": "application/xml; charset=utf-8"},
            timeout=30,
        )
        print(f"INFO hatena: draft post response status={resp.status_code} endpoint={entry_endpoint}")
        if resp.status_code >= 400:
            body_preview = (resp.text or "")[:500].replace("\n", " ").strip()
            msg = f"draft post failed body={body_preview}"
            print(f"ERROR hatena: {msg} status={resp.status_code} endpoint={entry_endpoint}")
            if resp.status_code == 404:
                log_hatena_service_document((HATENA_ID, HATENA_API_KEY), service_endpoint)
            return HatenaPostResult(ok=False, status_code=resp.status_code, endpoint=entry_endpoint, message=msg)
        print(f"INFO hatena: draft post succeeded status={resp.status_code} endpoint={entry_endpoint}")
        return HatenaPostResult(ok=True, status_code=resp.status_code, endpoint=entry_endpoint, message="draft post succeeded")
    except Exception as e:
        msg = f"failed to post top3 draft: {e}"
        print(f"ERROR hatena: {msg} endpoint={entry_endpoint}")
        traceback.print_exc()
        return HatenaPostResult(ok=False, status_code=None, endpoint=entry_endpoint, message=msg)


# =========================
# Google Sheets
# =========================
def load_service_account_dict_b64() -> dict:
    if not (SHEET_ID and GSPREAD_SERVICE_ACCOUNT_JSON_B64):
        raise RuntimeError("Missing SHEET_ID or GSPREAD_SERVICE_ACCOUNT_JSON_B64")
    raw = base64.b64decode(GSPREAD_SERVICE_ACCOUNT_JSON_B64.encode("utf-8")).decode("utf-8")
    return json.loads(raw)

def open_sheets():
    masked_sheet_id = f"{SHEET_ID[:4]}...{SHEET_ID[-4:]}" if len(SHEET_ID) >= 8 else "(masked)"
    print(f"DEBUG sheet: opening sheet... sheet_id={masked_sheet_id}")
    creds_dict = load_service_account_dict_b64()
    gc = gspread.service_account_from_dict(creds_dict)
    print("DEBUG sheet: gspread authentication success")
    sh = gc.open_by_key(SHEET_ID)

    master_ws = sh.worksheet("Master_List")
    print(f"DEBUG sheet: worksheet name={master_ws.title}")
    hist_ws = sh.worksheet("Price_History")
    print(f"DEBUG sheet: worksheet name={hist_ws.title}")

    # Min_Summary worksheet (create if missing)
    try:
        min_ws = sh.worksheet("Min_Summary")
        print(f"DEBUG sheet: worksheet name={min_ws.title}")
    except gspread.exceptions.WorksheetNotFound:
        print("DEBUG sheet: worksheet name=Min_Summary (not found, creating)")
        min_ws = sh.add_worksheet(title="Min_Summary", rows=2000, cols=10)
        min_ws.append_row(
            ["date", "canonical_id", "min_cost", "min_shop", "min_url", "updated_at"],
            value_input_option="RAW",
        )
        print(f"DEBUG sheet: worksheet name={min_ws.title} (created)")

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
    print(f"DEBUG sheet: appending {len(values)} rows")
    try:
        hist_ws.append_rows(values, value_input_option="RAW")
    except Exception:
        print("ERROR sheet: append_rows failed")
        traceback.print_exc()
        raise
    print("DEBUG sheet: append success")

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
def rakuten_search_page(keyword: str, page: int, hits: int) -> Tuple[List[Dict[str, Any]], int]:
    if not RAKUTEN_APP_ID:
        raise RuntimeError("Missing RAKUTEN_APP_ID")

    params = {
        "applicationId": RAKUTEN_APP_ID,
        "keyword": keyword,
        "hits": max(1, min(30, hits)),
        "page": page,
        "sort": "+itemPrice",
        "format": "json",
        "formatVersion": 2,
    }
    if RAKUTEN_AFFILIATE_ID:
        params["affiliateId"] = RAKUTEN_AFFILIATE_ID

    resp = requests.get(RAKUTEN_ENDPOINT, params=params, timeout=30)
    resp.raise_for_status()

    data = resp.json()

    print("DEBUG http:", resp.status_code, "keys:", list(data.keys())[:10])

    total_count = safe_int(data.get("count", 0), 0) if isinstance(data, dict) else 0

    # formatVersion=2 style
    if isinstance(data, dict) and data.get("items"):
        return data["items"], total_count

    # old style variants
    if isinstance(data, dict) and data.get("Items"):
        items = data["Items"]
        if not items:
            return []
        first = items[0]
        # {"Items":[{"Item":{...}}, ...]}
        if isinstance(first, dict) and "Item" in first:
            return [x["Item"] for x in items if isinstance(x, dict) and "Item" in x], total_count
        # {"Items":[{...}, ...]}  ← こっちもある
        if isinstance(first, dict):
            return items, total_count

    # API error payload
    if isinstance(data, dict) and (data.get("error") or data.get("error_description")):
        raise RuntimeError(f"Rakuten API error: {data.get('error')} {data.get('error_description')}")

    return [], total_count
    
def rakuten_search_multi_pages(keyword: str, total_hits: int) -> Tuple[List[Dict[str, Any]], int]:
    all_items: List[Dict[str, Any]] = []
    remaining = total_hits
    page = 1
    api_total_count = 0

    while remaining > 0:
        hits = min(30, remaining)
        items, total_count = rakuten_search_page(keyword, page=page, hits=hits)
        if page == 1:
            api_total_count = total_count
        if not items:
            break

        all_items.extend(items)
        remaining -= len(items)

        if len(items) < hits:
            break

        page += 1
        if page > 10:
            break

        time.sleep(0.3)

    return all_items, api_total_count

# =========================
# Filtering / Compute
# =========================
def looks_like_garbage(item_name: str) -> bool:
    name = item_name or ""
    return any(k in name for k in EXCLUDE_KEYWORDS)
    
def _norm_name(s: str) -> str:
    s = (s or "").lower()
    # 全角数字→半角
    s = s.translate(str.maketrans("０１２３４５６７８９", "0123456789"))
    # 全角英字っぽいのを半角へ寄せる（最低限）
    s = s.replace("ｋ", "k").replace("ｇ", "g").replace("Ｋ", "k").replace("Ｇ", "g")
    # スペース類を消す
    s = re.sub(r"\s+", "", s)
    return s

def capacity_strict_match(master: MasterItem, item_name: str) -> bool:
    if not STRICT_CAPACITY_MATCH:
        return False
    if master.capacity_kg <= 0:
        return True

    name = _norm_name(item_name)
    kg = master.capacity_kg

    if kg >= 1.0:
        n = int(round(kg))
        # 例: 3kg / 3kg×1 / 3kgx1 / 3kg(〜) / 3kg入り などを許容
        return re.search(rf"{n}kg($|[×x\(\)0-9]|入り|ﾊﾟｯｸ|袋|個)", name) is not None or f"{n}kg" in name

    grams = int(round(kg * 1000))
    return re.search(rf"{grams}g($|[×x\(\)0-9]|入り|ﾊﾟｯｸ|袋|個)", name) is not None or f"{grams}g" in name
    
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


def classify_item_filter(master: MasterItem, item: Dict[str, Any], seen_keys: set) -> Tuple[Optional[OfferRow], Optional[str]]:
    item_code = str(item.get("itemCode", "")).strip()
    shop_name = str(item.get("shopName", "")).strip()
    raw_price = safe_int(item.get("itemPrice", 0), 0)
    item_name = str(item.get("itemName", "")).strip()

    if not item_code or not shop_name or raw_price <= 0:
        return None, "missing_required_or_invalid_price"
    if looks_like_garbage(item_name):
        return None, "excluded_keyword"
    if not capacity_strict_match(master, item_name):
        return None, "capacity_mismatch"

    offer = compute_offer(master, item)
    if not offer:
        return None, "invalid_offer"

    key = (offer.date, offer.canonical_id, offer.item_code, offer.shop_name)
    if key in seen_keys:
        return None, "duplicate"

    return offer, None


# =========================
# Main
# =========================
def main():
    print("ACCESS_KEY len:", len(os.environ.get("RAKUTEN_ACCESS_KEY","")))
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
    best_offers_for_ranking: List[OfferRow] = []
    run_errors: List[str] = []

    for m in masters:
        time.sleep(REQUEST_SLEEP_SEC)

        # Fetch many, then compute effective cost and keep best STORE_HITS
        items, api_total_count = rakuten_search_multi_pages(m.search_keyword, total_hits=FETCH_HITS)
        print(
            "DEBUG fetch:",
            f"canonical_id={m.canonical_id}",
            f"keyword={m.search_keyword}",
            f"api_total_count={api_total_count}",
            f"fetched_items={len(items)}",
            f"sample={(items[0].get('itemName', '')[:60] if items else 'NONE')}",
        )

        seen = set()  # (date,cid,item_code,shop_name)
        offers_for_this: List[OfferRow] = []
        filter_drop_counts: Dict[str, int] = {
            "missing_required_or_invalid_price": 0,
            "excluded_keyword": 0,
            "capacity_mismatch": 0,
            "invalid_offer": 0,
            "duplicate": 0,
        }

        for it in items:
            offer, dropped_reason = classify_item_filter(m, it, seen)
            if not offer:
                if dropped_reason:
                    filter_drop_counts[dropped_reason] += 1
                continue
            key = (offer.date, offer.canonical_id, offer.item_code, offer.shop_name)
            seen.add(key)
            offers_for_this.append(offer)

        accepted_before_store_limit = len(offers_for_this)
        dropped_by_store_limit = max(0, accepted_before_store_limit - STORE_HITS)
        filter_drop_counts["store_hits_limit"] = dropped_by_store_limit

        print(
            "DEBUG filter:",
            f"canonical_id={m.canonical_id}",
            f"input_items={len(items)}",
            f"accepted_before_store_limit={accepted_before_store_limit}",
            "drop_counts=" + json.dumps(filter_drop_counts, ensure_ascii=False),
        )

        # Sort by effective cost (protein_cost) and keep top STORE_HITS
        offers_for_this.sort(key=lambda x: x.protein_cost)
        offers_for_this = offers_for_this[:STORE_HITS]

        # Append to history buffer
        all_offers.extend(offers_for_this)

        # Determine today's best and upsert Min_Summary
        if offers_for_this:
            best = offers_for_this[0]
            best_offers_for_ranking.append(best)
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
    print(f"DEBUG append: rows_to_append={len(all_offers)}")
    if len(all_offers) == 0:
        msg = "No offers to append after filtering."
        if STRICT_MODE:
            raise RuntimeError(f"STRICT_MODE=true: {msg}")
        print(f"WARNING: {msg} STRICT_MODE=false so run is treated as success.")

    append_history(hist_ws, all_offers)

    # Send notifications
    for title, lines in notify_payloads:
        discord_notify(title, lines)

    # Post Top3 ranking to Hatena Blog (draft)
    best_offers_for_ranking.sort(key=lambda x: x.protein_cost)
    ranking_markdown = build_top3_markdown(best_offers_for_ranking)
    hatena_result = post_top3_to_hatena(ranking_markdown)
    if not hatena_result.ok:
        run_errors.append(
            f"Hatena draft post failed (status={hatena_result.status_code}, endpoint={hatena_result.endpoint}): {hatena_result.message}"
        )

    summary_lines = [
        f"- date: {today}",
        f"- appended rows: {len(all_offers)}",
        f"- change notifications: {len(notify_payloads)}",
        f"- hatena status: {'OK' if hatena_result.ok else 'NG'}",
        f"- hatena endpoint: {hatena_result.endpoint or '(not built)'}",
        f"- hatena http_status: {hatena_result.status_code if hatena_result.status_code is not None else 'N/A'}",
    ]
    if run_errors:
        summary_lines.append("- errors:")
        for err in run_errors:
            summary_lines.append(f"  - {err[:300]}")

    discord_notify("📊 Rakuten protein tracker summary", summary_lines)

    if run_errors:
        raise RuntimeError("; ".join(run_errors))

    print(f"OK: appended {len(all_offers)} rows, notified {len(notify_payloads)} items.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        msg = "".join(traceback.format_exception(type(e), e, e.__traceback__))[-1800:]
        discord_notify("❌ Rakuten protein tracker failed", [f"```{msg}```"])
        raise
