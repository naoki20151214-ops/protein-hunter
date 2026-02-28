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
RANKING_N = int(os.environ.get("RANKING_N", "20"))
HERO_K = int(os.environ.get("HERO_K", "3"))

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
    image_url: str


# =========================
# Helpers
# =========================
def jst_date() -> datetime.date:
    return datetime.now(ZoneInfo("Asia/Tokyo")).date()

def jst_today_str() -> str:
    return jst_date().isoformat()

def jst_now_iso() -> str:
    return datetime.now(ZoneInfo("Asia/Tokyo")).isoformat(timespec="seconds")


def choose_variant_jst(now: Optional[datetime] = None) -> Tuple[str, str, str, str, str, str]:
    dt = now.astimezone(ZoneInfo("Asia/Tokyo")) if now else datetime.now(ZoneInfo("Asia/Tokyo"))
    weekday = dt.weekday()  # Mon=0..Sun=6
    weekday_names = ["月", "火", "水", "木", "金", "土", "日"]

    if weekday in {0, 2, 4}:  # Mon/Wed/Fri
        return (
            "A",
            "今日が買い時",
            "30日最安水準",
            "補充する人は今日が安全。ポイント条件だけ確認してGO。",
            dt.date().isoformat(),
            weekday_names[weekday],
        )
    return (
        "B",
        "逃すと損しやすい水準",
        "急落後は戻りやすい",
        "この水準は長く続かないことが多い。売り切れ前に確認。",
        dt.date().isoformat(),
        weekday_names[weekday],
    )


def normalize_image_url(url: str) -> str:
    image_url = (url or "").strip()
    if not image_url:
        return ""

    if image_url.startswith("//"):
        image_url = f"https:{image_url}"

    image_url = re.sub(r"^http://", "https://", image_url, flags=re.IGNORECASE)

    if re.search(r"([?&])_ex=\d+x\d+", image_url):
        image_url = re.sub(r"([?&])_ex=\d+x\d+", r"\1_ex=600x600", image_url)
    else:
        image_url = f"{image_url}&_ex=600x600" if "?" in image_url else f"{image_url}?_ex=600x600"

    return image_url


def pick_best_image_url(item: Dict[str, Any]) -> str:
    def first_image_url(raw: Any) -> str:
        if isinstance(raw, str):
            return normalize_image_url(raw)
        if isinstance(raw, dict):
            for key in ("imageUrl", "itemImageUrl", "url"):
                if raw.get(key):
                    return normalize_image_url(str(raw.get(key, "")))
            return ""
        if isinstance(raw, list):
            for elem in raw:
                selected = first_image_url(elem)
                if selected:
                    return selected
        return ""

    for key in (
        "mediumImageUrls",
        "smallImageUrls",
        "imageUrl",
        "itemImageUrl",
        "itemImageUrls",
    ):
        selected_url = first_image_url(item.get(key))
        if selected_url:
            return selected_url

    return ""


def shorten_item_name(name: str, limit: int = 40) -> str:
    text = (name or "").strip()
    if len(text) <= limit:
        return text
    return text[:limit] + "…"

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


@dataclass
class PriceChangeReport:
    level: str
    today_price: int
    yesterday_price: Optional[int]
    diff_yen: Optional[int]
    diff_pct: Optional[float]
    is_30d_low: bool
    min_30d_price: Optional[int]
    variant: str
    variant_headline: str
    variant_reason: str
    variant_push_text: str
    date_jst: str
    weekday_jst: str
    image_url: str
    image_selected: bool
    short_item_name: str
    x_text: str
    hatena_markdown: str
    persona_summary_lines: List[str]


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


PERSONA_SECTIONS: List[Tuple[str, str, bool]] = [
    ("① 初めての人", "🌱", True),
    ("② ガッツリ増量したい人", "💪", True),
    ("③ 本気で筋肥大したい人", "🏋️", False),
    ("④ ダイエット中の人", "🥗", True),
    ("⑤ 40代以上の健康維持層", "🧘", False),
    ("⑥ 家計重視・まとめ買い派", "💴", False),
    ("⑦ 味重視派", "😋", False),
    ("⑧ 無添加志向・成分重視派", "🧪", False),
    ("⑨ 運動は軽め・健康目的派", "🚶", False),
    ("⑩ 今だけ安い狙い撃ち派", "🎯", True),
]


def contains_any(text: str, patterns: List[str]) -> bool:
    src = (text or "").lower()
    return any(re.search(p, src, flags=re.IGNORECASE) for p in patterns)


def looks_official_or_major_shop(shop_name: str) -> bool:
    return contains_any(
        shop_name,
        [
            r"公式",
            r"オフィシャル",
            r"本店",
            r"楽天24",
            r"rakuten",
            r"amazon",
            r"yahoo",
            r"大手",
            r"直営",
        ],
    )


def choose_offer_from_candidates(
    candidates: List[OfferRow],
    fallback_sorted: List[OfferRow],
    used_urls: set,
    prefer_unused: bool = True,
) -> OfferRow:
    ordered = candidates + [o for o in fallback_sorted if o not in candidates]
    if prefer_unused:
        for offer in ordered:
            if offer.item_url not in used_urls:
                used_urls.add(offer.item_url)
                return offer
    chosen = ordered[0] if ordered else fallback_sorted[0]
    used_urls.add(chosen.item_url)
    return chosen


def assign_persona_sections(offers_for_this: List[OfferRow], prefer_unused: bool = True) -> Dict[str, OfferRow]:
    if not offers_for_this:
        return {}

    by_protein = sorted(offers_for_this, key=lambda x: x.protein_cost)
    by_price = sorted(offers_for_this, key=lambda x: x.raw_price)
    median_price_offer = by_price[len(by_price) // 2]
    median_protein_offer = by_protein[len(by_protein) // 2]
    used_urls: set = set()
    out: Dict[str, OfferRow] = {}

    def pick(section_name: str, candidates: List[OfferRow], fallback: List[OfferRow]) -> None:
        out[section_name] = choose_offer_from_candidates(candidates, fallback, used_urls, prefer_unused=prefer_unused)

    # ① 初めて
    pick("① 初めての人", [o for o in offers_for_this if contains_any(o.item_name, [r"1\s*kg", r"1\s*キロ", r"1キログラム"])], by_price)

    # ② 増量
    pick("② ガッツリ増量したい人", [o for o in offers_for_this if contains_any(o.item_name, [r"3\s*kg", r"3\s*キロ", r"3キログラム"])], by_protein)

    # ③ 筋肥大
    pick(
        "③ 本気で筋肥大したい人",
        [o for o in offers_for_this if contains_any(o.item_name, [r"wpi", r"アイソレート", r"高たんぱく", r"高タンパク"])],
        by_protein,
    )

    # ④ ダイエット
    pick(
        "④ ダイエット中の人",
        [o for o in offers_for_this if contains_any(o.item_name, [r"低脂質", r"低糖質", r"ダイエット", r"甘くない", r"プレーン"])],
        sorted(offers_for_this, key=lambda o: abs(o.raw_price - median_price_offer.raw_price)),
    )

    # ⑤ 40代
    protein_threshold = by_protein[max(0, len(by_protein) // 2 - 1)].protein_cost
    pick(
        "⑤ 40代以上の健康維持層",
        [o for o in offers_for_this if o.protein_cost <= protein_threshold and looks_official_or_major_shop(o.shop_name)],
        by_protein,
    )

    # ⑥ 家計
    pick(
        "⑥ 家計重視・まとめ買い派",
        [o for o in offers_for_this if contains_any(o.item_name, [r"3\s*kg", r"3\s*キロ", r"大容量", r"まとめ買い"])],
        sorted(offers_for_this, key=lambda o: (o.raw_price, o.protein_cost)),
    )

    # ⑦ 味
    flavor_hit = [
        o for o in offers_for_this
        if contains_any(o.item_name, [r"チョコ", r"バニラ", r"ストロベリー", r"抹茶", r"黒糖", r"ヨーグルト", r"マンゴー", r"ピーチ", r"メロン"])
    ]
    pick("⑦ 味重視派", flavor_hit, sorted(offers_for_this, key=lambda o: (0 if o.image_url else 1, o.protein_cost)))

    # ⑧ 無添加
    pick(
        "⑧ 無添加志向・成分重視派",
        [o for o in offers_for_this if contains_any(o.item_name, [r"無添加", r"人工甘味料不使用", r"甘味料不使用", r"保存料不使用"])],
        sorted(offers_for_this, key=lambda o: (0 if looks_official_or_major_shop(o.shop_name) else 1, o.protein_cost)),
    )

    # ⑨ 軽め
    low_idx = int((len(by_price) - 1) * 0.1)
    high_idx = int((len(by_price) - 1) * 0.9)
    low_price = by_price[low_idx].raw_price
    high_price = by_price[high_idx].raw_price
    moderate = [o for o in offers_for_this if low_price <= o.raw_price <= high_price]
    pick("⑨ 運動は軽め・健康目的派", moderate, sorted(offers_for_this, key=lambda o: abs(o.protein_cost - median_protein_offer.protein_cost)))

    # ⑩ 狙い撃ち
    max_point = max(o.point_rate for o in offers_for_this)
    pick("⑩ 今だけ安い狙い撃ち派", [o for o in offers_for_this if o.point_rate == max_point], sorted(offers_for_this, key=lambda o: (-o.point_rate, o.protein_cost)))

    return out


def build_persona_reason(section_name: str, offer: OfferRow, offers: List[OfferRow]) -> str:
    min_protein = min(o.protein_cost for o in offers)
    min_price = min(o.raw_price for o in offers)
    max_point = max(o.point_rate for o in offers)
    median_price = sorted(o.raw_price for o in offers)[len(offers) // 2]

    if section_name == "① 初めての人":
        if contains_any(offer.item_name, [r"1\s*kg", r"1\s*キロ", r"1キログラム"]):
            return "1kg前後の表記で量感がつかみやすく、初回でも選びやすい。価格帯も読みやすく、失敗しづらい一品。"
        if offer.raw_price == min_price:
            return "今日の価格帯で最安クラス。まず始める1袋として出費を抑えやすい。"
        return "価格と単価のバランスが安定していて、最初の1品として無理なく続けやすい。"
    if section_name == "② ガッツリ増量したい人":
        if offer.protein_cost == min_protein:
            return "単価が今日の最安。毎日しっかり飲む前提の人に刺さる構成。"
        if offer.point_rate >= 0.05:
            return "ポイント還元が強く、実質コスパがさらに伸びやすい。増量期の継続コストを抑えやすい。"
        return "容量寄りの候補として日々の消費に向く。送料込みでも総額で見て優位を作りやすい。"
    if section_name == "③ 本気で筋肥大したい人":
        if contains_any(offer.item_name, [r"wpi", r"アイソレート", r"高たんぱく", r"高タンパク"]):
            return "高たんぱく系のキーワードを含む候補。トレーニング重視で成分軸を優先したい日に合う。"
        if offer.protein_cost <= min_protein * 1.05:
            return "単価が上位水準なので、摂取量を増やす局面でも継続しやすい。"
        return "上位コスパ帯から選定。実行しやすさを重視した筋肥大向けの現実解。"
    if section_name == "④ ダイエット中の人":
        if contains_any(offer.item_name, [r"低脂質", r"低糖質", r"ダイエット", r"甘くない", r"プレーン"]):
            return "ダイエット向けキーワードを優先して選定。味付けが重すぎず、調整しやすい。"
        if abs(offer.raw_price - median_price) <= max(300, median_price * 0.05):
            return "価格が中位付近で極端さが少ない。続ける前提の置き換え用として扱いやすい。"
        return "単価と総額の偏りが小さく、減量中でも管理しやすい一本。"
    if section_name == "⑤ 40代以上の健康維持層":
        if looks_official_or_major_shop(offer.shop_name):
            return "公式・大手寄りショップを優先。購入動線がわかりやすく、継続しやすい。"
        if offer.protein_cost <= min_protein * 1.1:
            return "単価が上位50%以内の水準で、無理のない継続コストに寄せやすい。"
        return "価格バランス重視で選定。習慣化を崩しにくい堅実な候補。"
    if section_name == "⑥ 家計重視・まとめ買い派":
        if contains_any(offer.item_name, [r"3\s*kg", r"3\s*キロ", r"大容量", r"まとめ買い"]):
            return "大容量キーワード優先で、買い足し回数を減らしやすい。家計管理の手間も抑えやすい。"
        if offer.raw_price == min_price:
            return "本体価格が最安クラス。まず総支出を抑えたい日には有力。"
        return "価格と単価の両面から家計優先で選定。日次運用で扱いやすい。"
    if section_name == "⑦ 味重視派":
        if contains_any(offer.item_name, [r"チョコ", r"バニラ", r"ストロベリー", r"抹茶", r"黒糖", r"ヨーグルト", r"マンゴー", r"ピーチ", r"メロン"]):
            return "フレーバー語を含む候補を優先。毎日飲む前提でも飽きにくさを狙える。"
        if offer.image_url:
            return "画像付きで味のイメージを掴みやすい候補を優先。選ぶストレスを下げやすい。"
        return "味軸の候補が薄い日は、見た目情報と単価のバランスで無難に選定。"
    if section_name == "⑧ 無添加志向・成分重視派":
        if contains_any(offer.item_name, [r"無添加", r"人工甘味料不使用", r"甘味料不使用", r"保存料不使用"]):
            return "無添加系キーワードを優先。成分基準で選びたい日に判断しやすい。"
        if looks_official_or_major_shop(offer.shop_name):
            return "公式・大手ショップ寄りを採用。商品情報の確認がしやすい点を重視。"
        return "成分訴求が弱い日はショップ信頼度と価格安定性を優先して選定。"
    if section_name == "⑨ 運動は軽め・健康目的派":
        if offer.raw_price == median_price:
            return "価格が中央値で極端な高安を避けられる。軽め運動の補助として続けやすい。"
        if offer.protein_cost <= min_protein * 1.15:
            return "単価が中庸〜上位帯で、過不足のないコスパを取りやすい。"
        return "高すぎず安すぎない帯から選定。健康維持目的でも使いやすい。"
    if section_name == "⑩ 今だけ安い狙い撃ち派":
        if offer.point_rate == max_point:
            return "本日のポイント還元が最大クラス。実質負担を狙って取りにいける構成。"
        if offer.protein_cost == min_protein:
            return "還元を除いても単価が最安水準。タイミング買いの主軸にしやすい。"
        return "還元と単価の合算でお得感を優先。短期の狙い撃ちに向く候補。"
    return "当日の価格・還元条件から機械選定したおすすめです。"


def build_persona_sections_markdown(assignments: Dict[str, OfferRow], offers: List[OfferRow]) -> Tuple[List[str], List[str]]:
    markdown_lines: List[str] = ["## 人別おすすめ（今日の10枠）", ""]
    discord_lines: List[str] = []
    image_sections = {"① 初めての人", "② ガッツリ増量したい人", "④ ダイエット中の人", "⑩ 今だけ安い狙い撃ち派"}

    for section_name, emoji, show_image in PERSONA_SECTIONS:
        offer = assignments.get(section_name)
        if not offer:
            continue
        reason = build_persona_reason(section_name, offer, offers)
        point_pct = offer.point_rate * 100.0
        markdown_lines.extend(
            [
                f"## {emoji} {section_name}",
                f"- おすすめ: **{shorten_item_name(offer.item_name, 52)}**",
                f"- 理由: {reason}",
                f"- 実質: **{offer.protein_cost:,.0f}円/kg**｜価格: {offer.raw_price:,}円｜pt: {point_pct:.1f}%｜ショップ: {offer.shop_name}",
                f"**👉 [商品を見に行く]({offer.item_url})**",
            ]
        )
        if show_image and section_name in image_sections and offer.image_url:
            markdown_lines.append(f"![商品画像]({offer.image_url})")
        markdown_lines.append("")
        discord_lines.append(f"- {section_name}: {shorten_item_name(offer.item_name, 26)}｜{offer.protein_cost:,.0f}円/kg｜{offer.item_url}")

    return markdown_lines, discord_lines


def is_explosion_3kg_target(master: MasterItem) -> bool:
    cid = (master.canonical_id or "").lower()
    kw = (master.search_keyword or "").lower()
    brand = (master.brand or "").lower()
    name_hit = "explosion" in cid or "explosion" in kw or "エクスプロージョン" in master.search_keyword or "エクスプロージョン" in master.brand
    return name_hit and abs(master.capacity_kg - 3.0) < 1e-9


def read_price_history_daily_min(hist_ws, canonical_id: str) -> Dict[str, int]:
    rows = hist_ws.get_all_records()
    out: Dict[str, int] = {}
    for r in rows:
        cid = str(r.get("canonical_id", "")).strip()
        if cid != canonical_id:
            continue
        day = str(r.get("date", "")).strip()
        if not day:
            continue
        raw_price = safe_int(r.get("raw_price", math.inf), math.inf)
        if raw_price == math.inf:
            continue
        prev = out.get(day)
        if prev is None or raw_price < prev:
            out[day] = raw_price
    return out


def choose_level(diff_yen: Optional[int], diff_pct: Optional[float], is_30d_low: bool) -> str:
    if is_30d_low:
        return "big_drop"
    if diff_yen is None or diff_pct is None:
        return "normal"
    if diff_pct <= -5.0 or diff_yen <= -500:
        return "big_drop"
    if diff_pct <= -3.0 or diff_yen <= -300:
        return "drop"
    return "normal"


def build_marketing_report(
    master: MasterItem,
    best_offer: OfferRow,
    hist_ws,
    today: str,
    yesterday: str,
    ranking_offers: Optional[List[OfferRow]] = None,
) -> PriceChangeReport:
    daily_min = read_price_history_daily_min(hist_ws, master.canonical_id)
    today_price = best_offer.raw_price
    yesterday_price = daily_min.get(yesterday)

    diff_yen: Optional[int] = None
    diff_pct: Optional[float] = None
    if yesterday_price and yesterday_price > 0:
        diff_yen = today_price - yesterday_price
        diff_pct = (diff_yen / yesterday_price) * 100.0

    start_date = (jst_date() - timedelta(days=29)).isoformat()
    recent_prices = [p for d, p in daily_min.items() if start_date <= d <= today]
    if recent_prices:
        min_30d_price = min(recent_prices)
        is_30d_low = today_price <= min_30d_price
    else:
        min_30d_price = None
        is_30d_low = False

    level = choose_level(diff_yen, diff_pct, is_30d_low)
    variant, variant_headline, variant_reason, variant_push_text, date_jst, weekday_jst = choose_variant_jst()
    short_name = shorten_item_name(best_offer.item_name)

    diff_label = (
        f"前日比 {diff_yen:+,}円 ({diff_pct:+.1f}%)"
        if diff_yen is not None and diff_pct is not None
        else "前日比 データ不足"
    )
    low30_label = f"30日最安 {min_30d_price:,}円" if min_30d_price is not None else "30日最安 データ不足"
    diff_inline = (
        f"{diff_yen:+,}円（{diff_pct:+.1f}%）"
        if diff_yen is not None and diff_pct is not None
        else "データ不足"
    )
    low30_flag = "更新" if is_30d_low else "未更新"

    x_text = "\n".join(
        [
            "【Rakuten Protein Tracker】",
            "エクスプロージョン 3kg 価格チェック",
            f"今日の最安: {today_price:,}円",
            diff_label,
            f"変動レベル: {level}",
            f"{low30_label} / {low30_flag}",
            variant_push_text,
            best_offer.item_url,
            "#楽天市場 #プロテイン #エクスプロージョン",
        ]
    )

    image_block_lines: List[str] = []
    if best_offer.image_url:
        image_block_lines = [f"![商品画像]({best_offer.image_url})", ""]

    ranking_sections: List[str] = []
    persona_section_lines: List[str] = []
    persona_summary_lines: List[str] = []
    if ranking_offers is not None:
        persona_assignments = assign_persona_sections(ranking_offers, prefer_unused=True)
        assignment_summary = {
            section: (offer.item_url or offer.canonical_id)
            for section, offer in persona_assignments.items()
        }
        print("INFO section assignment summary:", json.dumps(assignment_summary, ensure_ascii=False))
        persona_section_lines, persona_summary_lines = build_persona_sections_markdown(persona_assignments, ranking_offers)

        hero_offers = ranking_offers[:HERO_K]
        top_offers = ranking_offers[:RANKING_N]

        if hero_offers:
            medals = ["🥇", "🥈", "🥉"]
            ranking_sections.extend(["## 今日の推し（TOP3）", ""])
            for i, offer in enumerate(hero_offers):
                medal = medals[i] if i < len(medals) else "🏅"
                point_pct = (offer.point_rate if offer.point_rate is not None else 0.0) * 100.0
                ranking_sections.append(f"### {medal} {shorten_item_name(offer.item_name, 60)}")
                if offer.item_url:
                    ranking_sections.append(f"**👉 [商品を見に行く]({offer.item_url})**")
                if offer.image_url:
                    ranking_sections.append(f"![商品画像]({offer.image_url})")
                ranking_sections.extend(
                    [
                        f"- 実質単価: **{offer.protein_cost:,.0f}円/kg**",
                        f"- 価格: {offer.raw_price:,}円（送料 {offer.shipping_cost:,}円）",
                        f"- pt: {point_pct:.1f}%",
                        f"- ショップ: {offer.shop_name or ''}",
                    ]
                )
                if offer.item_url:
                    ranking_sections.append(f"**👉 [楽天で価格と在庫を確認する]({offer.item_url})**")
                ranking_sections.append("")

        if top_offers:
            ranking_sections.extend(["## 今日のランキング（TOP20）", ""])
            for rank, offer in enumerate(top_offers, 1):
                ranking_sections.append(
                    f"- {rank}. {shorten_item_name(offer.item_name, 60)}｜**{offer.protein_cost:,.0f}円/kg**｜{offer.shop_name or ''}"
                )
                if offer.item_url:
                    ranking_sections.append(f"  - **👉 [商品を見に行く]({offer.item_url})**")
            ranking_sections.append("")

    hatena_markdown = "\n".join(
        image_block_lines + [
            f"🔥 判定：{variant_headline}（{variant_reason}）",
            f"実質：{today_price:,}円/kg｜前日比：{diff_inline}｜30日最安：{low30_flag}",
            "👉 価格と在庫は下のボタンから確認",
            "",
            f"# エクスプロージョン3kg 価格速報（{today}）",
            "",
            f"**{variant_headline}**",
            "",
            f"- 今日最安: **{today_price:,}円/kg**",
            f"- 前日比: **{diff_inline}**",
            f"- 30日最安: **{low30_flag}**（{f'{min_30d_price:,}円' if min_30d_price is not None else 'データ不足'}）",
            "",
            "## 今日の結論",
            f"- 判定: **{variant_headline}**",
            f"- 理由: {variant_reason}",
            "",
        ] + persona_section_lines + ranking_sections + [
            "## 価格データ",
            f"- 商品名: {short_name}",
            f"- ショップ: {best_offer.shop_name}",
            f"- 今日の実質価格: **{today_price:,}円/kg**",
            f"- 前日比: **{diff_inline}**",
            f"- 30日最安: **{low30_flag}**（{f'{min_30d_price:,}円' if min_30d_price is not None else 'データ不足'}）",
            "",
            "## 買い時コメント",
            variant_push_text,
            "",
            "## CTA",
            "### ✅ 今すぐ確認",
            f"**👉 [楽天で価格と在庫を確認する]({best_offer.item_url})**",
            "",
            "## 注意書き",
            "※ 価格・ポイント・在庫は変動します。購入前に楽天の商品ページで最新情報をご確認ください。",
        ]
    )

    return PriceChangeReport(
        level=level,
        today_price=today_price,
        yesterday_price=yesterday_price,
        diff_yen=diff_yen,
        diff_pct=diff_pct,
        is_30d_low=is_30d_low,
        min_30d_price=min_30d_price,
        variant=variant,
        variant_headline=variant_headline,
        variant_reason=variant_reason,
        variant_push_text=variant_push_text,
        date_jst=date_jst,
        weekday_jst=weekday_jst,
        image_url=best_offer.image_url,
        image_selected=bool(best_offer.image_url),
        short_item_name=short_name,
        x_text=x_text,
        hatena_markdown=hatena_markdown,
        persona_summary_lines=persona_summary_lines,
    )


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
<entry xmlns=\"http://www.w3.org/2005/Atom\" xmlns:app=\"http://www.w3.org/2007/app\" xmlns:hatena=\"http://www.hatena.ne.jp/info/xmlns#\">
  <title>{escape(title)}</title>
  <author><name>{escape(HATENA_ID)}</name></author>
  <hatena:syntax>markdown</hatena:syntax>
  <content type=\"text/plain\">{escape(markdown_body)}</content>
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
        # gspread warning対策: updateはキーワード引数(values/range_name)で統一する
        min_ws.update(
            values=[[str(min_cost), min_shop, min_url, updated_at]],
            range_name=f"C{target_row}:F{target_row}",
        )
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
    image_url = pick_best_image_url(item)

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
        image_url=image_url,
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

    masters = [m for m in masters if is_explosion_3kg_target(m)]
    if not masters:
        raise RuntimeError("Target product (エクスプロージョン3kg) not found in Master_List.")

    # Read minima from Min_Summary only (fast)
    yday_min = read_min_summary(min_ws, yesterday)   # {cid: (cost, shop, url)}
    alltime_min = read_alltime_min(min_ws)          # {cid: (cost, shop, url)}

    all_offers: List[OfferRow] = []
    notify_payloads: List[Tuple[str, List[str]]] = []
    best_offers_for_ranking: List[OfferRow] = []
    marketing_reports: List[Tuple[MasterItem, OfferRow, PriceChangeReport]] = []
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
            ranking_offers = offers_for_this[:RANKING_N]
            best_offers_for_ranking.append(best)
            if best.image_url:
                print(f"INFO selected best_offer.image_url canonical_id={m.canonical_id} url={best.image_url}")
            else:
                print(f"WARNING best_offer.image_url is empty canonical_id={m.canonical_id}")
            print(
                f"INFO ranking_count={len(ranking_offers)} hero_count={min(HERO_K, len(ranking_offers))} canonical_id={m.canonical_id}"
            )
            upsert_today_min(min_ws, today, m.canonical_id, best.protein_cost, best.shop_name, best.item_url)
            marketing_reports.append(
                (m, best, build_marketing_report(m, best, hist_ws, today, yesterday, ranking_offers=ranking_offers))
            )

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

    # Generate and notify revenue-maximized posting drafts for Explosion 3kg
    hatena_result = HatenaPostResult(ok=True, status_code=None, endpoint="", message="skipped")
    for m, best, report in marketing_reports:
        diff_line = (
            f"{report.diff_yen:+,}円 ({report.diff_pct:+.1f}%)"
            if report.diff_yen is not None and report.diff_pct is not None
            else "データ不足"
        )
        lines = [
            f"- product: {m.canonical_id} / エクスプロージョン3kg",
            f"- today: {report.today_price:,}円",
            f"- 前日比: {diff_line}",
            f"- 30日最安: {'更新' if report.is_30d_low else '未更新'}"
            + (f" ({report.min_30d_price:,}円)" if report.min_30d_price is not None else ""),
            f"- level: {report.level}",
            f"- variant: {report.variant} ({report.date_jst} {report.weekday_jst})",
            f"- image: {'採用' if report.image_selected else '未取得'}",
            "",
            "[人別セクション（要約）]",
        ] + report.persona_summary_lines + [
            "",
            "[X投稿案]",
            report.x_text,
            "",
            "[Hatena投稿Markdown案]",
            report.hatena_markdown[:1200],
        ]
        discord_notify("📝 投稿案通知（Rakuten Protein Tracker）", lines)

        print(
            "INFO marketing:",
            f"variant={report.variant}",
            f"date_jst={report.date_jst}",
            f"weekday_jst={report.weekday_jst}",
            f"image_url_status={'採用' if report.image_selected else '未取得'}",
        )

        hatena_result = post_top3_to_hatena(report.hatena_markdown)
        if not hatena_result.ok:
            run_errors.append(
                f"Hatena draft post failed (status={hatena_result.status_code}, endpoint={hatena_result.endpoint}): {hatena_result.message}"
            )

    summary_lines = [
        f"- date: {today}",
        f"- appended rows: {len(all_offers)}",
        f"- change notifications: {len(notify_payloads)}",
        f"- marketing drafts: {len(marketing_reports)}",
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
