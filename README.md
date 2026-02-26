# protein-hunter

## 0件時の考え方

このジョブは楽天APIから取得した商品候補に対して、除外・重複排除・容量一致などのフィルタを適用してから `Price_History` に追記します。

- `STRICT_MODE=true` の場合:
  - append対象行数が `0` ならジョブを失敗終了します。
  - 監視を厳格にしたい（=0件は異常とみなす）運用向けです。
- `STRICT_MODE` が未設定/`false` の場合:
  - append対象行数が `0` でもジョブは成功扱いです。
  - ただし警告ログを出力し、調査可能にします。

## デバッグ手順（fetched/appended が 0 のとき）

1. **取得フェーズログを確認**
   - `DEBUG fetch` に `api_total_count`（API上のヒット件数）と `fetched_items`（実際に取得した件数）が出ます。
   - `api_total_count > 0` なのに `fetched_items = 0` の場合は、ページング・レスポンス構造・API制限を疑います。

2. **フィルタ内訳ログを確認**
   - `DEBUG filter` の `drop_counts` で、各段階の落ち件数が見えます。
   - 主なキー:
     - `missing_required_or_invalid_price`
     - `excluded_keyword`
     - `capacity_mismatch`
     - `invalid_offer`
     - `duplicate`
     - `store_hits_limit`

3. **書き込み直前の件数を確認**
   - `DEBUG append: rows_to_append=N` が append直前の行数です。
   - `N=0` の場合は、上記 `DEBUG filter` のどこで落ちたかを追います。

4. **STRICT_MODE で失敗化して検知強化（必要時）**
   - CI/Workflowで `STRICT_MODE=true` を設定すると、0件を失敗として即検知できます。

## 主要な環境変数

- `RAKUTEN_APP_ID`
- `RAKUTEN_AFFILIATE_ID`（任意）
- `SHEET_ID`
- `GSPREAD_SERVICE_ACCOUNT_JSON_B64`
- `STRICT_MODE`（任意、`true/false`）
- `HATENA_ID`（任意、はてなブログBasic認証ユーザー）
- `HATENA_API_KEY`（任意、はてなブログAPIキー）
- `HATENA_BLOG_ID`（任意、ブログドメイン。例: `protain-hunter.hatenablog.com`）

## はてなブログ下書き投稿

ジョブ完了後、各 `canonical_id` の当日最安オファーを実質コストで並べ、TOP3ランキング記事を下書きで投稿します。

- Service Document: `https://blog.hatena.ne.jp/${HATENA_ID}/${HATENA_BLOG_ID}/atom`
- POST先: `https://blog.hatena.ne.jp/${HATENA_ID}/${HATENA_BLOG_ID}/atom/entry`
- タイトル形式: `【プロテイン価格ランキング】YYYY-MM-DD`
- 本文: TOP3ランキングMarkdown
- 認証: Basic認証（`HATENA_ID` + `HATENA_API_KEY`）
- 失敗時: エラーログを出力し、既存の価格収集処理は継続

### 設定例

```env
HATENA_ID=naoki1978
HATENA_BLOG_ID=protain-hunter.hatenablog.com
# HATENA_API_KEY=xxxxxxxx
```


## 収益最大化モード（エクスプロージョン3kg専用）

このジョブは `Master_List` から **エクスプロージョン3kg** のみを対象にして、毎回以下を実施します。

- 前日比（円・％）を算出
- 過去30日最安を判定
- 価格変動レベルを3段階で分類
  - `normal`
  - `drop`（-3% or -300円）
  - `big_drop`（-5% or -500円 or 30日最安）
- レベル別CTAテンプレートを生成
- X投稿文案を自動生成
- はてな投稿用Markdownを自動生成
- Discordに投稿案を通知

過度な煽りは避けつつ、価格下落時の緊急性を明確に出す設計です。
