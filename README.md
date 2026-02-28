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


## 収益最大化モード（Master_List全商品対応）

このジョブは `Master_List` の**全canonical_id**を対象に、毎回以下を実施します。

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

## はてな記事テンプレ仕様（自動下書き）

収益最大化のため、はてな下書き本文は以下の固定テンプレで生成します（公開は手動のまま）。

- 冒頭3行要約（結論→数値→CTA誘導）
  1. 判定（A/B文言に連動）+ 短い理由
  2. 実質価格（円/kg）+ 前日比（円・％）+ 30日最安フラグ
  3. CTA誘導文（「下のボタンから確認」）
- ヒーロー
  - H1: `<商品名> 価格速報（YYYY-MM-DD）`
  - 判定1行（A/B切替）
  - 価格サマリ3点（今日最安 / 前日比 / 30日最安）
- 大きめ商品画像
  - ヒーロー直下に `![商品画像](画像URL)` を配置
  - 画像が取れない場合は「商品画像はリンク先で確認」を表示
- 読みやすいブロック構造
  - 今日の結論
  - 価格データ
  - 買い時コメント
  - CTA
  - 注意書き
- 商品名は40文字で短縮表示（超過時は `…`）

## CTA仕様（生URL禁止）

- 本文内リンクはCTA内の1つだけに集約。
- 本文中に生URL文字列は出力しない。
- CTAは崩れにくい2段構成のMarkdownを採用:

```md
### ✅ 今すぐ確認
**👉 [楽天で価格と在庫を確認する](AFF_URL)**
```

## A/B切替仕様（JST）

`choose_variant_jst()` で `Asia/Tokyo` の曜日判定を行い、以下を自動切替します。

- 月・水・金: A
  - 判定文言: `今日が買い時`
  - 背中押し: `補充する人は今日が安全。ポイント条件だけ確認してGO。`
  - 要約理由: `30日最安水準`
- 火・木・土・日: B
  - 判定文言: `逃すと損しやすい水準`
  - 背中押し: `この水準は長く続かないことが多い。売り切れ前に確認。`
  - 要約理由: `急落後は戻りやすい`

A/Bで変更するのは本文の次の2箇所のみです。

1. ヒーロー直下の判定文言（1行）
2. CTA直前の背中押し文（1〜2行）

実行ログには `variant`, `date_jst`, `weekday_jst` を出力します。

## 画像取得fallback仕様

Rakuten APIレスポンスから商品画像URLを次の優先順位で取得します。

1. `mediumImageUrls[0].imageUrl`
2. `smallImageUrls[0].imageUrl`

取得可否は実行ログに `image_url_status=採用/未取得` として出力します。
