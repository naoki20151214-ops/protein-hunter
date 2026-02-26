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
