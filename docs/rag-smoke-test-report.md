# RAG スモークテスト結果まとめ

- 実行日時: 2025-10-03 (JST)
- 実行コマンド:
  - `export AWS_PROFILE=239339588912_AdministratorAccess`
  - `UV_CACHE_DIR=.uv-cache uv run -m s3_vectors_rag_hands_on.chatbot`
- 目的: `data/input` に格納した 5 件のドキュメントが、ナレッジベース経由で正しく検索・引用されることを検証する。

## シナリオ別確認結果

### 1. No filter overview
- クエリ: "Give me a broad overview of Aurora Dynamics as described in our knowledge base."
- フィルタ: なし
- 主な引用: `aurora_company_profile.pdf` ほかプレスリリース/カタログ/セキュリティ資料
- 判定: ✔ データ全体を横断した総合サマリーが生成され、複数ドキュメントが引用された。

### 2. Domain filter
- クエリ: "Summarize the public description of Aurora Dynamics."
- フィルタ: `equals(domain, "auroradynamics.com")`
- 主な引用: `aurora_company_profile.pdf`
- 判定: ✔ 指定ドメインの公開資料のみから回答が生成された。

### 3. Internal security docs
- クエリ: "What security governance guidance is available?"
- フィルタ: `equals(is_internal, true)` AND `equals(tags, "security,governance")`
- 主な引用: `aurora_security_brief.pdf`
- 判定: ✔ 社内向けセキュリティ資料のみがヒットし、想定どおりの回答が返った。

### 4. Press announcement
- クエリ: "What recent announcement did Aurora make?"
- フィルタ: `equals(is_internal, false)` AND `equals(tags, "press,announcement")` AND `greaterThanOrEquals(published_at, 1750000000)`
- 主な引用: `aurora_press_announcement.txt`
- 判定: ✔ 公開プレスアナウンスのみを対象に、日付条件を満たす内容が返された。

### 5. Metrics spotlight
- クエリ: "Share key operational metrics for Aurora Dynamics."
- フィルタ: `equals(is_internal, true)` AND `equals(tags, "metrics,operations")`
- 主な引用: `aurora_operational_metrics.xlsx`
- 判定: ✔ メトリクス資料から数値指標が返され、他ドキュメントは引用されなかった。

### 6. Catalog lookup
- クエリ: "List the solution offerings Aurora provides to customers."
- フィルタ: `equals(tags, "services,catalog")`
- 主な引用: `aurora_solution_catalog.docx`
- 判定: ✔ ソリューションカタログのみが参照され、想定の 3 つのモジュラー提供内容を回答。

## 所感
- すべてのシナリオで `retrieve_and_generate` が 200 応答を返し、引用ドキュメントとフィルタ条件の整合性が確認できた。
- S3 Vectors では `stringContains` や HYBRID 検索が未対応だったため、`equals`/`greaterThanOrEquals` ベースに調整している。
- 今後フィルタ項目が増えた場合は `chatbot.py` 内の `base_filters` にシナリオを追記するだけで同様の検証が行える。
