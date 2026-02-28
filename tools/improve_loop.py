#!/usr/bin/env python3
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import requests

GITHUB_API_BASE = "https://api.github.com"
TRIAGE_LABEL = "triage"
QUEUED_LABEL = "queued"
DONE_LABEL = "done"
BLOCKED_LABEL = "blocked"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@dataclass
class ScoreCard:
    impact: int
    effort: int
    risk: int
    measurable: int

    @property
    def total(self) -> int:
        return self.impact + self.effort + self.risk + self.measurable


@dataclass
class Evaluation:
    issue: Dict
    score: ScoreCard
    blocked: bool
    missing_info: List[str]
    questions: List[str]


class GitHubClient:
    def __init__(self, repository: str, token: str):
        self.repository = repository
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {token}",
                "X-GitHub-Api-Version": "2022-11-28",
            }
        )

    def list_open_triage_issues(self) -> List[Dict]:
        url = f"{GITHUB_API_BASE}/repos/{self.repository}/issues"
        params = {
            "state": "open",
            "labels": TRIAGE_LABEL,
            "sort": "created",
            "direction": "asc",
            "per_page": 100,
        }
        resp = self.session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        issues = resp.json()
        return [i for i in issues if "pull_request" not in i]

    def add_labels(self, issue_number: int, labels: List[str]) -> None:
        if not labels:
            return
        url = f"{GITHUB_API_BASE}/repos/{self.repository}/issues/{issue_number}/labels"
        resp = self.session.post(url, json={"labels": labels}, timeout=30)
        resp.raise_for_status()

    def create_comment(self, issue_number: int, body: str) -> None:
        url = f"{GITHUB_API_BASE}/repos/{self.repository}/issues/{issue_number}/comments"
        resp = self.session.post(url, json={"body": body}, timeout=30)
        resp.raise_for_status()


def clamp_score(n: int) -> int:
    return max(1, min(5, n))


def contains_any(text: str, words: List[str]) -> bool:
    lowered = text.lower()
    return any(w in lowered for w in words)


def parse_first_line_expectation(body: str) -> Tuple[bool, Optional[str]]:
    first_line = (body or "").splitlines()[0].strip() if body else ""
    if "→" in first_line:
        return True, first_line
    return False, first_line or None


def score_issue(issue: Dict) -> Evaluation:
    title = issue.get("title", "")
    body = issue.get("body", "") or ""
    full_text = f"{title}\n{body}"

    ok_format, first_line = parse_first_line_expectation(body)
    missing_info: List[str] = []
    questions: List[str] = []
    blocked = False

    if not ok_format:
        blocked = True
        missing_info.append("Issue本文の1行目が「症状 → 期待」の形式になっていません。")
        questions.append("1行目を『症状 → 期待』の形式で記載してください。")

    impact = 3
    if contains_any(full_text, ["cv", "cvr", "売上", "収益", "購入", "cta", "離脱", "コンバージョン"]):
        impact += 1
    if contains_any(full_text, ["致命", "大きい", "全ユーザー", "モバイル", "タップしづらい"]):
        impact += 1

    effort = 3
    if contains_any(full_text, ["文言", "css", "レイアウト", "配置", "表示", "markdown"]):
        effort += 1
    if contains_any(full_text, ["全面", "設計変更", "DB", "マイグレーション", "複数画面"]):
        effort -= 2

    risk = 3
    if contains_any(full_text, ["文言", "css", "表示", "markdown", "小修正"]):
        risk += 1
    if contains_any(full_text, ["決済", "認証", "在庫", "計算", "検索ロジック"]):
        risk -= 2

    measurable = 3
    if contains_any(full_text, ["クリック", "ctr", "cvr", "タップ", "確認", "再現", "比較"]):
        measurable += 1
    if contains_any(full_text, ["なんとなく", "違和感", "気がする"]):
        measurable -= 1

    score = ScoreCard(
        impact=clamp_score(impact),
        effort=clamp_score(effort),
        risk=clamp_score(risk),
        measurable=clamp_score(measurable),
    )

    if not body.strip():
        blocked = True
        missing_info.append("Issue本文が空です。")
        questions.append("再現条件・対象ページ・期待動作を本文に追記してください。")

    if first_line and "→" not in first_line:
        questions.append("1行目に『症状 → 期待』を追記してください。")

    return Evaluation(
        issue=issue,
        score=score,
        blocked=blocked,
        missing_info=missing_info,
        questions=questions,
    )


def build_codex_prompt(evaluation: Evaluation) -> str:
    issue = evaluation.issue
    title = issue.get("title", "")
    body = issue.get("body", "") or ""
    first_line = body.splitlines()[0].strip() if body else ""

    acceptance = first_line if "→" in first_line else "症状を解消し、期待結果を満たすこと"

    return "\n".join(
        [
            "あなたはこのリポジトリ（protein-hunter）の実装担当です。",
            f"対象Issue: #{issue.get('number')} {title}",
            "",
            "【背景（現状の問題）】",
            f"{first_line or 'Issue本文を参照し、現状の問題を具体化してください。'}",
            "",
            "【期待する挙動（Acceptance Criteria）】",
            f"- {acceptance}",
            "- 既存機能を壊さず、該当箇所のみ最小変更で修正すること",
            "",
            "【影響範囲（安全に進めるための制約）】",
            "- 主に Markdown / CSS / 文言修正を優先すること",
            "- ロジック改修が必要な場合は影響範囲を明示し、最小変更で実施すること",
            "",
            "【実装方針】",
            "- まずIssueの症状に対応する関数・テンプレート・出力箇所を探索する",
            "- 関連する生成テキスト（記事本文、CTA、通知文）の差分を確認して修正する",
            "- 必要に応じてテストや検証スクリプトを更新する",
            "",
            "【変更後の確認方法】",
            "- はてな投稿向けMarkdownを生成して見た目崩れがないことを確認する",
            "- Discord通知文面に必要情報が出ることを確認する",
            "- 実行ログにエラーがないことを確認する",
            "",
            "Issue詳細:",
            body if body.strip() else "（Issue本文なし）",
        ]
    )


def post_discord(webhook_url: str, content: str) -> None:
    resp = requests.post(webhook_url, json={"content": content[:1900]}, timeout=30)
    resp.raise_for_status()


def build_checklist(blocked: bool) -> List[str]:
    if blocked:
        return [
            "Issue 1行目を『症状 → 期待』形式で追記する",
            "再現手順と対象ページを明記する",
            "完了条件（確認観点）を3点以内で追加する",
        ]
    return [
        "Issue本文の受け入れ条件を満たす",
        "影響範囲を最小化して実装する",
        "はてな向けMarkdownの表示崩れがないことを確認する",
        "Discord通知とログにエラーがないことを確認する",
        "PRに変更点・確認手順を簡潔に記載する",
    ]


def build_discord_message(evaluation: Optional[Evaluation], prompt: str = "") -> str:
    if evaluation is None:
        return "🟢 improve-loop: triageラベルの未処理Issueはありません。"

    issue = evaluation.issue
    score = evaluation.score
    checklist = build_checklist(evaluation.blocked)

    lines = [
        "🛠️ improve-loop 評価結果",
        f"採用Issue: #{issue.get('number')} {issue.get('title', '')}",
        f"スコア: Impact={score.impact} / Effort={score.effort} / Risk={score.risk} / Measurable={score.measurable} / Total={score.total}",
    ]

    if evaluation.blocked:
        lines.append("状態: BLOCKED（情報不足）")
        if evaluation.missing_info:
            lines.append("不足情報:")
            lines.extend([f"- {m}" for m in evaluation.missing_info])
        if evaluation.questions:
            lines.append("質問:")
            lines.extend([f"- {q}" for q in evaluation.questions[:3]])
    else:
        lines.extend(
            [
                "Codex向けプロンプト:",
                f"```\n{prompt}\n```",
            ]
        )

    lines.append("チェックリスト:")
    lines.extend([f"- [ ] {item}" for item in checklist[:5]])

    return "\n".join(lines)


def main() -> None:
    repository = os.getenv("GITHUB_REPOSITORY")
    token = os.getenv("GITHUB_TOKEN")
    webhook_url = os.getenv("DISCORD_IMPROVE_WEBHOOK_URL")

    if not repository or not token:
        raise RuntimeError("GITHUB_REPOSITORY と GITHUB_TOKEN が必要です。")

    gh = GitHubClient(repository=repository, token=token)
    issues = gh.list_open_triage_issues()

    if not issues:
        if webhook_url:
            post_discord(webhook_url, build_discord_message(None))
        print("No triage issues found.")
        return

    evaluations = [score_issue(issue) for issue in issues]
    non_blocked = [e for e in evaluations if not e.blocked]

    if non_blocked:
        selected = sorted(non_blocked, key=lambda e: e.score.total, reverse=True)[0]
        prompt = build_codex_prompt(selected)
        message = build_discord_message(selected, prompt)

        issue_number = selected.issue["number"]
        gh.add_labels(issue_number, [QUEUED_LABEL])
        gh.create_comment(
            issue_number,
            f"Last evaluated at: {utc_now_iso()}\n\nStatus: queued\n"
            f"Score: Impact={selected.score.impact}, Effort={selected.score.effort}, "
            f"Risk={selected.score.risk}, Measurable={selected.score.measurable}, Total={selected.score.total}",
        )
    else:
        selected = evaluations[0]
        message = build_discord_message(selected)
        issue_number = selected.issue["number"]
        gh.add_labels(issue_number, [BLOCKED_LABEL])
        gh.create_comment(
            issue_number,
            f"Last evaluated at: {utc_now_iso()}\n\nStatus: blocked\n"
            "不足情報があるため着手を保留しました。\n"
            + "\n".join(f"- {q}" for q in selected.questions[:3]),
        )

    if webhook_url:
        post_discord(webhook_url, message)

    print(f"Evaluated {len(evaluations)} issue(s). Selected #{selected.issue['number']}")


if __name__ == "__main__":
    main()
