"""HTML-отчёт DQ в каталог reports рядом с каталогом моделей."""

import html
import logging
import os
from datetime import datetime, timezone

from pipeline.settings import REPORTS_DIR

logger = logging.getLogger(__name__)

STYLE = """
body { font-family: system-ui, sans-serif; margin: 24px; color: #1f2328; background: #fff; }
table { border-collapse: collapse; margin: 12px 0 24px; }
th, td { border: 1px solid #d0d7de; padding: 6px 10px; text-align: left; font-size: 14px; }
th { background: #f6f8fa; }
.pass { color: #1a7f37; font-weight: 600; }
.fail-critical { color: #cf222e; font-weight: 600; }
.fail-warning { color: #9a6700; font-weight: 600; }
"""


def status_cell(r: dict) -> str:
    if r["passed"]:
        return '<td class="pass">PASS</td>'
    css = "fail-critical" if r["severity"] == "critical" else "fail-warning"
    return f'<td class="{css}">FAIL</td>'


def render_report(dq: dict) -> dict:
    """Пишет отчёт reports/dq_report_<UTC-время>.html и обновляет reports/latest.html.

    Читает: результаты evaluate_rules. Пишет только в REPORTS_DIR.
    Каждый прогон — новый файл с уникальным именем, latest.html заменяется
    атомарно через os.replace. Повтор задачи создаст ещё один отчёт и ничего не сломает.
    """
    rules, profile = dq["rules"], dq["profile"]
    now = datetime.now(timezone.utc)
    failed = [r for r in rules if not r["passed"]]

    rule_rows = "\n".join(
        f'<tr><td>{r["id"]}</td><td>{html.escape(r["name"])}</td><td>{r["severity"]}</td>'
        f'{status_cell(r)}<td>{html.escape(str(r["expected"]))}</td><td>{html.escape(str(r["observed"]))}</td></tr>'
        for r in rules
    )
    month_rows = "\n".join(
        f'<tr><td>{m["month"]}</td><td>{m["rows"]}</td><td>{m["frauds"]}</td>'
        f'<td>{m["frauds"] / m["rows"]:.3%}</td></tr>'
        for m in profile["months"]
    )
    page = f"""<!doctype html>
<html lang="ru"><head><meta charset="utf-8"><title>DQ-отчёт витрины</title><style>{STYLE}</style></head>
<body>
<h1>DQ-отчёт: labeled_transactions</h1>
<p>Сформирован {now:%Y-%m-%d %H:%M:%S} UTC. Строк в витрине: {profile["rows"]}, месяцев: {len(profile["months"])}.
Правил: {len(rules)}, прошло: {len(rules) - len(failed)}, не прошло: {len(failed)}.</p>
<h2>Правила</h2>
<table><tr><th>ID</th><th>Правило</th><th>Важность</th><th>Статус</th><th>Ожидание</th><th>Факт</th></tr>
{rule_rows}
</table>
<h2>Строки и фрод по месяцам</h2>
<table><tr><th>Месяц</th><th>Строк</th><th>Фрод</th><th>Доля фрода</th></tr>
{month_rows}
</table>
</body></html>
"""
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report_path = REPORTS_DIR / f"dq_report_{now:%Y%m%dT%H%M%S}.html"
    report_path.write_text(page, encoding="utf-8")
    latest_tmp = REPORTS_DIR / ".latest.html.tmp"
    latest_tmp.write_text(page, encoding="utf-8")
    os.replace(latest_tmp, REPORTS_DIR / "latest.html")
    logger.info("DQ-отчёт: %s", report_path)
    return {"report_path": str(report_path), "rules": rules}
