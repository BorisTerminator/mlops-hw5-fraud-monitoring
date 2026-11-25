"""Финальный вердикт DQ-прогона."""

import logging

logger = logging.getLogger(__name__)


def check_critical(report_info: dict) -> str:
    """Роняет задачу, если не прошло хотя бы одно critical-правило.

    Читает: результаты правил и путь к уже записанному отчёту. Ничего не пишет.
    Отчёт формируется раньше, поэтому даже красный прогон оставляет HTML с причиной.
    Warning-правила только логируются. Повтор даёт тот же вердикт.
    """
    rules = report_info["rules"]
    critical = [r["id"] for r in rules if not r["passed"] and r["severity"] == "critical"]
    warnings = [r["id"] for r in rules if not r["passed"] and r["severity"] == "warning"]
    if warnings:
        logger.warning("Не прошли warning-правила: %s", warnings)
    if critical:
        raise ValueError(f"Не прошли critical-правила {critical}, см. {report_info['report_path']}")
    return f"OK, отчёт {report_info['report_path']}"
