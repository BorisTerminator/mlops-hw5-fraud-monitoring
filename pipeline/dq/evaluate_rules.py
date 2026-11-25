"""Правила качества данных витрины. Их же описывает README."""

import logging

from pipeline.db import LABELED_COLUMNS
from pipeline.dq.profile_showcase import CRITICAL_COLUMNS
from pipeline.settings import HISTORY_YEARS

logger = logging.getLogger(__name__)

MAX_NULL_SHARE = 0.01
MAX_DUPLICATE_SHARE = 0.005
MIN_MONTH_ROWS = 10_000
FRAUD_RATE_RANGE = (0.001, 0.03)


def rule(rule_id, name, severity, passed, expected, observed):
    return {
        "id": rule_id,
        "name": name,
        "severity": severity,
        "passed": bool(passed),
        "expected": expected,
        "observed": observed,
    }


def evaluate_rules(profile: dict) -> dict:
    """Проверяет профиль витрины на 9 правил DQ.

    Читает: профиль из profile_showcase. Пишет: ничего, результаты уходят через XCom.
    severity=critical — без выполнения правила обучать модель нельзя;
    warning — стоит посмотреть, но данные пригодны.
    Чистая функция от профиля: повтор даёт тот же результат.
    """
    rows = max(profile["rows"], 1)
    months = profile["months"]

    missing_cols = sorted(set(LABELED_COLUMNS) - set(profile["columns"]))
    critical_nulls = {c: n for c, n in profile["nulls"].items() if c in CRITICAL_COLUMNS and n > 0}
    other_nulls = {
        c: round(n / rows, 4) for c, n in profile["nulls"].items()
        if c not in CRITICAL_COLUMNS and n / rows > MAX_NULL_SHARE
    }
    small_months = [m["month"] for m in months if m["rows"] < MIN_MONTH_ROWS]
    odd_fraud = [
        f'{m["month"]}: {m["frauds"] / m["rows"]:.4f}' for m in months
        if not FRAUD_RATE_RANGE[0] <= m["frauds"] / m["rows"] <= FRAUD_RATE_RANGE[1]
    ]
    span = 0
    if months:
        first_y, first_m = map(int, months[0]["month"].split("-"))
        last_y, last_m = map(int, months[-1]["month"].split("-"))
        span = (last_y - first_y) * 12 + last_m - first_m + 1

    rules = [
        rule("DQ1", "Схема: все колонки витрины на месте", "critical",
             not missing_cols, f"{len(LABELED_COLUMNS)} колонок", f"нет: {missing_cols}" if missing_cols else "все на месте"),
        rule("DQ2", "Полнота ключевых колонок (время, сумма, target, координаты)", "critical",
             not critical_nulls, "0 NULL", critical_nulls or "0 NULL"),
        rule("DQ3", f"Полнота прочих колонок: доля NULL ≤ {MAX_NULL_SHARE:.0%}", "warning",
             not other_nulls, f"≤ {MAX_NULL_SHARE:.0%}", other_nulls or "в норме"),
        rule("DQ4", "Домен значений: target ∈ {0, 1}, gender ∈ {F, M}", "critical",
             set(profile["targets"]) <= {0, 1} and len(profile["targets"]) == 2 and set(profile["genders"]) <= {"F", "M"},
             "target {0,1}, gender {F,M}", f'target {profile["targets"]}, gender {profile["genders"]}'),
        rule("DQ5", "Сумма транзакции в диапазоне (0; 100 000]", "critical",
             profile["amount_out_of_range"] == 0, "0 строк вне диапазона",
             f'{profile["amount_out_of_range"]} строк, min {profile["amount_min"]}, max {profile["amount_max"]}'),
        rule("DQ6", "Координаты клиента и мерчанта: широта ±90, долгота ±180", "critical",
             profile["geo_out_of_range"] == 0, "0 строк вне диапазона", f'{profile["geo_out_of_range"]} строк'),
        rule("DQ7", f"Уникальность: доля полных дублей ≤ {MAX_DUPLICATE_SHARE:.1%}", "warning",
             profile["duplicates"] / rows <= MAX_DUPLICATE_SHARE, f"≤ {MAX_DUPLICATE_SHARE:.1%}",
             f'{profile["duplicates"]} строк ({profile["duplicates"] / rows:.3%})'),
        rule("DQ8", f"Объём: в каждом месяце ≥ {MIN_MONTH_ROWS:,} строк".replace(",", " "), "critical",
             bool(months) and not small_months, f"≥ {MIN_MONTH_ROWS}", small_months or f"{len(months)} месяцев в норме"),
        rule("DQ9", f"Доля фрода в каждом месяце в [{FRAUD_RATE_RANGE[0]:.1%}; {FRAUD_RATE_RANGE[1]:.0%}]", "warning",
             not odd_fraud, f"[{FRAUD_RATE_RANGE[0]:.1%}; {FRAUD_RATE_RANGE[1]:.0%}]", odd_fraud or "в норме"),
        rule("DQ10", f"Глубина истории ≤ {HISTORY_YEARS} лет: очистка витрины работает", "warning",
             span <= HISTORY_YEARS * 12 + 1, f"≤ {HISTORY_YEARS * 12 + 1} мес", f"{span} мес"),
    ]
    failed = [r["id"] for r in rules if not r["passed"]]
    logger.info("DQ: %s правил, не прошли %s", len(rules), failed or "нет")
    return {"profile": profile, "rules": rules}
