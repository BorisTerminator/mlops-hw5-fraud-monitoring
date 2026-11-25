# Проверка локального стенда

Проверено 25 ноября 2026 на Windows, Docker Desktop (Linux/WSL2).

## Что подтверждено

- Все 3 источника Prometheus доступны: scorer, writer, Node Exporter.
- Provisioning создал 4 дашборда, Prometheus и PostgreSQL datasources, 2 здоровых правила Grafana.
- Все запросы панелей выполнялись через Grafana API и вернули непустые данные. Скрипт также проверил рост счётчика на интервале 3 секунды.
- В браузере просмотрены все четыре дашборда. В Business выбор CA, затем конкретного мерчанта сократил число результатов с нескольких тысяч до одного; ошибок SQL не было.
- 35 тестов прошли и на хосте, и в Linux-контейнере Python 3.11. Проверены сохранение контекста, повреждённые сообщения, безопасный reload, счётчики, повторная вставка, rollback, TIMESTAMPTZ, read-only роль, SQL-фильтрация и последние 1000 строк.
- Реальный Airflow DAG обучил и опубликовал версию `201905_20261125T120518`, скорер загрузил её вместо `initial`, в БД появились обе версии. Время старта скорера оставалось `2026-11-25T12:03:31.058508163Z` — рестарта для обновления не было. Запрос аннотации дал 30 точек по секунде для новой версии.
- Остановка генератора привела к Firing алерта stream stalled; восстановление потока вернуло inactive/Normal.
- Искусственный высокий fraud ratio привёл к Firing второго алерта, после возврата порога 0.5 алерт восстановился. Этот эксперимент отдельно пересоздаёт скорер; он не используется как доказательство hot reload.

## Машинные доказательства

- [acceptance.json](acceptance.json) — последний прогон запросов всех панелей, источников и роста счётчика.
- [model-reload-evidence.json](model-reload-evidence.json) — версии, старт контейнера, счётчик загрузок и длительность аннотаций.
- [alert-stall-evidence.json](alert-stall-evidence.json) — наблюдения остановки потока, включая Firing.
- [demo-high-fraud.json](demo-high-fraud.json) — Firing и восстановление второго алерта.

Статус `inactive` в Prometheus-compatible API Grafana соответствует нормальному состоянию; интерфейс Alerting показывает Normal.

Это результаты конкретного запуска, не обещание одинакового TPS/latency на другой машине. Повторить: запустить стенд по README, затем `python scripts/verify_stack.py`, `python -m pytest -q`, `python scripts/demo.py stall`, `python scripts/demo.py high-fraud`.

Финальная независимая проверка дополнена регрессиями на некорректные даты/ID/числа, отсутствие latest.txt, неполный preprocessing, синхронную ошибку публикации, точный результат publish_model конкретного DAG run и проверку свежих commit/строк БД. Права Grafana сужены до scores.

Последняя проверка на запущенных контейнерах: три некорректных входных сообщения увеличили input errors на 3, два некорректных результата увеличили writer input errors на 2, следующее правильное сообщение сохранилось, оба процесса остались с прежним временем старта и RestartCount=0. При блокировке INSERT в PostgreSQL (SELECT и экспортер оставались доступны) проверка acceptance корректно остановилась на отсутствии роста written. См. [live-regression-evidence.json](live-regression-evidence.json).

При отсутствии телеметрии отдельно проверено: stream stalled — Firing, high flagged fraction — Normal. См. [telemetry-outage-evidence.json](telemetry-outage-evidence.json).
