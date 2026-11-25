# ДЗ №3: переобучение антифрод-модели в Airflow

Сервис скоринга фродовых транзакций из ДЗ-1 (Kafka → CatBoost → Kafka → Postgres), поверх которого Airflow:

- по расписанию дописывает в размеченную витрину новый месяц (`supply_labeled_batch`);
- переобучает модель на скользящем окне и выкатывает её, **только если она не хуже текущей** (`retrain_catboost`);
- проверяет витрину на 10 правил качества данных и пишет HTML-отчёт (`data_quality`).

Сервис скоринга подхватывает новую модель без перезапуска, в каждом скоре есть `model_version`.

## Архитектура

```
                     ┌──────────── Airflow (http://localhost:8081) ──────────────┐
 train.csv ─► data_loader ─► Postgres: labeled_transactions ◄── supply_labeled_batch
                                   │            ▲  training_state (указатель месяца)
                                   │            └── data_quality ──► ./reports/*.html
                                   ▼
                           retrain_catboost ──► ./models/  (my_catboost_<v>.cbm,
                                                            preprocessing_<v>.json, latest.txt)
                                                     │ тот же каталог смонтирован в /models
 transaction_simulator ─► Kafka [transactions] ─► fraud_detector ─► Kafka [scoring] ─► scoring_writer ─► Postgres: scores
```

| Сервис | Что делает |
|---|---|
| `zookeeper`, `kafka`, `kafka-setup` | Брокер; `kafka-setup` создаёт топики `transactions` и `scoring` и завершается |
| `kafka-ui` | Просмотр топиков, http://localhost:8080 |
| `transaction_simulator` | Каждые 0.1 с шлёт в `transactions` случайную строку `train.csv` с новым `transaction_id` и текущим временем |
| `fraud_detector` | Препроцессинг + CatBoost. Перед каждым батчем читает `/models/latest.txt` и при смене версии загружает новую пару «модель + статистики препроцессинга» |
| `scoring_writer` | Топик `scoring` → таблица `scores` (`score`, `fraud_flag`, `model_version`) |
| `postgres` | Витрина `labeled_transactions`, указатель окна `training_state`, результаты `scores` |
| `data_loader` | Один раз загружает `train.csv` в витрину и задаёт стартовое окно |
| `airflow` | Airflow 2.10 standalone с тремя DAG'ами |

Каталоги `./models` и `./reports` смонтированы и в Airflow, и (`./models`) в `fraud_detector`, поэтому опубликованные модели и DQ-отчёты видны прямо в папке проекта.

## Структура

```
├── dags/                         # только сборка графов
│   ├── retrain_catboost.py
│   ├── supply_labeled_batch.py
│   └── data_quality.py
├── pipeline/                     # функции задач, у каждой docstring: что читает, что пишет, почему повтор безопасен
│   ├── settings.py               # все параметры: окно, батч, глубина истории, гейт
│   ├── db.py
│   ├── retrain/                  # build_training_window, time_train_test_split, preprocess_dataset,
│   │                             # fit_catboost, compare_with_production, publish_model, metrics
│   ├── supply/                   # sample_next_month, append_labeled_batch, prune_old_months
│   └── dq/                       # profile_showcase, evaluate_rules, render_report, check_critical
├── fraud_detector/
│   ├── src/preprocessing.py      # препроцессинг: общий для сервиса и DAG
│   ├── src/scorer.py             # версионированный скорер с подхватом latest.txt
│   ├── src/service.py            # Kafka-цикл
│   ├── models/my_catboost.cbm    # стартовая модель из ДЗ-1 (версия initial)
│   └── artifacts/preprocessing.json
├── data_loader/  scoring_writer/  transaction_simulator/  airflow/
├── models/   reports/            # заполняются во время работы
└── docker-compose.yaml
```

## Параметры

Все значения лежат в [pipeline/settings.py](pipeline/settings.py).

| Параметр | Значение | На семинаре |
|---|---|---|
| Окно обучения `WINDOW_MONTHS` | **4 месяца**: 3 в train, 1 в test | 3 |
| Размер батча поставки `SAMPLE_SIZE` | **50 000 строк** | 100 000 |
| Глубина истории витрины `HISTORY_YEARS` | **2 года** | 3 |
| Расписание | `retrain` и `supply` раз в 5 минут, `data_quality` раз в 10 минут, `catchup=False` | — |
| Стартовое окно | январь–апрель 2019, первый прогон обучает на фев–апр и проверяет на мае 2019 | янв–мар |
| Гейт | PR-AUC кандидата ≥ PR-AUC прода на том же месяце (`GATE_TOLERANCE = 0`) | нет |

## Запуск

Нужны Docker Desktop с Compose v2, около 6 ГБ RAM для Docker и свободные порты `8080`, `8081`, `5432`, `9095`.

1. Клонировать репозиторий и положить `train.csv` соревнования
   [teta-ml-1-2025](https://www.kaggle.com/competitions/teta-ml-1-2025/data) в `data/train.csv`
   (786 431 строка, 2019-01…2020-03; в git не лежит, больше 100 МБ).
   Если файл лежит в другом месте, скопируйте `.env.example` в `.env` и укажите путь в `TRAIN_DATA_CSV`.

2. Поднять всё:

   ```bash
   docker compose up -d --build
   ```

   Первый запуск: сборка образов ~5–10 минут, затем `data_loader` около минуты грузит CSV в Postgres.
   Airflow стартует после него, ещё ~1 минута. Состояние:

   ```bash
   docker compose ps
   ```

   `data_loader` и `kafka-setup` должны быть в статусе `Exited (0)`, остальные — `Up`.

## Где что смотреть

| Что | Где |
|---|---|
| Airflow UI | http://localhost:8081, логин `admin`, пароль `admin` |
| Kafka UI | http://localhost:8080 → Topics → `scoring` → Messages |
| Опубликованные модели | папка `./models` |
| DQ-отчёты | `./reports/latest.html` (и история `dq_report_<время>.html`) |
| Postgres | `docker compose exec postgres psql -U fraud_user -d fraud_db` |

## Как проверить

### 1. Скоринг работает на стартовой модели

Сразу после старта `fraud_detector` кладёт модель ДЗ-1 в `./models` как версию `initial`:

```bash
docker compose exec postgres psql -U fraud_user -d fraud_db -c "SELECT model_version, count(*), round(avg(score)::numeric, 4) AS avg_score, sum(fraud_flag) AS frauds FROM scores GROUP BY 1 ORDER BY 1;"
```

Ожидаемо: одна строка `initial` с растущим `count`.

### 2. Запустить DAG переобучения

В Airflow UI откройте `retrain_catboost` → кнопка ▶ **Trigger DAG**. Можно и подождать расписания (раз в 5 минут).
Прогон занимает 1–2 минуты. Во вкладке **Graph** все 6 задач должны стать зелёными:

```
build_training_window → time_train_test_split → preprocess_dataset → fit_catboost → compare_with_production → publish_model
```

То же из консоли:

```bash
docker compose exec airflow airflow dags trigger retrain_catboost
docker compose exec airflow airflow dags list-runs -d retrain_catboost
```

### 3. В каталоге появилась новая модель

```bash
ls models
cat models/latest.txt
```

Появятся `my_catboost_201905_<время>.cbm`, `preprocessing_201905_<время>.json` и `my_catboost_201905_<время>.metrics.json`.
В метриках видно окно, PR-AUC/ROC-AUC/F1 на test-месяце, сравнение с продом и решение гейта (`status: published | rejected`).
Префикс версии — месяц, которым заканчивается окно.

### 4. Новые транзакции скорятся опубликованной версией

Через несколько секунд после публикации повторите запрос из пункта 1: появится строка с новой версией из `latest.txt`,
и её `count` растёт, а у `initial` — нет. Контейнер `fraud_detector` при этом не перезапускался:

```bash
docker compose logs fraud_detector | grep "Загружена модель"
```

Версия есть и в каждом сообщении топика `scoring` (Kafka UI):

```json
{"transaction_id": "…", "score": 0.0123, "fraud_flag": 0, "model_version": "201905_20261022T101500"}
```

### 5. Окно скользит

Каждый следующий прогон берёт следующий месяц:

```bash
docker compose exec postgres psql -U fraud_user -d fraud_db -c "SELECT * FROM training_state;"
```

После первого прогона `window_start=2019-02-01, window_end=2019-05-01, next_batch_month=2019-06-01`.

### 6. Поставка месяца и DQ

- `supply_labeled_batch` → Trigger. После прогона в витрине появляется следующий месяц после последнего (первый — 2020-04) на 50 000 строк:

  ```bash
  docker compose exec postgres psql -U fraud_user -d fraud_db -c "SELECT date_trunc('month', transaction_time)::date AS month, count(*) FROM labeled_transactions GROUP BY 1 ORDER BY 1;"
  ```

- `data_quality` → Trigger, затем откройте `reports/latest.html` в браузере.

### Остановка

```bash
docker compose down        # остановить, данные сохранятся
docker compose down -v     # удалить и тома; для полного сброса очистите и ./models, ./reports
```

## Как устроено переобучение

### Скользящее окно и split по времени

- Окно — 4 календарных месяца, которые заканчиваются месяцем `training_state.next_batch_month`.
  Новый месяц добавляется, самый старый выпадает, длина окна фиксирована.
- `time_train_test_split`: три ранних месяца — train, самый свежий — test. Случайного split нет:
  в проде модель всегда скорит будущее относительно обучающих данных.
- Окно **вычисляется из указателя**, а не хранится файлом, который сдвигается. Указатель меняет только `publish_model`.

### Свой препроцессинг

Не семинарский (haversine + календарь), а перенесённый из ДЗ-1 и сделанный обучаемым
([fraud_detector/src/preprocessing.py](fraud_detector/src/preprocessing.py)):

- категории (`merch`, `cat_id`, `jobs`, `one_city`, `us_state`, `gender`) кодируются рангом частоты, топ-50 + `cat_50+` + `cat_NAN`;
- mean target encoding для категорий и временных признаков (час, день месяца, день недели);
- расстояние клиент–мерчант по большому кругу, пропуски средним, `log(x+1)` для суммы, населения и расстояния;
- `fit_artifacts()` считает все статистики **только по train-месяцам окна**: target test-месяца не утекает в признаки;
- статистики сохраняются рядом с моделью как `preprocessing_<версия>.json`. Сервис грузит модель и её статистики парой,
  поэтому онлайн-признаки совпадают с обучающими;
- год и месяц в новые модели не идут (`DROPPED_FEATURES`): в test-месяце и в потоке их значения всегда новые.

### Повторный запуск не портит данные

- `build_training_window`, `time_train_test_split`, `preprocess_dataset`, `fit_catboost` пишут только в файлы своего месяца
  в томе `training_data`. Повтор перезаписывает те же файлы.
- `fit_catboost` не трогает `./models`: кандидат лежит рядом со сплитом.
- `publish_model`: сначала модель и статистики (через временный файл + `os.replace`), потом `latest.txt`,
  **потом** указатель месяца. Упало раньше — указатель стоит, и следующий прогон заново обучит тот же месяц.
  Указатель записывается абсолютными значениями от `batch_month`, а не «+1», поэтому повтор не сдвинет окно второй раз.
- Нет нового месяца в витрине — задачи `skipped`, а не `failed`.

## Проверка улучшения модели (гейт)

Задача `compare_with_production` между обучением и публикацией.

- **Как считаем.** Кандидат и текущая прод-модель (из `latest.txt`) скорят **одни и те же строки** свежего test-месяца,
  каждая со своими статистиками препроцессинга. Обе этот месяц не видели: кандидат учился на трёх месяцах до него,
  прод — на окне, сдвинутом ещё на месяц назад.
- **Метрика — PR-AUC** (average precision). Фрода ~0.6%: ROC-AUC на таком дисбалансе почти всегда 0.97–0.99 и плохо
  различает модели, а PR-AUC отражает качество на положительном классе. ROC-AUC и F1 при пороге 0.5 тоже считаются и пишутся в метрики.
- **Решение.** `PR-AUC(кандидат) ≥ PR-AUC(прод) − GATE_TOLERANCE` → публикуем: `latest.txt` переключается.
  Иначе модель сохраняется в `./models` со `status: rejected` для истории, `latest.txt` не меняется, прод продолжает скорить старой версией.
- Отклонённый месяц всё равно считается пройденным, иначе окно застряло бы на нём навсегда.
- **Исключение.** Стартовая `initial` (модель ДЗ-1) обучена на всём `train.csv`, включая будущие test-месяцы окна.
  Её метрика завышена утечкой, поэтому против неё гейт не применяется, и первая модель DAG'а публикуется сразу (причина пишется в метрики).

Пример из локального прогона: май-2019 — кандидат PR-AUC 0.668, опубликован (прод был `initial`); июнь-2019 — кандидат 0.661 против прода 0.592 на том же месяце, опубликован.

## Data Quality

DAG `data_quality`: `profile_showcase → evaluate_rules → render_report → check_critical`.
Отчёт — `reports/dq_report_<UTC-время>.html` и копия `reports/latest.html`. Каталог `reports` лежит рядом с `models`.
Если не прошло critical-правило, `check_critical` делает прогон красным. Отчёт к этому моменту уже записан.

| ID | Правило | Важность |
|---|---|---|
| DQ1 | Схема: все 18 колонок витрины на месте | critical |
| DQ2 | Полнота ключевых колонок: 0 NULL в `transaction_time`, `amount`, `target`, `lat`, `lon`, `merchant_lat`, `merchant_lon` | critical |
| DQ3 | Полнота прочих колонок: доля NULL ≤ 1% | warning |
| DQ4 | Домен: `target ∈ {0, 1}` и встречаются оба класса, `gender ∈ {F, M}` | critical |
| DQ5 | Сумма транзакции в (0; 100 000] | critical |
| DQ6 | Координаты клиента и мерчанта: широта в ±90, долгота в ±180 | critical |
| DQ7 | Полных дублей строк ≤ 0.5% | warning |
| DQ8 | В каждом месяце ≥ 10 000 строк: окну обучения хватит данных | critical |
| DQ9 | Доля фрода в каждом месяце в [0.1%; 3%]: разметка не сломалась | warning |
| DQ10 | Глубина истории ≤ 2 лет + 1 месяц: очистка в `supply_labeled_batch` работает | warning |

## Поставка нового месяца

DAG `supply_labeled_batch`: `sample_next_month → append_labeled_batch → prune_old_months`.

- Берёт 50 000 случайных строк витрины, переносит `transaction_time` в месяц после максимального (день, час и минута сохраняются,
  31-е число в коротком месяце обрезается до последнего дня).
- Дозапись идемпотентна: перед `COPY` в той же транзакции удаляются строки этого месяца, ретрай не задвоит батч.
- Удаляет строки старше 2 лет от начала самого нового месяца.
- Указатели у переобучения и поставки разные: `retrain` идёт по историческим месяцам с мая 2019,
  `supply` создаёт месяцы с 2020-04. Окно переобучения дойдёт до синтетических месяцев позже.
