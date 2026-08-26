# app-ads.txt Scraper — архитектурное решение

> **TL;DR:** два независимых пайплайна (weekly store discovery + daily fetch) поверх одного PostgreSQL; диспетчеризация через `FOR UPDATE SKIP LOCKED` без выделенного брокера; дедупликация по каноническим IAB fetch targets; Go-воркеры в Kubernetes; Redis — общий rate-limit и circuit breaker. Все объёмные допущения измеряются на PoC.

Сервис получает десятки миллионов `(store, Bundle ID)`, еженедельно обновляет данные приложения и developer URL в App Store / Google Play, а найденные `app-ads.txt` проверяет ежедневно.

## 1. Нагрузка и основные принципы

Допущение для расчёта: **30 млн приложений** — 12 млн App Store и 18 млн Google Play.

| Поток | Объём | Средняя скорость |
|---|---:|---:|
| Weekly store discovery | 30 млн/неделю | ~50 приложений/с |
| Daily app-ads.txt fetch | до ~1 млн уникальных targets/сутки | ~12 targets/с |

> **Гипотезы, а не факты:** количество доменов, доля изменяющихся файлов, размеры ответов и поддержка `ETag`/`Last-Modified` измеряются на PoC. Неизменившийся файл даст `304`, только если origin корректно поддерживает conditional requests.

**Главная оптимизация — канонические fetch targets.** Приложения дедуплицируются не просто по registrable domain: по правилам IAB из developer URL сохраняется ближайший значимый subdomain (префиксы `www.` и `m.` удаляются), и результат канонизации — упорядоченная цепочка, общая для всех приложений с тем же developer URL:

```text
another.games.example.com  →  games.example.com  →  example.com
       developer URL             primary host        fallback root
```

**Дедупликация действует на обоих уровнях цепочки.** Каждый физический хост, включая fallback-корни, — одна строка `hosts` со своим состоянием, разделяемая всеми цепочками, которые на него ссылаются. `games.example.com` и `music.example.com` падают на общий `example.com`, но сам `example.com/app-ads.txt` фетчится не чаще раза в сутки — иначе крупный издатель с десятками субдоменов получал бы десятки одинаковых запросов, ломая и дедупликацию, и politeness.

## 2. Архитектура

```text
Bundle IDs ──► PostgreSQL ◄────────────────────────────────────┐
                  │                                            │
       due apps   │                                due targets │
                  ▼                                            ▼
          Store Workers                                 Fetch Workers
     source adapters + limiter                   conditional GET + parser
       │          │          │                           │
   bulk/feed  Apple lookup  store HTML                    ├─► file_versions
       │          fallback / degradation                  ├─► entries/directives
       └──────── publisher + developer URL ───────────────┘

Redis: общий rate-limit/circuit-breaker для реплик и источников.
```

**`Bundle IDs → PostgreSQL` — не отдельный ingest-пайплайн.** Это первичный bulk-импорт (`COPY`) и дальнейшая инкрементальная синхронизация с исходной базой экосистемы: таблица `apps` (30 млн строк ≈ 6–10 ГБ с индексами) одновременно является состоянием планировщика — `next_*_at`, lease и статусы живут на ней, отдельного хранилища задач нет.

**Пайплайны изолированы.** Проблемы стора или поставщика метаданных не останавливают ежедневную проверку уже известных targets, а ошибки сайтов издателей не расходуют store quota.

### Store discovery и стратегия источников

**Доступ к сторам скрыт за интерфейсом `StoreSource`** — источник меняется без изменения бизнес-логики:

| Стор | Приоритет источников |
|---|---|
| App Store | разрешённый bulk / partner / licensed feed → Lookup API → HTML fallback |
| Google Play | официальный / лицензированный источник → HTML parser с versioned fixtures и контролем изменений разметки |

**Ёмкость важнее лимитов.** Мы не считаем опубликованный API limit контрактом и не предполагаем стабильность API. Требуемая ёмкость источника проверяется формулой `dataset_size / 7 days`: для 12 млн iOS-приложений это около **20 результатов/с**. Если источник не держит её на PoC, нужен bulk/provider или другой разрешённый источник — token bucket сам по себе пропускную способность не создаёт.

Ограничение запросов состоит из:

- **локального concurrency limit** в Go;
- **иерархического Redis token bucket** `source → credential → egress` — in-process bucket в каждом pod умножил бы общий RPS при HPA; запрос получает токен на каждом уровне;
- **конфигурируемой скорости** без зашитой в код цифры и небольшого burst, чтобы не создавать короткие антибот-пики;
- **управляемой деградации** — реакция на `Retry-After`, `429`, `403` и рост `5xx`: уменьшение скорости, jitter, cooldown/circuit breaker, постепенное восстановление.

*Несколько credentials или egress IP используются только если это разрешено условиями источника, а не для обхода его ограничений.*

### Диспетчеризация без отдельного брокера

**Источник истины — `next_*_at` в Postgres.** Воркеры конкурентно забирают due rows через `FOR UPDATE SKIP LOCKED`; порядок и exactly-once не требуются, операции идемпотентны. Для 5,3 млн задач/сутки это **~61 задача/с** и при batch 500 около **0,12 claim-запроса/с**.

```sql
WITH picked AS (
  SELECT id FROM hosts
  WHERE status = 'active'
    AND next_fetch_at <= now()
    AND (locked_until IS NULL OR locked_until < now())
  ORDER BY next_fetch_at
  LIMIT 500
  FOR UPDATE SKIP LOCKED
)
UPDATE hosts h
SET locked_until = now() + interval '10 min', lock_token = gen_random_uuid()
FROM picked
WHERE h.id = picked.id
RETURNING h.*;
```

- **`lock_token`** — завершение обновляет строку только при совпадении токена, чтобы опоздавший воркер не перезаписал более свежий результат;
- **lease** — batch выбирается так, чтобы завершиться до истечения `locked_until`; при необходимости lease продлевается;
- **частичные индексы** — `(next_store_check_at) WHERE status = 'active'` и `(next_fetch_at) WHERE status = 'active'`.

### Fetch, parse и хранение

**Fetcher**: `GET https://{host}/app-ads.txt` с conditional headers, timeout 10 с, ответ ≤ 1 МБ, контролируемые redirects. Состояние условных запросов (`ETag`/`Last-Modified`/hash) живёт на каждом физическом URL, а не на цепочке: если сегодня отвечал primary, а завтра ушли в fallback — отправляются заголовки именно этого URL. Fetcher уважает `robots.txt` издателя для нашего user-agent; доля заблокированных хостов — отдельная метрика.

**SSRF-защита, минимально достаточная**: только публичные IP и порты 80/443, повторная проверка каждого redirect, запрет loopback/private/link-local адресов; имя резолвится один раз, соединение идёт по уже проверенному IP (Host/SNI — именем), что закрывает DNS rebinding. Отдельный sandbox-прокси для этого дизайна не нужен.

**Версионируются только валидные файлы.** При изменении hash запускается parser; корректный результат транзакционно заменяет snapshot `entries` (исчезнувшие строки должны исчезнуть и из текущего среза), и только для него сохраняется raw gzip-версия.

> Невалидный ответ **не версионируется**: заглушки и parked-страницы — динамический HTML, их hash меняется каждым fetch-ом, и хранение таких «версий» превратило бы историю в мусорный поток гигабайтами в сутки. Для невалидных фиксируются только HTTP-код, `parse_status` и причина; payload — лишь при смене класса ошибки. Последний корректный snapshot никогда не затирается.

**Парсер** поддерживает seller records и `OWNERDOMAIN`, `MANAGERDOMAIN`, `CONTACT`. Директива `SUBDOMAIN` в app-ads.txt по IAB не используется и игнорируется; fallback с primary host на registrable root определяется developer URL, а не содержимым файла.

**Хранение**: при ожидаемом росте в десятки ГБ/год raw gzip живёт в помесячно партиционированном Postgres. Решение пересматривается в пользу S3/MinIO после измерений — если история растёт до сотен ГБ–ТБ или нужна внешним аналитическим системам.

## 3. Данные и надёжность

| Таблица | Содержимое |
|---|---|
| `apps` | store, bundle ID, store app ID/storefront, publisher, developer URL, ссылка на fetch target, status, due/attempt/success timestamps, lease |
| `fetch_targets` | канонизированная цепочка `primary host → fallback host`, общая для приложений с одинаковым developer URL |
| `hosts` | один физический хост = одна строка (включая разделяемые fallback-корни): ETag/Last-Modified/hash, due/attempt/success/change timestamps, lease |
| `file_versions` | raw gzip **только валидных** версий, hash, HTTP/parse status, observed time |
| `entries`, `directives` | текущий корректный snapshot; обратный индекс `(ad_system, account_id)` — «какие домены авторизовали account X» |
| `app_events` | append-only история смены publisher, URL, domain и статуса |

**Потребители** читают `entries`/`directives` и `app_events` через read-only API или экспорт; scraper — писатель, выдача — отдельная задача поверх этих таблиц.

**Retries и мёртвые хосты.** После трёх retries с exponential backoff и jitter задача остаётся в следующем daily/weekly цикле — намеренно увеличивать период до семи дней нельзя, это нарушает условие задания. Стабильно мёртвые хосты (NXDOMAIN, connection refused N циклов подряд) проверяются не реже, а **дешевле**: негативный DNS-кэш и укороченный timeout — суточный период сохраняется, но бюджет на них минимален. Удалённые приложения продолжают weekly-check с соответствующим статусом; осиротевший target исключается из daily-цикла, только когда на него больше не ссылается ни одно отслеживаемое приложение.

**Метрики и SLO**: successful/failed fetch, `304` ratio, `429/403/5xx` по источникам, oldest due task, p95/p99 latency, freshness. `last_attempt_at`, `last_success_at`, `last_changed_at` разделены; никогда успешно не проверенная запись считается нарушением SLO. Цели: **daily freshness ≤ 36 ч**, **weekly ≤ 9 дней**.

## 4. Этапы реализации

| Этап | Срок | Скоуп и критерий выхода |
|---|---|---|
| **PoC** | 1 неделя | 10–50 тыс. приложений; измерить доступность metadata, устойчивую скорость каждого source adapter, процент developer URL / app-ads.txt / 304, размеры файлов, ошибки, proxy/provider cost |
| **MVP** | 2–3 недели | до 1 млн приложений; Postgres claims, IAB canonicalization и fallback, conditional fetch, parser/versioning, Redis limiter, dashboard, retries |
| **Scale-out** | 2–3 недели | 30 млн; горизонтальные Go workers в Kubernetes, HPA по oldest due/backlog; подтверждение daily/weekly SLO и стоимости на полном объёме |
| **Hardening** | — | алерты, data-quality проверки, резервный metadata source и контролируемое переключение при деградации основного |
