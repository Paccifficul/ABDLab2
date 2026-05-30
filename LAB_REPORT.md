# Отчет по лабораторной работе N2

## Тема

Анализ больших данных. ETL-пайплайн на Apache Spark: преобразование исходных данных в DWH-модель в PostgreSQL и построение отчетных витрин в NoSQL БД.

## Реализация

В работе реализованы обязательные пункты лабораторной:

- исходные CSV загружаются в PostgreSQL в таблицу `mock_data`;
- Spark-приложение на C# преобразует данные из `mock_data` в модель звезда/снежинка в PostgreSQL;
- Spark-приложение строит 6 отчетов из DWH-модели;
- каждый отчет сохраняется отдельной таблицей в ClickHouse;
- дополнительно те же 6 отчетов сохраняются отдельными таблицами в Cassandra.

## Используемые технологии

- C# / .NET 8 и Microsoft.Spark - код Spark ETL.
- Apache Spark 3.1.2 - выполнение распределенных преобразований.
- PostgreSQL 17 - источник данных и DWH-хранилище.
- ClickHouse 24.3 - обязательная аналитическая NoSQL БД для отчетных витрин.
- Cassandra 4.1 - дополнительная NoSQL БД для тех же отчетных витрин.
- Docker Compose - запуск всей инфраструктуры.

## Структура проекта

- `docker-compose.yml` - сервисы PostgreSQL, ClickHouse, Cassandra, и одноразовый Spark ETL.
- `init.sql` - создание `mock_data` и загрузка 10 CSV-файлов.
- `cassandra/init.cql` - создание keyspace `reports` и 6 таблиц Cassandra.
- `src/ABDLab2.Etl/Program.cs` - Spark.NET ETL-приложение на C#.
- `src/ABDLab2.Etl/submit.sh` - запуск `spark-submit`.
- `исходные данные` - исходные CSV-файлы.

## Архитектура ETL

1. PostgreSQL при старте контейнера выполняет `init.sql` и загружает 10000 строк в `mock_data`.
2. Контейнер `spark-etl` запускает `spark-submit`.
3. Spark через JDBC читает `mock_data` из PostgreSQL.
4. Spark создает DWH-таблицы в PostgreSQL:
   `dim_country`, `dim_city`, `dim_location`, `dim_pet_type`, `dim_pet_breed`, `dim_pet`, `dim_category`, `dim_pet_category`, `dim_brand`, `dim_date`, `dim_customer`, `dim_seller`, `dim_store`, `dim_supplier`, `dim_product`, `fact_sales`.
5. Spark читает DWH-таблицы обратно как временные представления.
6. Spark формирует 6 отчетных DataFrame.
7. Эти DataFrame записываются в ClickHouse и Cassandra.

## Реализованные отчеты

- `report_product_sales` - продажи по продуктам: количество, выручка, рейтинг, отзывы, ранг продаж.
- `report_customer_sales` - продажи по клиентам: сумма покупок, средний чек, страна, ранг клиента.
- `report_time_sales` - продажи по времени: месячная выручка, средний заказ, накопительная выручка.
- `report_store_sales` - продажи по магазинам: выручка, средний чек, город, страна, ранг магазина.
- `report_supplier_sales` - продажи по поставщикам: выручка, средняя цена товара, страна, ранг поставщика.
- `report_product_quality` - качество продукции: идентификатор товара, рейтинг, отзывы, продажи, корреляция рейтинга и продаж.

## Зачем нужна Cassandra

Cassandra полезна не как замена ClickHouse для произвольной аналитики, а как wide-column хранилище под заранее известные запросы. Поэтому таблицы Cassandra спроектированы вокруг сценариев чтения: например, продажи продуктов читаются по `category_name`, продажи магазинов - по `country`, продажи поставщиков - по `supplier_country`. Это демонстрирует важное отличие Cassandra: сначала проектируется запрос, затем под него выбирается partition key и clustering key.

## Подключения (в целях безопасности данные подключения к БД заменены)

PostgreSQL:

- Host с хоста: `localhost`
- Port: `5555`
- Database: `abd_lab2`
- User: `postgres`
- Password: `secret`

ClickHouse:

- HTTP: `localhost:8123`
- Native: `localhost:9000`
- Database: `reports`
- User: `student`
- Password: `student`

Cassandra:

- Host с хоста: `localhost`
- Port: `9042`
- Keyspace: `reports`

## Запуск

Из папки `ABDLab2`:

```powershell
docker compose up --build
```

Для чистого перезапуска с удалением старых данных:

```powershell
docker compose down -v --remove-orphans
docker compose up --build
```

Контейнер `spark-etl` является одноразовой Spark-задачей. После успешного выполнения он завершается с кодом `0`; PostgreSQL, ClickHouse и Cassandra остаются запущенными для проверки.

## Проверка логов Spark

```powershell
docker compose logs spark-etl
```

В логах должны быть строки:

```text
ABDLab2 Spark ETL started
PostgreSQL source rows: 10000
Report report_product_sales: ...
Report report_customer_sales: ...
Report report_time_sales: ...
Report report_store_sales: ...
Report report_supplier_sales: ...
Report report_product_quality: ...
PostgreSQL fact rows: 10000
ABDLab2 Spark ETL finished
```

## Проверка ClickHouse

Подключение к консоли ClickHouse:

```powershell
docker compose exec clickhouse clickhouse-client --user student --password student --database reports
```

Проверить список таблиц:

```sql
SHOW TABLES;
```

Ожидаемые таблицы:

```text
report_customer_sales
report_product_quality
report_product_sales
report_store_sales
report_supplier_sales
report_time_sales
```

Проверить количество строк во всех витринах:

```sql
SELECT 'report_product_sales' AS table_name, count() AS rows FROM report_product_sales
UNION ALL
SELECT 'report_customer_sales', count() FROM report_customer_sales
UNION ALL
SELECT 'report_time_sales', count() FROM report_time_sales
UNION ALL
SELECT 'report_store_sales', count() FROM report_store_sales
UNION ALL
SELECT 'report_supplier_sales', count() FROM report_supplier_sales
UNION ALL
SELECT 'report_product_quality', count() FROM report_product_quality;
```

Во всех таблицах количество строк должно быть больше `0`.

Примеры аналитических запросов:

```sql
SELECT *
FROM report_product_sales
ORDER BY total_quantity_sold DESC
LIMIT 10;
```

```sql
SELECT *
FROM report_customer_sales
ORDER BY total_spent DESC
LIMIT 10;
```

```sql
SELECT *
FROM report_time_sales
ORDER BY year, month;
```

Быстрая проверка ClickHouse через HTTP из PowerShell:

```powershell
Invoke-RestMethod "http://student:student@localhost:8123/?database=reports&query=SHOW%20TABLES"
```

```powershell
Invoke-RestMethod "http://student:student@localhost:8123/?database=reports&query=SELECT%20count()%20FROM%20report_product_sales"
```

## Проверка Cassandra

Подключение к Cassandra:

```powershell
docker compose exec cassandra cqlsh
```

Проверить keyspace и таблицы:

```sql
DESCRIBE KEYSPACES;
USE reports;
DESCRIBE TABLES;
```

Ожидаемые таблицы такие же, как в ClickHouse:

```text
report_customer_sales
report_product_quality
report_product_sales
report_store_sales
report_supplier_sales
report_time_sales
```

Проверить количество строк:

```sql
SELECT COUNT(*) FROM report_product_sales;
SELECT COUNT(*) FROM report_customer_sales;
SELECT COUNT(*) FROM report_time_sales;
SELECT COUNT(*) FROM report_store_sales;
SELECT COUNT(*) FROM report_supplier_sales;
SELECT COUNT(*) FROM report_product_quality;
```

Примеры запросов с учетом модели Cassandra:

```sql
SELECT *
FROM report_customer_sales
LIMIT 10;
```

```sql
SELECT *
FROM report_time_sales
WHERE year = 2023;
```

```sql
SELECT *
FROM report_product_sales
WHERE category_name = 'Food'
LIMIT 10;
```

В Cassandra эффективные запросы обычно должны использовать partition key. Для `report_product_sales` это `category_name`, для `report_time_sales` - `year`, для `report_store_sales` - `country`, для `report_supplier_sales` - `supplier_country`. Запросы без partition key возможны только для небольших лабораторных данных или с `ALLOW FILTERING`, но в реальных проектах так делать не стоит.

## Проверка PostgreSQL

Подключение:

```powershell
docker compose exec postgres psql -U postgres -d abd_lab2 -p 5555
```

Проверить исходные и фактовые строки:

```sql
SELECT COUNT(*) AS mock_data_rows FROM mock_data;
SELECT COUNT(*) AS fact_sales_rows FROM fact_sales;
```

Ожидаемо:

- `mock_data_rows = 10000`;
- `fact_sales_rows = 10000`.

Проверить, что факты связаны с измерениями:

```sql
SELECT COUNT(*) AS broken_fk_rows
FROM fact_sales f
LEFT JOIN dim_date d ON d.date_id = f.date_id
LEFT JOIN dim_customer c ON c.customer_id = f.customer_id
LEFT JOIN dim_seller s ON s.seller_id = f.seller_id
LEFT JOIN dim_product p ON p.product_id = f.product_id
LEFT JOIN dim_store st ON st.store_id = f.store_id
LEFT JOIN dim_supplier sp ON sp.supplier_id = f.supplier_id
WHERE d.date_id IS NULL
   OR c.customer_id IS NULL
   OR s.seller_id IS NULL
   OR p.product_id IS NULL
   OR st.store_id IS NULL
   OR sp.supplier_id IS NULL;
```

Ожидаемо: `broken_fk_rows = 0`.

## Итог

Главный критерий успешной сдачи: Spark-задача завершается с кодом `0`, в PostgreSQL создана DWH-модель, а в ClickHouse и Cassandra есть все 6 отчетных таблиц с заполненными данными.
