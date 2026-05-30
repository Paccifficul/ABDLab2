using System.Net.Http.Headers;
using Microsoft.Spark.Sql;

var pgHost = Env("POSTGRES_HOST", "postgres");
var pgPort = Env("POSTGRES_PORT", "5555");
var pgDb = Env("POSTGRES_DB", "abd_lab2");
var pgUser = Env("POSTGRES_USER", "postgres");
var pgPassword = Env("POSTGRES_PASSWORD", "secret");
var pgUrl = $"jdbc:postgresql://{pgHost}:{pgPort}/{pgDb}";

var clickHouseUrl = Env("CLICKHOUSE_JDBC_URL", "jdbc:clickhouse://clickhouse:8123/reports");
var clickHouseHttpUrl = Env("CLICKHOUSE_HTTP_URL", "http://clickhouse:8123/?database=reports&user=student&password=student");
var clickHouseUser = Env("CLICKHOUSE_USER", "student");
var clickHousePassword = Env("CLICKHOUSE_PASSWORD", "student");
var cassandraKeyspace = Env("CASSANDRA_KEYSPACE", "reports");

var reportTables = new[]
{
    "report_product_sales",
    "report_customer_sales",
    "report_time_sales",
    "report_store_sales",
    "report_supplier_sales",
    "report_product_quality"
};

var spark = SparkSession
    .Builder()
    .AppName("ABDLab2-SparkNET-ETL")
    .Config("spark.sql.adaptive.enabled", "true")
    .GetOrCreate();

try
{
    Console.WriteLine("ABDLab2 Spark ETL started");

    var rawData = ReadPostgres(spark, pgUrl, pgUser, pgPassword, "mock_data");
    rawData.CreateOrReplaceTempView("mock_data");
    Console.WriteLine($"PostgreSQL source rows: {rawData.Count()}");

    BuildWarehouse(spark, pgUrl, pgUser, pgPassword);
    LoadWarehouseViews(spark, pgUrl, pgUser, pgPassword);

    var reports = BuildReports(spark);
    await PrepareClickHouseTablesAsync(clickHouseHttpUrl);

    foreach (var (table, frame) in reports)
    {
        frame.Cache();
        var rows = frame.Count();
        WriteClickHouse(frame, clickHouseUrl, clickHouseUser, clickHousePassword, table);
        WriteCassandra(frame, cassandraKeyspace, table);
        Console.WriteLine($"Report {table}: {rows}");
        frame.Unpersist();
    }

    Console.WriteLine($"PostgreSQL fact rows: {ReadPostgres(spark, pgUrl, pgUser, pgPassword, "fact_sales").Count()}");
    Console.WriteLine("ABDLab2 Spark ETL finished");
}
finally
{
    spark.Stop();
}

static string Env(string name, string fallback) => Environment.GetEnvironmentVariable(name) ?? fallback;

static DataFrame ReadPostgres(SparkSession spark, string url, string user, string password, string table)
{
    return spark.Read()
        .Format("jdbc")
        .Option("url", url)
        .Option("dbtable", table)
        .Option("user", user)
        .Option("password", password)
        .Option("driver", "org.postgresql.Driver")
        .Load();
}

static void WritePostgres(DataFrame frame, string url, string user, string password, string table)
{
    frame.Write()
        .Mode(SaveMode.Overwrite)
        .Format("jdbc")
        .Option("url", url)
        .Option("dbtable", table)
        .Option("user", user)
        .Option("password", password)
        .Option("driver", "org.postgresql.Driver")
        .Save();
}

static void WriteClickHouse(DataFrame frame, string url, string user, string password, string table)
{
    frame.Write()
        .Mode(SaveMode.Append)
        .Format("jdbc")
        .Option("url", url)
        .Option("dbtable", table)
        .Option("user", user)
        .Option("password", password)
        .Option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .Save();
}

static void WriteCassandra(DataFrame frame, string keyspace, string table)
{
    frame.Write()
        .Mode(SaveMode.Append)
        .Format("org.apache.spark.sql.cassandra")
        .Option("keyspace", keyspace)
        .Option("table", table)
        .Save();
}

static DataFrame SqlWriteView(SparkSession spark, string sql, string view, string pgUrl, string pgUser, string pgPassword)
{
    var frame = spark.Sql(sql);
    frame.CreateOrReplaceTempView(view);
    WritePostgres(frame, pgUrl, pgUser, pgPassword, view);
    return frame;
}

static void BuildWarehouse(SparkSession spark, string pgUrl, string pgUser, string pgPassword)
{
    SqlWriteView(spark, @"
        SELECT CAST(row_number() OVER (ORDER BY country) AS INT) AS country_id, country
        FROM (
            SELECT DISTINCT country
            FROM (
                SELECT customer_country AS country FROM mock_data
                UNION SELECT seller_country FROM mock_data
                UNION SELECT store_country FROM mock_data
                UNION SELECT supplier_country FROM mock_data
            ) s
            WHERE country IS NOT NULL AND country <> ''
        ) t", "dim_country", pgUrl, pgUser, pgPassword);

    SqlWriteView(spark, @"
        SELECT CAST(row_number() OVER (ORDER BY city, state) AS INT) AS city_id, city, state
        FROM (
            SELECT DISTINCT city, state
            FROM (
                SELECT store_city AS city, store_state AS state FROM mock_data
                UNION SELECT supplier_city, CAST(NULL AS STRING) AS state FROM mock_data
            ) s
            WHERE city IS NOT NULL AND city <> ''
        ) t", "dim_city", pgUrl, pgUser, pgPassword);

    SqlWriteView(spark, @"
        SELECT CAST(row_number() OVER (ORDER BY country_id, city_id, postal_code, address) AS INT) AS location_id,
               country_id, city_id, postal_code, address
        FROM (
            SELECT DISTINCT dc.country_id, CAST(NULL AS INT) AS city_id, m.customer_postal_code AS postal_code, CAST(NULL AS STRING) AS address
            FROM mock_data m JOIN dim_country dc ON m.customer_country = dc.country
            UNION
            SELECT DISTINCT dc.country_id, CAST(NULL AS INT) AS city_id, m.seller_postal_code AS postal_code, CAST(NULL AS STRING) AS address
            FROM mock_data m JOIN dim_country dc ON m.seller_country = dc.country
            UNION
            SELECT DISTINCT dc.country_id, dci.city_id, CAST(NULL AS STRING) AS postal_code, m.store_location AS address
            FROM mock_data m
            JOIN dim_country dc ON m.store_country = dc.country
            JOIN dim_city dci ON m.store_city = dci.city AND (m.store_state = dci.state OR (m.store_state IS NULL AND dci.state IS NULL))
            UNION
            SELECT DISTINCT dc.country_id, dci.city_id, CAST(NULL AS STRING) AS postal_code, m.supplier_address AS address
            FROM mock_data m
            JOIN dim_country dc ON m.supplier_country = dc.country
            JOIN dim_city dci ON m.supplier_city = dci.city
        ) t", "dim_location", pgUrl, pgUser, pgPassword);

    SqlWriteView(spark, @"
        SELECT CAST(row_number() OVER (ORDER BY type) AS INT) AS pet_type_id, type
        FROM (SELECT DISTINCT customer_pet_type AS type FROM mock_data WHERE customer_pet_type IS NOT NULL AND customer_pet_type <> '') t", "dim_pet_type", pgUrl, pgUser, pgPassword);

    SqlWriteView(spark, @"
        SELECT CAST(row_number() OVER (ORDER BY breed) AS INT) AS pet_breed_id, breed
        FROM (SELECT DISTINCT customer_pet_breed AS breed FROM mock_data WHERE customer_pet_breed IS NOT NULL AND customer_pet_breed <> '') t", "dim_pet_breed", pgUrl, pgUser, pgPassword);

    SqlWriteView(spark, @"
        SELECT CAST(row_number() OVER (ORDER BY pet_type_id, pet_breed_id) AS INT) AS pet_id, pet_type_id, pet_breed_id
        FROM (
            SELECT DISTINCT dpt.pet_type_id, dpb.pet_breed_id
            FROM mock_data m
            JOIN dim_pet_type dpt ON m.customer_pet_type = dpt.type
            JOIN dim_pet_breed dpb ON m.customer_pet_breed = dpb.breed
        ) t", "dim_pet", pgUrl, pgUser, pgPassword);

    SqlWriteView(spark, @"
        SELECT CAST(row_number() OVER (ORDER BY category_name) AS INT) AS category_id, category_name
        FROM (SELECT DISTINCT product_category AS category_name FROM mock_data WHERE product_category IS NOT NULL AND product_category <> '') t", "dim_category", pgUrl, pgUser, pgPassword);

    SqlWriteView(spark, @"
        SELECT CAST(row_number() OVER (ORDER BY category_name) AS INT) AS pet_category_id, category_name
        FROM (SELECT DISTINCT pet_category AS category_name FROM mock_data WHERE pet_category IS NOT NULL AND pet_category <> '') t", "dim_pet_category", pgUrl, pgUser, pgPassword);

    SqlWriteView(spark, @"
        SELECT CAST(row_number() OVER (ORDER BY brand_name) AS INT) AS brand_id, brand_name
        FROM (SELECT DISTINCT product_brand AS brand_name FROM mock_data WHERE product_brand IS NOT NULL AND product_brand <> '') t", "dim_brand", pgUrl, pgUser, pgPassword);

    SqlWriteView(spark, @"
        SELECT CAST(row_number() OVER (ORDER BY full_date) AS INT) AS date_id,
               full_date,
               year(full_date) AS year,
               month(full_date) AS month,
               dayofweek(full_date) AS day_of_week
        FROM (
            SELECT DISTINCT full_date
            FROM (
                SELECT to_date(sale_date, 'M/d/yyyy') AS full_date FROM mock_data
                UNION SELECT to_date(product_release_date, 'M/d/yyyy') FROM mock_data
                UNION SELECT to_date(product_expiry_date, 'M/d/yyyy') FROM mock_data
            ) s
            WHERE full_date IS NOT NULL
        ) t", "dim_date", pgUrl, pgUser, pgPassword);

    SqlWriteView(spark, @"
        SELECT CAST(sale_customer_id AS INT) AS customer_id, first_name, last_name, age, email, location_id, pet_id, pet_name
        FROM (
            SELECT m.sale_customer_id, m.customer_first_name AS first_name, m.customer_last_name AS last_name,
                   m.customer_age AS age, m.customer_email AS email, dl.location_id, dp.pet_id,
                   m.customer_pet_name AS pet_name, row_number() OVER (PARTITION BY m.sale_customer_id ORDER BY m.id) AS rn
            FROM mock_data m
            JOIN dim_country dc ON m.customer_country = dc.country
            JOIN dim_location dl ON dl.country_id = dc.country_id AND dl.city_id IS NULL
                AND (dl.postal_code = m.customer_postal_code OR (dl.postal_code IS NULL AND coalesce(m.customer_postal_code, '') = ''))
            JOIN dim_pet_type dpt ON m.customer_pet_type = dpt.type
            JOIN dim_pet_breed dpb ON m.customer_pet_breed = dpb.breed
            JOIN dim_pet dp ON dp.pet_type_id = dpt.pet_type_id AND dp.pet_breed_id = dpb.pet_breed_id
        ) t WHERE rn = 1", "dim_customer", pgUrl, pgUser, pgPassword);

    SqlWriteView(spark, @"
        SELECT CAST(sale_seller_id AS INT) AS seller_id, first_name, last_name, email, location_id
        FROM (
            SELECT m.sale_seller_id, m.seller_first_name AS first_name, m.seller_last_name AS last_name,
                   m.seller_email AS email, dl.location_id, row_number() OVER (PARTITION BY m.sale_seller_id ORDER BY m.id) AS rn
            FROM mock_data m
            JOIN dim_country dc ON m.seller_country = dc.country
            JOIN dim_location dl ON dl.country_id = dc.country_id AND dl.city_id IS NULL
                AND (dl.postal_code = m.seller_postal_code OR (dl.postal_code IS NULL AND coalesce(m.seller_postal_code, '') = ''))
        ) t WHERE rn = 1", "dim_seller", pgUrl, pgUser, pgPassword);

    SqlWriteView(spark, @"
        SELECT CAST(row_number() OVER (ORDER BY store_name, email) AS INT) AS store_id, store_name, location_id, phone, email
        FROM (
            SELECT m.store_name, dl.location_id, m.store_phone AS phone, m.store_email AS email,
                   row_number() OVER (PARTITION BY m.store_name, m.store_email ORDER BY m.id) AS rn
            FROM mock_data m
            JOIN dim_country dc ON m.store_country = dc.country
            JOIN dim_city dci ON m.store_city = dci.city AND (m.store_state = dci.state OR (m.store_state IS NULL AND dci.state IS NULL))
            JOIN dim_location dl ON dl.country_id = dc.country_id AND dl.city_id = dci.city_id
        ) t WHERE rn = 1", "dim_store", pgUrl, pgUser, pgPassword);

    SqlWriteView(spark, @"
        SELECT CAST(row_number() OVER (ORDER BY name, email) AS INT) AS supplier_id, name, contact, email, phone, location_id
        FROM (
            SELECT m.supplier_name AS name, m.supplier_contact AS contact, m.supplier_email AS email,
                   m.supplier_phone AS phone, dl.location_id,
                   row_number() OVER (PARTITION BY m.supplier_name, m.supplier_email ORDER BY m.id) AS rn
            FROM mock_data m
            JOIN dim_country dc ON m.supplier_country = dc.country
            JOIN dim_city dci ON m.supplier_city = dci.city
            JOIN dim_location dl ON dl.country_id = dc.country_id AND dl.city_id = dci.city_id
        ) t WHERE rn = 1", "dim_supplier", pgUrl, pgUser, pgPassword);

    SqlWriteView(spark, @"
        SELECT CAST(sale_product_id AS INT) AS product_id,
               product_name, category_id, pet_category_id, brand_id, price, quantity, weight,
               color, size, material, description, rating, reviews, release_date_id, expiry_date_id
        FROM (
            SELECT m.sale_product_id, m.product_name, dc.category_id, dpc.pet_category_id, db.brand_id,
                   CAST(m.product_price AS DOUBLE) AS price, m.product_quantity AS quantity,
                   CAST(m.product_weight AS DOUBLE) AS weight, m.product_color AS color,
                   m.product_size AS size, m.product_material AS material, m.product_description AS description,
                   CAST(m.product_rating AS DOUBLE) AS rating, m.product_reviews AS reviews,
                   ddr.date_id AS release_date_id, dde.date_id AS expiry_date_id,
                   row_number() OVER (PARTITION BY m.sale_product_id ORDER BY m.id) AS rn
            FROM mock_data m
            JOIN dim_category dc ON m.product_category = dc.category_name
            JOIN dim_pet_category dpc ON m.pet_category = dpc.category_name
            JOIN dim_brand db ON m.product_brand = db.brand_name
            JOIN dim_date ddr ON ddr.full_date = to_date(m.product_release_date, 'M/d/yyyy')
            JOIN dim_date dde ON dde.full_date = to_date(m.product_expiry_date, 'M/d/yyyy')
        ) t WHERE rn = 1", "dim_product", pgUrl, pgUser, pgPassword);

    SqlWriteView(spark, @"
        SELECT CAST(row_number() OVER (ORDER BY m.id, m.sale_date, m.sale_customer_id, m.sale_seller_id, m.product_name) AS BIGINT) AS sale_id,
               dd.date_id, dc.customer_id, ds.seller_id, dp.product_id, dst.store_id, dsu.supplier_id,
               CAST(m.sale_quantity AS INT) AS quantity, CAST(m.sale_total_price AS DOUBLE) AS total_price
        FROM mock_data m
        JOIN dim_date dd ON dd.full_date = to_date(m.sale_date, 'M/d/yyyy')
        JOIN dim_customer dc ON dc.customer_id = m.sale_customer_id
        JOIN dim_seller ds ON ds.seller_id = m.sale_seller_id
        JOIN dim_product dp ON dp.product_id = m.sale_product_id
        JOIN dim_store dst ON dst.store_name = m.store_name AND dst.email = m.store_email
        JOIN dim_supplier dsu ON dsu.name = m.supplier_name AND dsu.email = m.supplier_email", "fact_sales", pgUrl, pgUser, pgPassword);
}

static void LoadWarehouseViews(SparkSession spark, string pgUrl, string pgUser, string pgPassword)
{
    foreach (var table in new[] { "dim_country", "dim_city", "dim_location", "dim_pet_type", "dim_pet_breed", "dim_pet", "dim_category", "dim_pet_category", "dim_brand", "dim_date", "dim_customer", "dim_seller", "dim_store", "dim_supplier", "dim_product", "fact_sales" })
    {
        ReadPostgres(spark, pgUrl, pgUser, pgPassword, table).CreateOrReplaceTempView(table);
    }
}

static Dictionary<string, DataFrame> BuildReports(SparkSession spark)
{
    return new Dictionary<string, DataFrame>
    {
        ["report_product_sales"] = spark.Sql(@"
            SELECT product_name, category_name, total_quantity_sold, total_revenue, avg_rating, reviews_count,
                   CAST(rank() OVER (ORDER BY total_quantity_sold DESC) AS INT) AS sales_rank
            FROM (
                SELECT p.product_name, cat.category_name,
                       CAST(sum(f.quantity) AS BIGINT) AS total_quantity_sold,
                       sum(f.total_price) AS total_revenue,
                       avg(p.rating) AS avg_rating,
                       CAST(max(p.reviews) AS BIGINT) AS reviews_count
                FROM fact_sales f
                JOIN dim_product p ON f.product_id = p.product_id
                JOIN dim_category cat ON p.category_id = cat.category_id
                GROUP BY p.product_name, cat.category_name
            ) t"),
        ["report_customer_sales"] = spark.Sql(@"
            SELECT CAST(customer_id AS INT) AS customer_id, first_name, last_name, email, country,
                   orders_count, total_spent, avg_check,
                   CAST(rank() OVER (ORDER BY total_spent DESC) AS INT) AS spending_rank
            FROM (
                SELECT c.customer_id, c.first_name, c.last_name, c.email, co.country,
                       CAST(count(f.sale_id) AS BIGINT) AS orders_count,
                       sum(f.total_price) AS total_spent,
                       avg(f.total_price) AS avg_check
                FROM fact_sales f
                JOIN dim_customer c ON f.customer_id = c.customer_id
                JOIN dim_location l ON c.location_id = l.location_id
                JOIN dim_country co ON l.country_id = co.country_id
                GROUP BY c.customer_id, c.first_name, c.last_name, c.email, co.country
            ) t"),
        ["report_time_sales"] = spark.Sql(@"
            SELECT year, month, orders_count, total_revenue, avg_order_size,
                   sum(total_revenue) OVER (PARTITION BY year ORDER BY month) AS cumulative_revenue
            FROM (
                SELECT d.year, d.month,
                       CAST(count(f.sale_id) AS BIGINT) AS orders_count,
                       sum(f.total_price) AS total_revenue,
                       avg(f.total_price) AS avg_order_size
                FROM fact_sales f
                JOIN dim_date d ON f.date_id = d.date_id
                GROUP BY d.year, d.month
            ) t"),
        ["report_store_sales"] = spark.Sql(@"
            SELECT store_name, coalesce(city, 'Unknown') AS city, country,
                   orders_count, total_revenue, avg_check,
                   CAST(rank() OVER (ORDER BY total_revenue DESC) AS INT) AS revenue_rank
            FROM (
                SELECT s.store_name, ci.city, co.country,
                       CAST(count(f.sale_id) AS BIGINT) AS orders_count,
                       sum(f.total_price) AS total_revenue,
                       avg(f.total_price) AS avg_check
                FROM fact_sales f
                JOIN dim_store s ON f.store_id = s.store_id
                JOIN dim_location l ON s.location_id = l.location_id
                LEFT JOIN dim_city ci ON l.city_id = ci.city_id
                JOIN dim_country co ON l.country_id = co.country_id
                GROUP BY s.store_name, ci.city, co.country
            ) t"),
        ["report_supplier_sales"] = spark.Sql(@"
            SELECT supplier_name, supplier_country, orders_count, total_revenue, avg_product_price,
                   CAST(rank() OVER (ORDER BY total_revenue DESC) AS INT) AS revenue_rank
            FROM (
                SELECT su.name AS supplier_name, co.country AS supplier_country,
                       CAST(count(f.sale_id) AS BIGINT) AS orders_count,
                       sum(f.total_price) AS total_revenue,
                       avg(p.price) AS avg_product_price
                FROM fact_sales f
                JOIN dim_supplier su ON f.supplier_id = su.supplier_id
                JOIN dim_product p ON f.product_id = p.product_id
                JOIN dim_location l ON su.location_id = l.location_id
                JOIN dim_country co ON l.country_id = co.country_id
                GROUP BY su.name, co.country
            ) t"),
        ["report_product_quality"] = spark.Sql(@"
            SELECT CAST(p.product_id AS INT) AS product_id,
                   p.product_name, cat.category_name,
                   p.rating,
                   CAST(p.reviews AS BIGINT) AS reviews_count,
                   CAST(sum(f.quantity) AS BIGINT) AS total_sold,
                   sum(f.total_price) AS total_revenue,
                   coalesce(corr(p.rating, CAST(f.quantity AS DOUBLE)), 0.0) AS rating_sales_corr
            FROM fact_sales f
            JOIN dim_product p ON f.product_id = p.product_id
            JOIN dim_category cat ON p.category_id = cat.category_id
            GROUP BY p.product_id, p.product_name, cat.category_name, p.rating, p.reviews")
    };
}

static async Task PrepareClickHouseTablesAsync(string clickHouseUrl)
{
    var statements = new[]
    {
        "CREATE TABLE IF NOT EXISTS report_product_sales (product_name String, category_name String, total_quantity_sold Int64, total_revenue Float64, avg_rating Float64, reviews_count Int64, sales_rank Int32) ENGINE = MergeTree ORDER BY (sales_rank, product_name)",
        "CREATE TABLE IF NOT EXISTS report_customer_sales (customer_id Int32, first_name String, last_name String, email String, country String, orders_count Int64, total_spent Float64, avg_check Float64, spending_rank Int32) ENGINE = MergeTree ORDER BY (spending_rank, customer_id)",
        "CREATE TABLE IF NOT EXISTS report_time_sales (year Int32, month Int32, orders_count Int64, total_revenue Float64, avg_order_size Float64, cumulative_revenue Float64) ENGINE = MergeTree ORDER BY (year, month)",
        "CREATE TABLE IF NOT EXISTS report_store_sales (store_name String, city String, country String, orders_count Int64, total_revenue Float64, avg_check Float64, revenue_rank Int32) ENGINE = MergeTree ORDER BY (revenue_rank, store_name)",
        "CREATE TABLE IF NOT EXISTS report_supplier_sales (supplier_name String, supplier_country String, orders_count Int64, total_revenue Float64, avg_product_price Float64, revenue_rank Int32) ENGINE = MergeTree ORDER BY (revenue_rank, supplier_name)",
        "CREATE TABLE IF NOT EXISTS report_product_quality (product_id Int32, product_name String, category_name String, rating Float64, reviews_count Int64, total_sold Int64, total_revenue Float64, rating_sales_corr Float64) ENGINE = MergeTree ORDER BY (category_name, rating, product_id)",
        "TRUNCATE TABLE report_product_sales",
        "TRUNCATE TABLE report_customer_sales",
        "TRUNCATE TABLE report_time_sales",
        "TRUNCATE TABLE report_store_sales",
        "TRUNCATE TABLE report_supplier_sales",
        "TRUNCATE TABLE report_product_quality"
    };

    using var client = new HttpClient();
    client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("text/plain"));
    foreach (var statement in statements)
    {
        using var response = await client.PostAsync(clickHouseUrl, new StringContent(statement));
        var body = await response.Content.ReadAsStringAsync();
        if (!response.IsSuccessStatusCode)
        {
            throw new InvalidOperationException($"ClickHouse DDL error {(int)response.StatusCode}: {body}\nSQL:\n{statement}");
        }
    }
}
