CREATE KEYSPACE IF NOT EXISTS reports
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS reports.report_product_sales (
    product_name text,
    category_name text,
    total_quantity_sold bigint,
    total_revenue double,
    avg_rating double,
    reviews_count bigint,
    sales_rank int,
    PRIMARY KEY ((category_name), sales_rank, product_name)
);

CREATE TABLE IF NOT EXISTS reports.report_customer_sales (
    customer_id int PRIMARY KEY,
    first_name text,
    last_name text,
    email text,
    country text,
    orders_count bigint,
    total_spent double,
    avg_check double,
    spending_rank int
);

CREATE TABLE IF NOT EXISTS reports.report_time_sales (
    year int,
    month int,
    orders_count bigint,
    total_revenue double,
    avg_order_size double,
    cumulative_revenue double,
    PRIMARY KEY ((year), month)
);

CREATE TABLE IF NOT EXISTS reports.report_store_sales (
    store_name text,
    city text,
    country text,
    orders_count bigint,
    total_revenue double,
    avg_check double,
    revenue_rank int,
    PRIMARY KEY ((country), revenue_rank, city, store_name)
);

CREATE TABLE IF NOT EXISTS reports.report_supplier_sales (
    supplier_name text,
    supplier_country text,
    orders_count bigint,
    total_revenue double,
    avg_product_price double,
    revenue_rank int,
    PRIMARY KEY ((supplier_country), revenue_rank, supplier_name)
);

CREATE TABLE IF NOT EXISTS reports.report_product_quality (
    product_id int,
    product_name text,
    category_name text,
    rating double,
    reviews_count bigint,
    total_sold bigint,
    total_revenue double,
    rating_sales_corr double,
    PRIMARY KEY ((category_name), rating, product_id)
) WITH CLUSTERING ORDER BY (rating DESC, product_id ASC);

TRUNCATE reports.report_product_sales;
TRUNCATE reports.report_customer_sales;
TRUNCATE reports.report_time_sales;
TRUNCATE reports.report_store_sales;
TRUNCATE reports.report_supplier_sales;
TRUNCATE reports.report_product_quality;
