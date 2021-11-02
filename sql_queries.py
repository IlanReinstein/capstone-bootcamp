CREATE_USER_PURCHASE_TABLE = """
CREATE TABLE IF NOT EXISTS purchases.users_purchases (
    invoice_number varchar(10),
    stock_code varchar(20),
    detail varchar(1000),
    quantity int,
    invoice_date timestamp,
    unit_price numeric(8, 3),
    customer_id int,
    country varchar(20);
)
"""

COPY_SQL = """
COPY {}
FROM '{}'
IGNOREHEADER 1
DELIMITER ','
"""

COPY_ALL_USER_PURCHASE_SQL = COPY_SQL.format(
    "user_purchase.csv",
    "gs://capstone-ir"
)
