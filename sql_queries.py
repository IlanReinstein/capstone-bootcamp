create_user_table = """CREATE SCHEMA IF NOT EXISTS purchases_db;
CREATE TABLE IF NOT EXISTS purchases_db.users_purchases (
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
