import mysql.connector
import pandas as pd
import datetime
import os
from dotenv import load_dotenv

load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_HOST = os.getenv("DB_HOST")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_USER = os.getenv("DB_USER")


mydb = mysql.connector.connect(
  host=DB_HOST,
  user=DB_USER,
  password=DB_PASSWORD,
  database=DB_NAME
)

mycursor = mydb.cursor()

tables = ["discount", "info", "sales_data"]

for table in tables:
  columns = "(product_id BIGINT, product_type VARCHAR(16), cogs BIGINT, segment CHAR(2)"

  if table == "discount":
    columns = "(product_id BIGINT, date_key DATE, discount_pctg INT(3)"

  elif table == "sales_data":
    columns = "(product_id BIGINT, date_key DATE, qty_sold BIGINT, Instock BOOLEAN"

  try:
    mycursor.execute(f"CREATE TABLE {table} {columns})")
  except Exception:
    pass

info_df = pd.read_csv("Data Bangkit - Info_clean.csv")

for product_id, product_type, cogs, segment in info_df.values:
    sql = "INSERT INTO info (product_id, product_type, cogs, segment) VALUES (%s, %s, %s, %s)"
    val = (product_id, product_type, int(cogs), segment)
    mycursor.execute(sql, val)

mydb.commit()

print(mycursor.rowcount, "record inserted.")

discount_df = pd.read_csv("Data Bangkit - discount_clean.csv")

for product_id, date_key, discount_pctg in discount_df.values:
    sql = "INSERT INTO discount (product_id, date_key, discount_pctg) VALUES (%s, %s, %s)"
    val = (product_id, datetime.datetime.strptime(date_key, "%Y-%m-%d"), int(discount_pctg))
    mycursor.execute(sql, val)

mydb.commit()

print(mycursor.rowcount, "record inserted.")

sales_data_df = pd.read_csv("Data Bangkit - sales_data.csv")

for product_id, date_key, qty_sold, instock in sales_data_df.values:
    sql = "INSERT INTO sales_data (product_id, date_key, qty_sold, Instock) VALUES (%s, %s, %s, %s)"
    val = (product_id, datetime.datetime.strptime(date_key, "%Y-%m-%d"), int(qty_sold), int(instock))
    mycursor.execute(sql, val)

mydb.commit()

print(mycursor.rowcount, "record inserted.")

mycursor.close()
mydb.close()