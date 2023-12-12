import pyspark.sql.functions as F  
import pandas as pd
df = spark.sql('select * from eldb.rt_eldb_pt_abd_2017_2023_service_level_100pct')
### Helper Function to Set Filters based on Various Conditional Logic  
def apply_filter(df, conditions):
  for col, op, val in conditions:
    if op == "==":
      df = df.filter(F.col(col) == val)
    elif op == ">":
      df = df.filter(F.col(col) > val)
    elif op == "<":
      df = df.filter(F.col(col) < val)
    elif op == ">=":
      df = df.filter(F.col(col) >= val)
    elif op == "isin":
      df = df.filter(F.col(col).isin(*val))
    elif op == "not":
      df = df.filter(~F.col(col) != val)
    elif op == "not_in":
      df = df.filter(~F.col(col).isin(*val))
  return df
### Helper Function to Group and Order
def create_order(df: F.DataFrame, group_columns: list, sum_column: str, sort_conditions: list):
  grouped_df = df.groupBy(group_columns).agg(F.sum(sum_column).alias("sum_" +sum_column))
  sort_exprs = [F.col(col).asc() if ascending else F.col(col).desc()
                for col, ascending in sort_conditions]
  ordered_df = grouped_df.orderBy(sort_exprs)
  return ordered_df
### Helper Function for Dollar Amounts Formatting
def format_currency(amount):
     return "${:,.2f}".format(amount)    
### Select all from ELDB at service level
df = spark.sql('select * from eldb.rt_eldb_pt_abd_2017_2023_service_level_100pct')
review_years = [2017, 2018, 2019, 2020, 2021, 2022, 2023]
review_codes = [code_id] + [row['code_id'] for row in df_filtered.select('code_id').collect()]
filter_conditions = [
  ("allowed_amt", ">", 0),
  ("srvc_dlvrd", "isin", review_codes),
  ("srvc_yr", "isin", review_years)
]
group_columns = ["srvc_yr", "srvc_dlvrd"]
sum_column = "allowed_amt"
sort_conditions = [("srvc_yr", True), ("srvc_dlvrd", True)]
df = apply_filter(df, filter_conditions)
result_df = create_order(df, group_columns, sum_column, sort_conditions)
result_df.display()
