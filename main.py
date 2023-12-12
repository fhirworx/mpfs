from pyspark.sql import DataFrame as PySparkDataFrame
from pyspark.sql import functions as F

# Helper function to load data and filter by code ID.
def load_and_filter_df(code_id, load_function, column):
    if code_id:
      df = load_function()
      return df.filter(F.col(column) == code_id)
    return None

def load_ruc():
    df = spark.sql('select * from eldb.ruc')
    df = df.withColumn("current_preservice",
                      F.coalesce(F.col("pre_time_pckg"), F.lit(0))+
                      F.coalesce(F.col("pre_eval_time"), F.lit(0))+
                      F.coalesce(F.col("pre_posi_time"), F.lit(0)))
    df = df.withColumn("current_postservice",
                      F.coalesce(F.col("post_imed_time"), F.lit(0))+
                      F.coalesce(F.col("post_visit_time"), F.lit(0)))
    return df

def load_equip():
    df = spark.sql('select * from eldb.pfs_equip_23')
    return df

def load_supply():
    df = spark.sql('select * from eldb.pfs_supply_23')
    return df

def load_labor():
    df = spark.sql('select * from eldb.pfs_labor_23')
    return df

def load_rvu():
    df = spark.sql('select * from eldb.pfs_rvu_23')
    return df

def get_current_intensity(code_id):
    df = load_and_filter_df(code_id, load_ruc, column='code_id')
    if df is not None:
      row = df.first()
      return tuple(row[col] for col in ['global_value', 'current_tt', 'current_ist', 'current_preservice', 'current_postservice', 'current_work'])
    return None
  
def get_current_labor(code_id):
    df = load_and_filter_df(code_id, load_labor, column='hcpcs')
    if df is not None:
      rate_per_minute = df.select('rate_per_minute').collect()[0][0]
      nf_total = df.select([F.sum(F.col(c)) for c in df.columns if c.startswith('nf_')]).collect()[0][0] * rate_per_minute
      f_total = df.select([F.sum(F.col(c)) for c in df.columns if c.startswith('f_')]).collect()[0][0] * rate_per_minute
      df = df.withColumn('nf_tot', F.lit(nf_total)).withColumn('f_tot', F.lit(f_total))
      return df
    return None

def get_current_equip(code_id):
    df = load_and_filter_df(code_id, load_equip, column='hcpcs')
    if df is not None:
      time_factor = (df['price'] / df['useful_life'] /df['minutes_per_year'])
      df = df.withColumn('nf_tot', F.col('nf_time') * time_factor)
      df = df.withColumn('f_tot', F.col('f_time') * time_factor)
      return df
    return None

def get_current_supply(code_id):
    df = load_and_filter_df(code_id, load_supply, column='hcpcs')
    if df is not None:
      df = df.withColumn('nf_tot', F.col('nf_quantity') * F.col('price'))
      df = df.withColumn('f_tot', F.col('f_quantity') * F.col('price'))
      return df
    return None

def get_filtered_rvu(code_id):
    df = load_and_filter_df(code_id, load_rvu, column='hcpcs')
    return df

def get_direct_pe(code_id):
    df1 = get_current_supply(code_id)
    df2 = get_current_equip(code_id)
    df3 = get_current_labor(code_id)
    if df1 is not None and df2 is not None and df3 is not None:
      current_dpe_tot_f = df1.select(F.sum('f_tot')).first()[0] + df2.select(F.sum('f_tot')).first()[0] + df3.select(F.sum('f_tot')).first()[0]
      current_dpe_tot_nf = df1.select(F.sum('nf_tot')).first()[0] + df2.select(F.sum('nf_tot')).first()[0] + df3.select(F.sum('nf_tot')).first()[0]
      return current_dpe_tot_f, current_dpe_tot_nf
    return None, None

def get_df_global(code_id):
    df = load_and_filter_df(code_id, load_ruc, column='code_id')
    if df is not None:
      search_global_value = df.select('global_value').first()[0]
      return df.filter(F.col('global_value') == search_global_value)
    return None

def get_filtered_data(code_id, tt_lower, tt_upper, ist_lower, ist_upper):
    search_global_value, _, _, _, tt_lower, tt_upper, ist_lower, ist_upper = get_search_params(code_id)
    df = spark.sql('select * from eldb.ruc')
    df = df.filter(df['global_value'] == search_global_value)
    if df is not None:
      df_filtered = df.filter((F.col('current_tt') >= tt_lower) & (F.col('current_tt') <= tt_upper) & (F.col('current_ist') >= ist_lower) & (F.col('current_ist') <= ist_upper))
      work_25th_percentile = df_filtered.approxQuantile('current_work', [0.25], 0.05)[0]
      df_work25th = df_filtered.filter(F.col('current_work') <= work_25th_percentile)
      return df_filtered, df_work25th
    return None, None

def get_search_params(code_id):
    search_global_value, current_tt, current_ist, _, _, _ = get_current_intensity(code_id)
    if search_global_value is not None:
        tt_lower = max(current_tt - 5, 1)
        tt_upper = current_tt + 5
        ist_lower = current_ist
        ist_upper = current_ist
        tt_min = max(current_tt - current_tt, 0)
        tt_max = tt_upper * 2.0
        ist_min = max(current_ist - current_ist, 0)
        ist_max = ist_upper * 2.0
        return tt_min, tt_max, ist_min, ist_max, tt_lower, tt_upper, ist_lower, ist_upper
    return None

def get_time_bounds(code_id):
    _, current_tt, current_ist, _, _, _ = get_current_intensity(code_id)
    if current_tt is not None and current_ist is not None:
        tt_min = 0
        tt_max = current_tt * 2.0
        ist_min = 0
        ist_max = current_ist * 2.0
        return tt_min, tt_max, ist_min, ist_max
    return None, None, None, None, None

def get_tt_ratio(ruc_tt, current_tt):
    return ruc_tt / current_tt if current_tt != 0 else 0.0

def get_tt_ratio_percent(tt_ratio):
    return tt_ratio * 100

def get_tt_ratio_work(tt_ratio, current_work):
    return tt_ratio * current_work

def get_ist_ratio(ruc_ist, current_ist):
    return ruc_ist / current_ist if current_ist != 0 else 0.0

def get_ist_ratio_work(ist_ratio, current_work):
    return ist_ratio * current_work

def filtered_search_count(df_filtered):
    return df_filtered.count()

def quartile_search_count(df_work25th):
    return df_work25th.count()

def get_median_work25th(df_work25th):
    return df_work25th.approxQuantile('current_work', [0.5], 0.05)[0]

def count_lower_values(df_work25th, ruc_work):
    return df_work25th.filter(F.col('current_work') < ruc_work).count()

def filter_for_crosswalks(df_work25th, cms_work):
    return df_work25th.filter(F.col('current_work') == cms_work)

def filter_by_hcpcs(df, hcpcs_code):
    return df.filter(F.col('hcpcs') == hcpcs_code)

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
