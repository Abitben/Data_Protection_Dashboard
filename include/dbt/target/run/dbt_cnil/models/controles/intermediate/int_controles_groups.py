
  
    
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('smallTest').getOrCreate()

spark.conf.set("viewsEnabled","true")
spark.conf.set("temporaryGcsBucket","cnil-392113")

import pandas as pd
import pandas as pd
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

def find_best_match(name, company_list, threshold=90):
    """
    Compare the input name with a list of company names and returns the best match if similarity is above the threshold.
    """
    matches = process.extractBests(name, company_list, scorer=fuzz.token_set_ratio, score_cutoff=threshold)
    if matches:
        return matches[0][0]
    else:
        return name


def model(dbt, session):
    dbt.config(
        submission_method="cluster",
        packages = ['pandas', 'fuzzywuzzy'],
    )
    df = dbt.ref("int_controles__union").toPandas()  # Convert Spark DataFrame to Pandas DataFrame
    df['organismes'] = df['organismes'].str.lower().str.strip()
    df = df.sort_values(by='organismes')
    company_list = df['organismes'].unique()
    df['grouped_company'] = df['organismes'].apply(lambda x: find_best_match(x, company_list))
    return df


# This part is user provided model code
# you will need to copy the next section to run the code
# COMMAND ----------
# this part is dbt logic for get ref work, do not modify

def ref(*args, **kwargs):
    refs = {"int_controles__union": "cnil-392113.dev_dbt_intermediate.int_controles__union"}
    key = '.'.join(args)
    version = kwargs.get("v") or kwargs.get("version")
    if version:
        key += f".v{version}"
    dbt_load_df_function = kwargs.get("dbt_load_df_function")
    return dbt_load_df_function(refs[key])


def source(*args, dbt_load_df_function):
    sources = {}
    key = '.'.join(args)
    return dbt_load_df_function(sources[key])


config_dict = {}


class config:
    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def get(key, default=None):
        return config_dict.get(key, default)

class this:
    """dbt.this() or dbt.this.identifier"""
    database = "cnil-392113"
    schema = "dev_dbt_intermediate"
    identifier = "int_controles_groups"
    
    def __repr__(self):
        return 'cnil-392113.dev_dbt_intermediate.int_controles_groups'


class dbtObj:
    def __init__(self, load_df_function) -> None:
        self.source = lambda *args: source(*args, dbt_load_df_function=load_df_function)
        self.ref = lambda *args, **kwargs: ref(*args, **kwargs, dbt_load_df_function=load_df_function)
        self.config = config
        self.this = this()
        self.is_incremental = False

# COMMAND ----------



dbt = dbtObj(spark.read.format("bigquery").load)
df = model(dbt, spark)

# COMMAND ----------
# this is materialization code dbt generated, please do not modify

import pyspark
# make sure pandas exists before using it
try:
  import pandas
  pandas_available = True
except ImportError:
  pandas_available = False

# make sure pyspark.pandas exists before using it
try:
  import pyspark.pandas
  pyspark_pandas_api_available = True
except ImportError:
  pyspark_pandas_api_available = False

# make sure databricks.koalas exists before using it
try:
  import databricks.koalas
  koalas_available = True
except ImportError:
  koalas_available = False

# preferentially convert pandas DataFrames to pandas-on-Spark or Koalas DataFrames first
# since they know how to convert pandas DataFrames better than `spark.createDataFrame(df)`
# and converting from pandas-on-Spark to Spark DataFrame has no overhead
if pyspark_pandas_api_available and pandas_available and isinstance(df, pandas.core.frame.DataFrame):
  df = pyspark.pandas.frame.DataFrame(df)
elif koalas_available and pandas_available and isinstance(df, pandas.core.frame.DataFrame):
  df = databricks.koalas.frame.DataFrame(df)

# convert to pyspark.sql.dataframe.DataFrame
if isinstance(df, pyspark.sql.dataframe.DataFrame):
  pass  # since it is already a Spark DataFrame
elif pyspark_pandas_api_available and isinstance(df, pyspark.pandas.frame.DataFrame):
  df = df.to_spark()
elif koalas_available and isinstance(df, databricks.koalas.frame.DataFrame):
  df = df.to_spark()
elif pandas_available and isinstance(df, pandas.core.frame.DataFrame):
  df = spark.createDataFrame(df)
else:
  msg = f"{type(df)} is not a supported type for dbt Python materialization"
  raise Exception(msg)

df.write \
  .mode("overwrite") \
  .format("bigquery") \
  .option("writeMethod", "direct").option("writeDisposition", 'WRITE_TRUNCATE') \
  .save("cnil-392113.dev_dbt_intermediate.int_controles_groups")

  