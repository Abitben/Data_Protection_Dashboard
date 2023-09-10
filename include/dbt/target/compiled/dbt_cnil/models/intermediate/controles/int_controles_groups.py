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


