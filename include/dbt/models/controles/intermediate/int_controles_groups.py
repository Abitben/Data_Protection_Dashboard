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
