

  create or replace view `cnil-392113`.`dev_dbt_final`.`sanctions_stats_eu_all`
  OPTIONS()
  as SELECT
  *,
  SPLIT(REGEXP_REPLACE(quoted_art, r',\s*A', r';A'), ";") AS quoted_art_array,
  EXTRACT(YEAR from date_of_decision) AS year
FROM
  `cnil-392113`.`dev_dbt_staging`.`stg_sanctions_all_eu`
ORDER BY
  date_of_decision DESC;

