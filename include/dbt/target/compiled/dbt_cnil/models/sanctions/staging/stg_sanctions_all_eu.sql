SELECT
  etid,
  country,
  CASE
    WHEN REGEXP_CONTAINS(date_of_decision,'Unknown') THEN NULL
    WHEN REGEXP_CONTAINS(date_of_decision, '^[0-9]{4}$') THEN PARSE_DATE('%Y', date_of_decision)
    WHEN REGEXP_CONTAINS(date_of_decision, '^[0-9]{4}-[0-9]{2}$') THEN PARSE_DATE('%Y-%m', date_of_decision)
    WHEN REGEXP_CONTAINS(date_of_decision, '^[0-9]{4}-[0-9]{2}-[0-9]{4}$') THEN PARSE_DATE('%Y-%m', LEFT(date_of_decision, 7))
    ELSE CAST(date_of_decision AS DATE)
  END
  AS date_of_decision,
  SAFE_CAST(REPLACE(fine_EUR, ",","") AS INT64) AS fine_EUR,
  controllerprocessor,
  quoted_art,
  TYPE as reason,
  source,
  authority,
  sector,
  summary,
  direct_url
FROM
  `cnil-392113`.`sanctions_eu`.`sanctions_all_eu`
ORDER BY date_of_decision DESC