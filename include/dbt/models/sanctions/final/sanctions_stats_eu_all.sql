SELECT
  *,
  SPLIT(REGEXP_REPLACE(quoted_art, r',\s*A', r';A'), ";") AS quoted_art_array,
  EXTRACT(YEAR from date_of_decision) AS year
FROM
  {{ ref('stg_sanctions_all_eu') }}
ORDER BY
  date_of_decision DESC
