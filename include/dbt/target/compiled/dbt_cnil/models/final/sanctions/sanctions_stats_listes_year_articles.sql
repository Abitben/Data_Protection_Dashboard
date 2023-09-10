SELECT
  EXTRACT(
    YEAR
    FROM
      date_date
  ) AS date_date,
  concat_article,
  COUNT(*) AS nb_sanctions,
  SUM(amount) AS sum_amount
FROM
  `cnil-392113`.`dev_dbt_intermediate`.`int_listes_sanctions__unnest`
GROUP BY
  date_date,
  concat_article
ORDER BY
  date_date DESC,
  concat_article ASC