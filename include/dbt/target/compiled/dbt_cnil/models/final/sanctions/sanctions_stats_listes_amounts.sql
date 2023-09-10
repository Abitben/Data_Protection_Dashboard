SELECT
  EXTRACT(
    YEAR
    FROM
      date_date
  ) AS date_date,
  SUM(amount) AS sum_amount
FROM
  `cnil-392113`.`dev_dbt_intermediate`.`int_listes_sanctions__array`
GROUP BY
  date_date
ORDER BY
  date_date DESC