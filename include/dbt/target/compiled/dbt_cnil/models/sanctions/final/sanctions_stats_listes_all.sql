SELECT 
  *,
  ARRAY(
    SELECT string_field_4 as articles
    FROM UNNEST(manquements_principaux) AS manquement
    JOIN cnil-392113.manquement_articles.manquements_for_sanctions
    ON  TRIM(manquement) = TRIM(LOWER(string_field_0))
  ) AS articles,
  EXTRACT(YEAR FROM date_date) AS year,
FROM `cnil-392113`.`dev_dbt_intermediate`.`int_listes_sanctions__array`
ORDER BY date_date