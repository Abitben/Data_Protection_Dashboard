WITH amounts_by_decision AS (
  SELECT
    DISTINCT Decision,
    amount
  FROM
    cnil-392113.to_complete_sets.amounts_sanctions
  WHERE
    amount IS NOT NULL
),

amounts_by_date AS (
  SELECT
    date_date as date_ind,
    amount
  FROM
    cnil-392113.to_complete_sets.amount_by_date
),

amounts_by_link AS (
  SELECT
    string_field_0 as link,
    SAFE_CAST(string_field_1 AS INT64) as amount
  FROM
    cnil-392113.to_complete_sets.amount_by_link
)

SELECT
  date_date,
  nom_ou_type_dorganisme,
  theme,
  manquement_principal,
  decision_adoptee,
  COALESCE(amounts.amount, amounts_link.amount, amounts_date.amount) AS amount,
  lien_vers_la_decision
FROM
  `cnil-392113`.`dev_dbt_intermediate`.`int_listes_sanctions__union` AS listes
LEFT JOIN amounts_by_decision AS amounts
  ON listes.decision_adoptee = amounts.Decision
LEFT JOIN amounts_by_link AS amounts_link
  ON listes.lien_vers_la_decision = amounts_link.link
LEFT JOIN amounts_by_date AS amounts_date
  ON listes.date_date = amounts_date.date_ind
ORDER BY date_date