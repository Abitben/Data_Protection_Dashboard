SELECT
  parse_date('%d/%m/%Y', TRIM(DATE)) AS date_date,
  nom_ou_type_dorganisme,
  CAST(
    NULL AS STRING
  ) AS theme,
  manquements_principaux__theme AS manquement_principal,
  decision_adoptee,
  lien_vers_la_decision
FROM
  {{ source(
    'sanctions',
    'listes_sanctions_2019'
  ) }}
