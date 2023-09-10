WITH array_manquement AS (
  SELECT
    date_date,
    nom_ou_type_dorganisme,
    theme,
    secteur,
    amount,
    LOWER(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(manquement_principal, r'\s+([A-Z][a-zéèêëàâäôöùûüç])|,', r';\1'), ',', ';'))) AS manquements_principaux,
    decision_adoptee,
    lien_vers_la_decision
    FROM
      {{ ref('int_listes_sanctions__secteur') }})
SELECT
  date_date,
  nom_ou_type_dorganisme,
  theme,
  secteur,
  SPLIT(manquements_principaux, ";") as manquements_principaux,
  amount,
  decision_adoptee,
  lien_vers_la_decision
FROM
  array_manquement
ORDER BY
  date_date DESC