WITH sectors AS (
  SELECT
  DISTINCT(string_field_0) as organismes,
  string_field_1 as secteur,
  FROM cnil-392113.organismes_secteur.organismes_secteur
)
SELECT date_date,
  nom_ou_type_dorganisme,
  theme,
  sec.secteur as secteur,
  manquement_principal,
  decision_adoptee,
  amount,
  lien_vers_la_decision
FROM {{ ref('int_listes_sanctions__amount') }} as list
LEFT JOIN sectors AS sec
ON trim(list.nom_ou_type_dorganisme) = trim(sec.organismes)
