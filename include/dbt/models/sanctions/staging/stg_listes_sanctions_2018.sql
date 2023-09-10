SELECT PARSE_DATE('%d/%m/%Y',TRIM(date)) as date_date,
nom_ou_type_dorganisme,
CAST(NULL AS STRING) as theme,
manquements_principaux__theme as manquement_principal,
decision_adoptee,
lien_vers_la_decision
FROM {{source('sanctions','listes_sanctions_2018')}}