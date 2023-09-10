SELECT
  annee,
  organismes,
  secteur_dactivite,
  type_de_controle,
  modalite_de_controle,
  lieu_de_controle,
  CAST(departement AS int) as departement,
  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(grouped_company, 'é', 'e'), 'à', 'a'), 'è', 'e'), 'ç', 'c'), 'ù', 'u'), 'â', 'a'), 'ê', 'e'), 'î', 'i'), 'ô', 'o'), 'û', 'u'), 'ë', 'e'), 'ï', 'i'), 'ü', 'u') as grouped_company,
FROM
  {{ref("int_controles_groups")}}
ORDER BY
  annee
