

  create or replace view `cnil-392113`.`dev_dbt_final`.`controles_all`
  OPTIONS()
  as SELECT
  annee,
  organismes,
  secteur_dactivite,
  type_de_controle,
  modalite_de_controle,
  lieu_de_controle,
  CAST(departement AS int) as departement,
  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(grouped_company, 'é', 'e'), 'à', 'a'), 'è', 'e'), 'ç', 'c'), 'ù', 'u'), 'â', 'a'), 'ê', 'e'), 'î', 'i'), 'ô', 'o'), 'û', 'u'), 'ë', 'e'), 'ï', 'i'), 'ü', 'u') as grouped_company,
FROM
  `cnil-392113`.`dev_dbt_intermediate`.`int_controles_groups`
ORDER BY
  annee;

