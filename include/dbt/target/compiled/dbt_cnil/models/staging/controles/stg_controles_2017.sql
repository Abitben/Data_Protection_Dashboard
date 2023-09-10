SELECT 
2017 as annee,
type_de_controle,
organismes,
lieu as lieu_de_controle, 
CAST(departement AS INT64) as departement, 
secteur_dactivite, 
modalite_de_controle
FROM `cnil-392113`.`controles_realises_par_la_cnil`.`Liste_des_controles_realises_par_la_CNIL_en_2017`
LIMIT 1000
OFFSET 2