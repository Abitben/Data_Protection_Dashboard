SELECT 
2020 as annee,
categorie_de_controle as type_de_controle,
organismes,
ville as lieu_de_controle, 
CAST(dept AS INT64) as departement, 
activite_de_lorganisme as secteur_dactivite, 
modalite_de_controle
FROM {{ source('controles', 'open_data_controles_2020_vd_20210603') }}
LIMIT 1000