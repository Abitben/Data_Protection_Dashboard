SELECT 
2021 as annee,
categorie_de_controle as type_de_controle,
organismes_controles as organismes,
ville as lieu_de_controle, 
CAST(dept AS INT64) as departement, 
activite_de_lorganisme as secteur_dactivite, 
modalite_de_controle
FROM {{ source('controles', 'open_data_controles_2021_v20220921') }}
LIMIT 1000