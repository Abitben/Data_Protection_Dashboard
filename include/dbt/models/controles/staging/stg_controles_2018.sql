SELECT 
2018 as annee,
type_de_controle,
organismes,
lieu as lieu_de_controle, 
CAST(dep AS INT64) as departement, 
secteur_dactivite, 
modalite_de_controle
FROM {{ source('controles', 'opencnil_liste_controles_2018') }}
LIMIT 1000
OFFSET 2