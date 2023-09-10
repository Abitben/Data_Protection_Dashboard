SELECT annee,
type_de_controle,
nom_de_lorganisme_controle_ou_nom_de_domaine as organismes,
lieu_de_controle, 
CAST(LEFT(departement, 2) AS INT64) as departement, 
secteur_dactivite,
CAST(NULL as STRING) as modalite_de_controle 
FROM {{ source('controles', 'Liste_des_controles_realises_par_la_CNIL_en_2014') }}