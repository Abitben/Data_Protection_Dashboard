
  
    

    create or replace table `cnil-392113`.`dev_dbt_staging`.`stg_controles_2019`
      
    
    

    OPTIONS()
    as (
      SELECT 
2019 as annee,
type_de_controle,
organismes,
lieu as lieu_de_controle, 
CAST(dep AS INT64) as departement, 
secteur_dactivite, 
modalite_de_controle
FROM `cnil-392113`.`controles_realises_par_la_cnil`.`opencnil_liste_controles_2019`
LIMIT 1000
OFFSET 2
    );
  