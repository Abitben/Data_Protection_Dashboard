
  
    

    create or replace table `cnil-392113`.`dev_dbt_staging`.`stg_controles_2016`
      
    
    

    OPTIONS()
    as (
      SELECT annee,
type_de_controle,
organismes,
lieu_de_controle, 
departement, 
secteur_dactivite_de_lorganisme as secteur_dactivite,
CAST(NULL as STRING) as modalite_de_controle  
FROM `cnil-392113`.`controles_realises_par_la_cnil`.`Liste_des_controles_realises_par_la_CNIL_en_2016`
    );
  