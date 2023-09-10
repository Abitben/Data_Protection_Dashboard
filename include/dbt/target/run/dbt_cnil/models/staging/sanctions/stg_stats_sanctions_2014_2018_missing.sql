
  
    

    create or replace table `cnil-392113`.`dev_dbt_staging`.`stg_stats_sanctions_2014_2018_missing`
    
    

    OPTIONS()
    as (
      SELECT annee,
  avertissements_non_publics,
  avertissements_publics
FROM `cnil-392113`.`sanctions_prononcees_par_la_cnil`.`open_cnil_ventilation_sanctions_depuis_2014_vd`
    );
  