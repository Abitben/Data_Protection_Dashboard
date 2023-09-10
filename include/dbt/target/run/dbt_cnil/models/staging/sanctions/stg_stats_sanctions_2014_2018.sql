
  
    

    create or replace table `cnil-392113`.`dev_dbt_staging`.`stg_stats_sanctions_2014_2018`
    
    

    OPTIONS()
    as (
      SELECT annee,
  sanctions_pecuniaires_non_publiques AS amende_administrative_non_publique,
  sanctions_pecuniaires_publiques AS amende_administrative_publique,
  relaxesnonlieu,
  total
FROM `cnil-392113`.`sanctions_prononcees_par_la_cnil`.`open_cnil_ventilation_sanctions_depuis_2014_vd`
    );
  