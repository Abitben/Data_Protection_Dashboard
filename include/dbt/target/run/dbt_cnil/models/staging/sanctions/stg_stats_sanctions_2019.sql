
  
    

    create or replace table `cnil-392113`.`dev_dbt_staging`.`stg_stats_sanctions_2019`
    
    

    OPTIONS()
    as (
      SELECT annee,
amende_administrative_non_publique,
amende_administrative_publique,
relaxesnonlieu,
total
FROM `cnil-392113`.`sanctions_prononcees_par_la_cnil`.`opencnil_sanctions_depuis_2019_maj_aout_2022`
    );
  