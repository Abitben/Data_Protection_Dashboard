
  
    

    create or replace table `cnil-392113`.`dev_dbt_final`.`sanctions_stats`
    
    

    OPTIONS()
    as (
      select * FROM `cnil-392113`.`dev_dbt_intermediate`.`int_stats_sanctions__union`
LEFT JOIN `cnil-392113`.`dev_dbt_staging`.`stg_stats_sanctions_2019_missing` USING (annee)
ORDER BY annee
    );
  