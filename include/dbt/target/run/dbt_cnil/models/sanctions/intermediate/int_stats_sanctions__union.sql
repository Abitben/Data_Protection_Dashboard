
  
    

    create or replace table `cnil-392113`.`dev_dbt_intermediate`.`int_stats_sanctions__union`
      
    
    

    OPTIONS()
    as (
      SELECT  * FROM `cnil-392113`.`dev_dbt_staging`.`stg_stats_sanctions_2019`
UNION ALL
SELECT * FROM `cnil-392113`.`dev_dbt_staging`.`stg_stats_sanctions_2014_2018`
ORDER BY annee
    );
  