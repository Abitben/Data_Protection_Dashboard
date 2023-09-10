
  
    

    create or replace table `cnil-392113`.`dev_dbt_final`.`sanctions_stats_secteur`
    
    

    OPTIONS()
    as (
      SELECT secteur, count(*) as nb_sanctions
FROM `cnil-392113`.`dev_dbt_intermediate`.`int_listes_sanctions__secteur`
GROUP BY secteur
ORDER BY nb_sanctions DESC
    );
  