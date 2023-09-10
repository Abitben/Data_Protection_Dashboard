
  
    

    create or replace table `cnil-392113`.`dev_dbt_final`.`controles_groups`
    
    

    OPTIONS()
    as (
      SELECT 	
grouped_company, count(*) as nb_controles
FROM `cnil-392113`.`dev_dbt_final`.`controles_all`
GROUP BY 	
grouped_company
ORDER BY nb_controles DESC
    );
  