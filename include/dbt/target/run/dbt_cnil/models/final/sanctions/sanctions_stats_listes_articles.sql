
  
    

    create or replace table `cnil-392113`.`dev_dbt_final`.`sanctions_stats_listes_articles`
    
    

    OPTIONS()
    as (
      SELECT concat_article, SUM(amount) as sum_amount, count(*) as nb_sanctions
FROM `cnil-392113`.`dev_dbt_intermediate`.`int_listes_sanctions__unnest`
GROUP BY concat_article
ORDER BY nb_sanctions DESC
    );
  