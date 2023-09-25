select * FROM `cnil-392113`.`dev_dbt_intermediate`.`int_stats_sanctions__union`
LEFT JOIN `cnil-392113`.`dev_dbt_staging`.`stg_stats_sanctions_2019_missing` USING (annee)
LEFT JOIN `cnil-392113`.`dev_dbt_staging`.`stg_stats_sanctions_2014_2018_missing` USING (annee)
ORDER BY annee