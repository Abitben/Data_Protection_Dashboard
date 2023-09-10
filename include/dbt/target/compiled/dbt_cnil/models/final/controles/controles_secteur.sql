SELECT 	
secteur_dactivite, count(*) as nb_controles
FROM `cnil-392113`.`dev_dbt_final`.`controles_all`
GROUP BY 	
secteur_dactivite
ORDER BY nb_controles DESC