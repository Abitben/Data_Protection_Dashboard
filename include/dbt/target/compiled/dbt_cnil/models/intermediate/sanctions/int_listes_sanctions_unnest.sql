SELECT ARRAY_AGG(element) AS distinct_elements
FROM `cnil-392113`.`dev_dbt_intermediate`.`int_listes_sanctions__array`, UNNEST(manquement_principal) AS element