
  
    

    create or replace table `cnil-392113`.`dev_dbt_staging`.`stg_listes_sanctions_2015`
      
    
    

    OPTIONS()
    as (
      WITH without_ce AS (
SELECT *
FROM `cnil-392113`.`sanctions_prononcees_par_la_cnil`.`listes_sanctions_2015`
WHERE nom_ou_type_dorganisme NOT LIKE "%Conseil%"
)
SELECT PARSE_DATE('%d/%m/%Y',TRIM(date)) as date_date,
nom_ou_type_dorganisme,
theme,
manquements_principaux as manquement_principal,
decision_adoptee,
lien_vers_la_decision
FROM without_ce
    );
  