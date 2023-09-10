
  
    

    create or replace table `cnil-392113`.`dev_dbt_staging`.`stg_listes_sanctions_2017`
    
    

    OPTIONS()
    as (
      SELECT PARSE_DATE('%d/%m/%Y',TRIM(date)) as date_date,
nom_ou_type_dorganisme,
theme,
manquements_principaux as manquement_principal,
decision_adoptee,
lien_vers_la_decision
FROM `cnil-392113`.`sanctions_prononcees_par_la_cnil`.`listes_sanctions_2017`
    );
  