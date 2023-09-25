
  
    

    create or replace table `cnil-392113`.`dev_dbt_staging`.`stg_listes_sanctions_2020`
      
    
    

    OPTIONS()
    as (
      SELECT PARSE_DATE('%d/%m/%Y',TRIM(date)) as date_date,
type_dorganisme as nom_ou_type_dorganisme,
CAST(NULL AS STRING) as theme,
manquements_principaux__theme as manquement_principal,
decision_adoptee,
lien_vers_la_decision
FROM `cnil-392113`.`sanctions_prononcees_par_la_cnil`.`listes_sanctions_2020`
    );
  