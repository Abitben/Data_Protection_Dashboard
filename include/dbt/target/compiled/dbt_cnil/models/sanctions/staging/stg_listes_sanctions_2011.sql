SELECT PARSE_DATE('%d/%m/%Y',TRIM(date)) as date_date,
nom_ou_type_dorganisme,
theme,
manquement_principal,
decision_adoptee,
lien_vers_la_decision
FROM `cnil-392113`.`sanctions_prononcees_par_la_cnil`.`listes_sanctions_2011`