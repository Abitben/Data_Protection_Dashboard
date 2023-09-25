
  
    

    create or replace table `cnil-392113`.`dev_dbt_staging`.`stg_controles_2022`
      
    
    

    OPTIONS()
    as (
      SELECT
  2022 AS annee,
  categorie__de_controle AS type_de_controle,
  modalite__de_controle AS organismes,
  ville AS lieu_de_controle,
  CAST(
    dept AS int64
  ) AS departement,
  activite__de_lorganisme AS secteur_dactivite,
  organismes_controles AS modalite_de_controle
FROM
  `cnil-392113`.`controles_realises_par_la_cnil`.`open_data_controles_cnil_2022_v20230621`
LIMIT
  1000
    );
  