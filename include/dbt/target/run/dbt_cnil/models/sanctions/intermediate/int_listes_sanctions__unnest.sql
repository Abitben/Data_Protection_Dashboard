
  
    

    create or replace table `cnil-392113`.`dev_dbt_intermediate`.`int_listes_sanctions__unnest`
      
    
    

    OPTIONS()
    as (
      WITH unnested_manquements AS (
    SELECT 
        date_date,
        nom_ou_type_dorganisme,
        TRIM(element) AS manquements_principaux, 
        theme,
        amount,
        secteur,
        decision_adoptee,
        lien_vers_la_decision
    FROM `cnil-392113`.`dev_dbt_intermediate`.`int_listes_sanctions__array`, UNNEST(manquements_principaux) AS element
),

liste_sanctions AS (
    SELECT
        DISTINCT(string_field_0) AS manquement,
        string_field_1 AS article_RGPD,
        string_field_3 AS titre_article,
        string_field_4 AS concat_article
    FROM
        `cnil-392113.manquement_articles.manquements_for_sanctions`
)

SELECT * 
FROM unnested_manquements AS sanc
LEFT JOIN liste_sanctions AS list
ON TRIM(sanc.manquements_principaux) = TRIM(LOWER(list.manquement))
    );
  