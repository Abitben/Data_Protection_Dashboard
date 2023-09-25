
  
    

    create or replace table `cnil-392113`.`dev_dbt_staging`.`stg_stats_sanctions_2019_missing`
      
    
    

    OPTIONS()
    as (
      SELECT annee, 
avec_injonction as am_ad_non_pub_avec_inj, 
avec_injonction as am_ad_non_pub_sans_inj, 
avec_injonction2 as am_ad_pub_avec_inj, 
sans_injonction2 as am_ad_pub_sans_inj, 
injonction_seule_non_publique, 
injonction_seule_publique, 
rappel_a_lordre_non_public, 
rappel_a_lordre_public, 
autres_mesures_non_publiques_prevues_par_larticle_20_lil, 
autres_mesures_publiques_prevues_par_larticle_20_lil 
FROM `cnil-392113`.`sanctions_prononcees_par_la_cnil`.`opencnil_sanctions_depuis_2019_maj_aout_2022`
    );
  