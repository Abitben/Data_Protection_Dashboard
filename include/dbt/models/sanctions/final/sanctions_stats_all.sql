select * FROM {{ref("int_stats_sanctions__union")}}
LEFT JOIN {{ref("stg_stats_sanctions_2019_missing")}} USING (annee)
LEFT JOIN {{ref("stg_stats_sanctions_2014_2018_missing")}} USING (annee)
ORDER BY annee