SELECT  * FROM {{ref('stg_stats_sanctions_2019')}}
UNION ALL
SELECT * FROM {{ref('stg_stats_sanctions_2014_2018')}}
ORDER BY annee