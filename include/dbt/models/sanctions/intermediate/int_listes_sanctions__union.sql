SELECT * FROM {{ ref('stg_listes_sanctions_2011') }}
UNION ALL
SELECT * FROM {{ ref('stg_listes_sanctions_2012') }}
UNION ALL
SELECT * FROM {{ ref('stg_listes_sanctions_2013') }}
UNION ALL
SELECT * FROM {{ ref('stg_listes_sanctions_2014') }}
UNION ALL
SELECT * FROM {{ ref('stg_listes_sanctions_2015') }}
UNION ALL
SELECT * FROM {{ ref('stg_listes_sanctions_2016') }}
UNION ALL
SELECT * FROM {{ ref('stg_listes_sanctions_2017') }}
UNION ALL
SELECT * FROM {{ ref('stg_listes_sanctions_2018') }}
UNION ALL
SELECT * FROM {{ ref('stg_listes_sanctions_2019') }}
UNION ALL
SELECT * FROM {{ ref('stg_listes_sanctions_2020') }}
UNION ALL
SELECT * FROM {{ ref('stg_listes_sanctions_2021') }}
UNION ALL
SELECT * FROM {{ ref('stg_listes_sanctions_2022') }}
UNION ALL
SELECT * FROM {{ ref('stg_listes_sanctions_2023') }}
ORDER BY date_date DESC