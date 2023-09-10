WITH tables_union AS (
  SELECT
    *
  FROM
    {{ ref('stg_controles_2014') }}
  UNION ALL
  SELECT
    *
  FROM
    {{ ref('stg_controles_2015') }}
  UNION ALL
  SELECT
    *
  FROM
    {{ ref('stg_controles_2016') }}
  UNION ALL
  SELECT
    *
  FROM
    {{ ref('stg_controles_2017') }}
  UNION ALL
  SELECT
    *
  FROM
    {{ ref('stg_controles_2018') }}
  UNION ALL
  SELECT
    *
  FROM
    {{ ref('stg_controles_2019') }}
  UNION ALL
  SELECT
    *
  FROM
    {{ ref('stg_controles_2020') }}
  UNION ALL
  SELECT
    *
  FROM
    {{ ref('stg_controles_2021') }}
  UNION ALL
  SELECT
    *
  FROM
    {{ ref('stg_controles_2022') }}
  ORDER BY
    annee
)
SELECT
  annee,
  LOWER(organismes) AS organismes,
  CASE
    WHEN secteur_dactivite IN (
      'Banques',
      'Banques-Finance',
      'Banque'
    ) THEN 'Banque/Finance'
    WHEN secteur_dactivite IN (
      'Collectivités locales',
      'Collectivités territoriales'
    ) THEN 'Collectivités locales'
    WHEN secteur_dactivite IN (
      'Education',
      'Education/culture/sport',
      'Éducation'
    ) THEN 'Éducation/Culture/Sport'
    WHEN secteur_dactivite IN (
      'Police',
      'Police/justice/sécurité',
      'Police/ justice/ sécurité'
    ) THEN 'Police/Justice/Sécurité'
    WHEN secteur_dactivite IN (
      'Santé',
      'Santé ', 
      'Social',
      'Santé/social',
      'Santé/ social'
    ) THEN 'Santé/Social'
    WHEN secteur_dactivite IN (
      'Télécommunications',
      'Nouvelles technologies'
    ) THEN 'Télécommunications/Technologies'
    WHEN secteur_dactivite IN (
      'Transport',
      'Transports'
    ) THEN 'Transport'
    WHEN secteur_dactivite IN (
      'Assurances',
      'Assurance'
    ) THEN 'Assurance'
    WHEN secteur_dactivite IN (
      'Énergie',
      'Energie'
    ) THEN 'Énergie'
    WHEN secteur_dactivite IN ('Travail') THEN 'Travail'
    WHEN secteur_dactivite IN ('Immobilier') THEN 'Immobilier'
    WHEN secteur_dactivite IN ('Associatif') THEN 'Associatif'
    WHEN secteur_dactivite IN ('Sport') THEN 'Sport'
    WHEN secteur_dactivite IN ('Tourisme') THEN 'Tourisme'
    WHEN secteur_dactivite IN ('Jeux') THEN 'Jeux'
    WHEN secteur_dactivite IN (
      'Régalien',
      'Ministères',
      'Finances publiques'
    ) THEN 'État'
    WHEN secteur_dactivite IN (
      'Politique',
      'Activités politiques'
    ) THEN 'Activités politiques'
    ELSE secteur_dactivite
  END AS secteur_dactivite,
  CASE
    WHEN regexp_contains(LOWER(type_de_controle), 'vidéo|videoprotection') THEN "video"
    WHEN regexp_contains(LOWER(type_de_controle), 'loi 1978|loi 78') THEN "loi 1978"
    WHEN regexp_contains(LOWER(type_de_controle), 'contrôles en ligne|contrôle en ligne') THEN "controle en ligne"
    WHEN regexp_contains(LOWER(type_de_controle), 'rgpd') THEN "rgpd"
    WHEN regexp_contains(LOWER(type_de_controle), 'directive police justice|directive') THEN "rgpd - directive police justice"END AS type_de_controle,
    modalite_de_controle,
    CASE
      WHEN LOWER(lieu_de_controle) IN (
        'levallois perret',
        'levallois-perret'
      ) THEN 'levallois perret'
      WHEN LOWER(lieu_de_controle) IN (
        'rueil-malmaison',
        'rueil malmaison'
      ) THEN 'rueil-malmaison'
      WHEN LOWER(lieu_de_controle) IN (
        'boulogne-billancourt',
        'boulogne billancourt'
      ) THEN 'boulogne-billancourt'
      WHEN LOWER(lieu_de_controle) IN (
        'paris la defense',
        'paris la défense'
      ) THEN 'paris la defense'
      WHEN LOWER(lieu_de_controle) IN (
        'ivry sur seine',
        'ivry-sur-seine'
      ) THEN 'ivry-sur-seine'
      WHEN LOWER(lieu_de_controle) IN (
        'tremblay en france',
        'tremblay-en-france'
      ) THEN 'tremblay-en-france'
      WHEN LOWER(lieu_de_controle) IN (
        'montigny le bretonneux',
        'montigny-le-bretonneux'
      ) THEN 'montigny-le-bretonneux'
      WHEN LOWER(lieu_de_controle) IN (
        'sunnyvale (usa)',
        'sunnyvale'
      ) THEN 'sunnyvale'
      WHEN LOWER(lieu_de_controle) IN (
        'paris (audition locaux cnil)',
        'paris'
      ) THEN 'paris'
      WHEN LOWER(lieu_de_controle) IN (
        'saint maur des fosses',
        'saint-maur-des-fosses'
      ) THEN 'saint-maur-des-fosses'
      WHEN LOWER(lieu_de_controle) IN (
        "saint ouen l\'aumône",
        "saint-ouen l'aumône"
      ) THEN "saint-ouen l\'aumône"
      WHEN LOWER(lieu_de_controle) IN (
        'cergy pontoise',
        'cergy'
      ) THEN 'cergy'
      WHEN LOWER(lieu_de_controle) IN (
        'neuilly plaisance',
        'neuilly-plaisance'
      ) THEN 'neuilly-plaisance'
      WHEN LOWER(lieu_de_controle) IN (
        'neuilly-sur-seine',
        'neuilly'
      ) THEN 'neuilly-sur-seine'
      WHEN LOWER(lieu_de_controle) IN (
        'nice',
        'nice '
      ) THEN 'nice'
      WHEN LOWER(lieu_de_controle) IN (
        'issy-les-moulineaux',
        'issy les moulineaux'
      ) THEN 'issy-les-moulineaux'
      WHEN LOWER(lieu_de_controle) IN (
        'clichy',
        'clichy la garenne'
      ) THEN 'clichy'
      WHEN LOWER(lieu_de_controle) IN (
        'saint-quentin',
        'saint quentin'
      ) THEN 'saint-quentin'
      WHEN LOWER(lieu_de_controle) IN (
        'saint cloud',
        'saint-cloud'
      ) THEN 'saint-cloud'
      WHEN LOWER(lieu_de_controle) IN (
        'nogent-sur-oise',
        'nogent sur oise'
      ) THEN 'nogent-sur-oise'
      ELSE LOWER(lieu_de_controle)
    END AS lieu_de_controle,
    departement
    FROM
      tables_union
