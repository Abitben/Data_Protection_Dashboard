SELECT annee,
  avertissements_non_publics,
  avertissements_publics
FROM {{ source('sanctions', 'open_cnil_ventilation_sanctions_depuis_2014_vd') }}