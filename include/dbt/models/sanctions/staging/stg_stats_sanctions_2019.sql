SELECT annee,
amende_administrative_non_publique,
amende_administrative_publique,
relaxesnonlieu,
total
FROM {{ source('sanctions', 'opencnil_sanctions_depuis_2019_maj_aout_2022') }}



	
