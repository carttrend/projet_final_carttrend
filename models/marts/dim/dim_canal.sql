SELECT DISTINCT
  ROW_NUMBER() OVER() AS id_canal,
  canal AS nom_canal
FROM {{ ref('stg_campaigns') }}
GROUP BY canal