-- 📁 models/marts/facts/facts_campagnes.sql
SELECT DISTINCT
  ca.id_campagne as id_campaign,
  CAST(ca.evenement_oui_non AS STRING) AS evenement,
  ca.evenement_type AS evenement_type,
  ca.budget,
  ca.impressions,
  ca.clics,
  ca.conversions,
  ca.ctr,
  dc.id_canal AS id_canal_dim_canal
FROM {{ ref('stg_campaigns') }} AS ca
JOIN {{ ref('dim_canal') }} AS dc
  ON ca.canal = dc.nom_canal