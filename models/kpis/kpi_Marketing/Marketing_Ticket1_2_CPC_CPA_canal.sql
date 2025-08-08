-- -------------------------------------------------------------------------------
-- Analyse des performances des campagnes marketing par canal
-- -------------------------------------------------------------------------------
-- Permet d'agréger le budget total, les clics, et les conversions par canal.
-- Elle calcule également le coût par clic (CPC) et le coût par acquisition (CPA)
-- pour évaluer l'efficacité des dépenses publicitaires par canal.
-- -------------------------------------------------------------------------------

SELECT
  dc.nom_canal AS canal,                           -- Nom du canal (ex: Email, Réseaux Sociaux, etc.)
  SUM(fc.budget) AS budget_total,                 -- Somme du budget par canal
  SUM(fc.clics) AS total_clics,                   -- Total des clics générés
  SUM(fc.conversions) AS total_conversions,       -- Total des conversions
  ROUND(SUM(fc.budget) / NULLIF(SUM(fc.clics), 0), 2) AS cpc,   -- Coût par clic
  ROUND(SUM(fc.budget) / NULLIF(SUM(fc.conversions), 0), 2) AS cpa  -- Coût par conversion
FROM {{ ref('facts_campaigns') }} AS fc
JOIN {{ ref('dim_canal') }} AS dc
  ON fc.id_canal_dim_canal = dc.id_canal           -- Jointure entre fact et dimension
GROUP BY dc.nom_canal
ORDER BY budget_total DESC