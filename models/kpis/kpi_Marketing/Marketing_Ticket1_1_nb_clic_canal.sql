-- -------------------------------------------------------------------------------
-- Analyse des clics totaux par canal marketing
-- -------------------------------------------------------------------------------
-- Permet de calculer le nombre total de clics reçus par canal 
-- de campagne marketing afin d'identifier les canaux les plus performants.
-- -------------------------------------------------------------------------------

SELECT
  dc.nom_canal AS canal,            -- Nom lisible du canal marketing
  SUM(fc.clics) AS total_clics      -- Somme des clics pour chaque canal
FROM {{ ref('facts_campaigns') }} AS fc
JOIN {{ ref('dim_canal') }} AS dc
  ON fc.id_canal_dim_canal = dc.id_canal
GROUP BY dc.nom_canal
ORDER BY total_clics DESC