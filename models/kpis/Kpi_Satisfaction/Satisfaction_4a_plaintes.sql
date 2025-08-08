-- -------------------------------------------------------------------------------
-- Analyse de la satisfaction moyenne selon le type de plainte client
-- -------------------------------------------------------------------------------
-- Permet de calculer la note moyenne attribuée par les clients pour chaque 
-- catégorie de plainte recensée dans les avis.
-- Elle permet d’identifier les types de plaintes qui génèrent la plus forte 
-- insatisfaction (notes faibles) ainsi que ceux mieux perçus.
-- Le nombre total d’avis par type est également affiché pour évaluer la robustesse 
-- des résultats.
-- -------------------------------------------------------------------------------

SELECT
  type_plainte,                      -- Type de plainte ou catégorie de réclamation
  ROUND(AVG(note_client), 2) AS note_moyenne,  -- Note moyenne des clients pour ce type
  COUNT(*) AS nb_avis                -- Nombre total d’avis pour ce type de plainte
FROM {{ ref('dim_satisfaction') }}
GROUP BY type_plainte                -- Regroupement par type de plainte
ORDER BY note_moyenne ASC           -- Tri ascendant pour mettre en avant les notes les plus basses