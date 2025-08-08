--  Analyse des produits les plus ajoutés aux favoris
-- ------------------------------------------------------------------------------
-- Permet d'identifie les produits les plus fréquemment ajoutés aux favoris par les clients. 
-- Permet de détecter les produits les plus populaires en intention d'achat.
-- ------------------------------------------------------------------------------

WITH favoris AS (
  SELECT
    TRIM(fav) AS id_produit  -- Nettoyage des espaces autour des identifiants de produit
  FROM {{ ref('dim_clients') }},
       UNNEST(SPLIT(favoris, ',')) AS fav  -- Transformation de la chaîne de favoris en liste
)

SELECT
  p.produit,                     -- Nom du produit
  COUNT(*) AS nb_fois_ajoute     -- Nombre de fois que le produit a été mis en favori
FROM favoris f
JOIN {{ ref('dim_produits') }} p
  ON f.id_produit = p.id_produit
GROUP BY p.produit
ORDER BY nb_fois_ajoute DESC    -- Tri décroissant pour obtenir les produits les plus ajoutés