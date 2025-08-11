--  Analyse des produits les plus ajoutés aux favoris
-- ------------------------------------------------------------------------------
-- Permet d'identifie les produits les plus fréquemment ajoutés aux favoris par les clients. 
-- Permet de détecter les produits les plus populaires en intention d'achat.
-- ------------------------------------------------------------------------------

SELECT
  p.produit,
  COUNT(f.id_client) AS nb_fois_ajoute
FROM {{ ref('dim_produits') }} p
JOIN {{ ref('dim_favoris') }} f
  ON p.id_produit = CONCAT('P', LPAD(SUBSTR(f.favoris, 2) , 5, '0'))
GROUP BY p.id_produit, p.produit
ORDER BY nb_fois_ajoute DESC