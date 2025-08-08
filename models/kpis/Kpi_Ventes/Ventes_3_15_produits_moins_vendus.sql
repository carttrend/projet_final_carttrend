-- Top 15 des produits les moins vendus
-- Permet d'identifier les produits avec une faible rotation pour éventuellement les retirer ou les promouvoir.

SELECT 
  p.produit, 
  SUM(dc.quantite) AS total_vendus, 
  'moins_vendus' AS type_resultat 
FROM {{ ref('dim_details_commandes') }} dc 
JOIN {{ ref('dim_produits') }} p 
  ON dc.id_details_produits = p.id_produit
GROUP BY p.produit 
HAVING total_vendus > 0  -- On exclut les produits jamais vendus
ORDER BY total_vendus ASC 