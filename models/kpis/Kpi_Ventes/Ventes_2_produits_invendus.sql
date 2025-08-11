-- Liste des produits jamais vendus
-- Permet d’identifier les produits du catalogue qui n’ont jamais été associés à une commande (zéro vente).

--Produits invendus
SELECT 
  p.produit, 
  0 AS total_vendus, 
  'jamais_vendu' AS type_resultat
FROM {{ ref('dim_produits') }} p
LEFT JOIN {{ ref('dim_details_commandes') }} dc
  ON p.id_produit = dc.id_details_produits
WHERE dc.id_details_produits IS NULL