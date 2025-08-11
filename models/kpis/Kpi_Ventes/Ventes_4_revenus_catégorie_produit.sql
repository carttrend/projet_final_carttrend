-- Évolution quotidienne du chiffre d'affaires par catégorie de produits
-- ------------------------------------------------------------------------------
-- Permet de calculer le chiffre d'affaires (CA) généré chaque jour, pour chaque catégorie de produits.
-- Elle permet aussi de visualiser les tendances de vente journalières par typologie.
-- ------------------------------------------------------------------------------

SELECT 
  d.date AS date_commande,              -- Date de la commande
  p.categorie AS categorie,                      -- Catégorie du produit
  ROUND(SUM(dc.quantite * p.prix), 2) AS total_revenu  -- Revenu total (quantité × prix)
FROM {{ ref('dim_details_commandes') }} dc 
JOIN {{ ref('facts_commandes') }}c 
  ON dc.id_commande = c.id_commande 
JOIN {{ ref('dim_date') }} d ON d.id_date = c.id_date_commande
JOIN {{ ref('dim_produits') }} p 
  ON dc.id_details_produits = p.id_produit
GROUP BY date_commande, categorie 
ORDER BY date_commande, total_revenu DESC 