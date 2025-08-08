-- Top 15 des produits qui génèrent le plus de chiffre d'affaires,en les regroupant par catégorie et en les classant du plus au moins rentable.

SELECT 
  --p.categorie,  -- Catégorie du produit
   p.produit,    -- Nom du produit
   p.prix
  --SUM(dc.quantite) AS total_vendus,  -- Quantité totale vendue pour ce produit
  --ROUND(SUM(dc.quantite * p.prix), 2) AS chiffre_affaires  -- Chiffre d'affaires généré (prix x quantité)
FROM {{ ref('dim_details_commandes') }} dc 
JOIN {{ ref('dim_produits') }} p 
  ON dc.id_details_produits = p.id_produit  -- Jointure corrigée avec la bonne colonne
--GROUP BY p.categorie, p.produit -- Regroupement par catégorie et produit
--ORDER BY chiffre_affaires DESC   -- Classement par chiffre d'affaires décroissant