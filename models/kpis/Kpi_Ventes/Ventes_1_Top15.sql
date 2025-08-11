-- Top 15 des produits qui génèrent le plus de chiffre d'affaires,en les regroupant par catégorie et en les classant du plus au moins rentable.

SELECT
p.categorie,
p.Produit,
SUM(dc.quantite) AS total_vendus,
ROUND(SUM(dc.quantite * p.prix), 2) AS chiffre_affaires
FROM {{ ref('dim_details_commandes') }} dc 
JOIN {{ ref('dim_produits') }} p 
ON dc.id_details_produits = p.id_produit
GROUP BY p.categorie, p.Produit
ORDER BY chiffre_affaires DESC