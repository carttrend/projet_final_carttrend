-- 📁 models/marts/dim/dim_details_produits.sql
SELECT DISTINCT
  p.id,
  p.categorie AS categorie,
  p.marque,
  p.prix,
  p.sous_categorie AS sous_categorie,
  p.variation,
  d.id_produit
FROM {{ ref('stg_produits') }} AS p
JOIN {{ ref('dim_produits') }} AS d
  ON p.Produit = d.produit