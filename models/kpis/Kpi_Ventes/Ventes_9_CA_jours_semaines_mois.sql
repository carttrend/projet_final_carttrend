-- -------------------------------------------------------------------------------
-- Chiffre d'affaires (CA) agrégé par jour, semaine et mois
-- -------------------------------------------------------------------------------

-- CA par jour
SELECT
  'jour' AS type,
  EXTRACT(YEAR FROM dt.date) AS annee,
  EXTRACT(MONTH FROM dt.date) AS mois,
  EXTRACT(WEEK FROM dt.date) AS semaine,
  CAST(dt.date AS STRING) AS periode,
  FORMAT_DATE('%F', dt.date) AS periode_standard, -- Format : YYYY-MM-DD
  ROUND(SUM(dc.quantite * p.prix), 2) AS chiffre_affaires
FROM {{ ref('facts_commandes') }} c
JOIN {{ ref('dim_date') }} dt ON dt.id_date = c.id_date_commande
JOIN {{ ref('dim_details_commandes') }} dc ON c.id_commande = dc.id_commande
JOIN {{ ref('dim_produits') }} p ON dc.id_details_produits = p.id_produit
WHERE LOWER(c.statut_commande) NOT IN ('annulée', 'cancelled')
GROUP BY dt.date

UNION ALL

-- CA par semaine
SELECT
  'semaine' AS type,
  EXTRACT(YEAR FROM dt.date) AS annee,
  EXTRACT(MONTH FROM dt.date) AS mois,
  EXTRACT(WEEK FROM dt.date) AS semaine,
  CONCAT('Semaine ', CAST(EXTRACT(WEEK FROM dt.date) AS STRING)) AS periode,
  FORMAT_DATE('%G-W%V', dt.date) AS periode_standard, -- Format : YYYY-WW
  ROUND(SUM(dc.quantite * p.prix), 2) AS chiffre_affaires
FROM {{ ref('facts_commandes') }} c
JOIN {{ ref('dim_date') }} dt ON dt.id_date = c.id_date_commande
JOIN {{ ref('dim_details_commandes') }} dc ON c.id_commande = dc.id_commande
JOIN {{ ref('dim_produits') }} p ON dc.id_details_produits = p.id_produit
WHERE LOWER(c.statut_commande) NOT IN ('annulée', 'cancelled')
GROUP BY EXTRACT(YEAR FROM dt.date), EXTRACT(MONTH FROM dt.date), EXTRACT(WEEK FROM dt.date), dt.date

UNION ALL

-- CA par mois
SELECT
  'mois' AS type,
  EXTRACT(YEAR FROM dt.date) AS annee,
  EXTRACT(MONTH FROM dt.date) AS mois,
  NULL AS semaine,
  CONCAT('Mois ', CAST(EXTRACT(MONTH FROM dt.date) AS STRING)) AS periode,
  FORMAT_DATE('%Y-%m', dt.date) AS periode_standard, -- Format : YYYY-MM
  ROUND(SUM(dc.quantite * p.prix), 2) AS chiffre_affaires
FROM {{ ref('facts_commandes') }} c
JOIN {{ ref('dim_date') }} dt ON dt.id_date = c.id_date_commande
JOIN {{ ref('dim_details_commandes') }} dc ON c.id_commande = dc.id_commande
JOIN {{ ref('dim_produits') }} p ON dc.id_details_produits = p.id_produit
WHERE LOWER(c.statut_commande) NOT IN ('annulée', 'cancelled')
GROUP BY EXTRACT(YEAR FROM dt.date), EXTRACT(MONTH FROM dt.date), dt.date