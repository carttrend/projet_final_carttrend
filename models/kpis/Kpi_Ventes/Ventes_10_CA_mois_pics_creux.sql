-- -------------------------------------------------------------------------------
-- Analyse du chiffre d'affaires mensuel avec classification des variations
-- -------------------------------------------------------------------------------
-- Permet de calculer le chiffre d'affaires total par mois et compare chaque mois à la moyenne globale.
-- Elle classe ensuite chaque mois selon sa performance :
--   - "Mois fort" si CA > 130% de la moyenne
--   - "Mois faible" si CA < 70% de la moyenne
--   - "Normal" sinon
-- Ceci permet d’identifier rapidement les mois atypiques en termes de ventes.
-- -------------------------------------------------------------------------------

WITH ca_mensuel AS (
  SELECT
    EXTRACT(YEAR FROM dt.date) AS annee,
    EXTRACT(MONTH FROM dt.date) AS mois,
    ROUND(SUM(dc.quantite * p.prix), 2) AS chiffre_affaires
  FROM {{ ref('facts_commandes') }} c
  JOIN {{ ref('dim_date') }} dt ON dt.id_date = c.id_date_commande
  JOIN {{ ref('dim_details_commandes') }} dc ON c.id_commande = dc.id_commande
  JOIN {{ ref('dim_produits') }} p ON dc.id_details_produits = p.id_produit
  WHERE LOWER(c.statut_commande) NOT IN ('annulée', 'cancelled')
  GROUP BY annee, mois
)

SELECT
  CONCAT(annee, '-', LPAD(CAST(mois AS STRING), 2, '0')) AS periode,
  chiffre_affaires,
  ROUND(AVG(chiffre_affaires) OVER (), 2) AS moyenne_globale,
  CASE
    WHEN chiffre_affaires > AVG(chiffre_affaires) OVER () * 1.3 THEN 'Mois fort'
    WHEN chiffre_affaires < AVG(chiffre_affaires) OVER () * 0.7 THEN 'Mois faible'
    ELSE 'Normal'
  END AS variation
FROM ca_mensuel
ORDER BY periode