-- -------------------------------------------------------------------------------
-- Analyse comparative de l'impact des promotions sur les ventes des produits
-- -------------------------------------------------------------------------------
-- Permet de mesurer l'effet des promotions en comparant le chiffre d'affaires et les quantités vendues avant et pendant la promotion pour chaque produit.
-- Elle calcule aussi les variations en pourcentage et donne une interprétation simple de la réaction des produits à la promotion.
-- -------------------------------------------------------------------------------

WITH ventes_par_periode AS (
  -- Agrégation du CA et des quantités vendues selon les périodes "avant", "pendant" ou "autre"
  SELECT
    p.id_produit AS id_produit,
    p.produit AS produit,
    CASE
      WHEN dt3.date BETWEEN DATE_SUB(dt1.date, INTERVAL 7 DAY) 
                               AND DATE_SUB(dt2.date, INTERVAL 1 DAY)
        THEN 'avant'    -- 7 jours avant le début de la promo

      WHEN dt3.date BETWEEN dt1.date AND dt2.date
        THEN 'pendant'  -- Période de la promotion

      ELSE 'autre'     -- Toutes les autres périodes (exclues ensuite)
    END AS periode,
    SUM(dc.quantite * p.prix) AS chiffre_affaires,
    SUM(dc.quantite) AS quantite_vendue
  FROM {{ ref('facts_commandes') }} c
  JOIN {{ ref('dim_details_commandes') }} dc
    ON c.id_commande = dc.id_commande
  JOIN {{ ref('dim_produits') }} p 
    ON dc.id_details_produits = p.id_produit
  JOIN {{ ref('dim_promotions') }} pr 
    ON pr.id_produit = p.id_produit
  JOIN {{ ref('dim_date') }} dt1 ON dt1.id_date = pr.id_date_debut
  JOIN {{ ref('dim_date') }} dt2 ON dt2.id_date = pr.id_date_fin
  JOIN {{ ref('dim_date') }} dt3 ON dt3.id_date = c.id_date_commande 
  WHERE dt3.date BETWEEN DATE_SUB(dt1.date, INTERVAL 7 DAY) AND dt2.date
    AND LOWER(c.statut_commande) NOT IN ('annulée', 'cancelled')  -- Exclure commandes annulées
  GROUP BY p.id_produit, p.produit, periode
),

comparaison AS (
  -- Mise en relation des données "avant" et "pendant" pour chaque produit
  SELECT
    v_avant.id_produit,
    v_avant.produit,
    v_avant.chiffre_affaires AS ca_avant,
    v_pendant.chiffre_affaires AS ca_pendant,
    v_avant.quantite_vendue AS qte_avant,
    v_pendant.quantite_vendue AS qte_pendant
  FROM ventes_par_periode v_avant
  JOIN ventes_par_periode v_pendant
    ON v_avant.id_produit = v_pendant.id_produit
   AND v_avant.periode = 'avant'
   AND v_pendant.periode = 'pendant'
)

SELECT
  produit,                    -- Nom du produit
  ca_avant,                  -- CA sur la période avant promo
  ca_pendant,                -- CA sur la période pendant promo
  ROUND(SAFE_DIVIDE(ca_pendant - ca_avant, ca_avant) * 100, 2) AS variation_CA_pct,  -- Variation en %
  qte_avant,                 -- Quantité vendue avant promo
  qte_pendant,               -- Quantité vendue pendant promo
  ROUND(SAFE_DIVIDE(qte_pendant - qte_avant, qte_avant) * 100, 2) AS variation_qte_pct,  -- Variation en %
  CASE
    WHEN SAFE_DIVIDE(ca_pendant - ca_avant, ca_avant) >= 0.3 THEN 'Réagit très bien à la promo'  -- +30% ou plus
    WHEN SAFE_DIVIDE(ca_pendant - ca_avant, ca_avant) <= 0.1 THEN 'Insensible à la promo'          -- +10% ou moins
    ELSE 'Effet modéré'                                                                     -- Entre 10% et 30%
  END AS interpretation
FROM comparaison
ORDER BY variation_CA_pct DESC  -- Classement des produits par impact promo décroissant