-- Analyse de l'effet des promotions sur les ventes

SELECT
  -- Classification des commandes par rapport à la période de promotion
  CASE
    WHEN dt3.date BETWEEN DATE_SUB(dt1.date, INTERVAL 7 DAY) AND DATE_SUB(dt1.date, INTERVAL 1 DAY)
      THEN 'Avant promo'     -- 7 jours avant la promo
    WHEN dt3.date BETWEEN dt1.date AND dt2.date
      THEN 'Pendant promo'   -- Pendant la période de promotion
    WHEN dt3.date BETWEEN DATE_ADD(dt2.date, INTERVAL 1 DAY) AND DATE_ADD(dt2.date, INTERVAL 7 DAY)
      THEN 'Après promo'     -- 7 jours après la fin de la promo
    ELSE 'Hors période'      -- Toutes les autres dates (exclues ensuite par WHERE)
  END AS periode_analyse,

  p.produit AS produit,  -- Nom du produit concerné

  -- Calcul du chiffre d'affaires (quantité * prix unitaire)
  ROUND(SUM(dc.quantite * p.prix), 2) AS chiffre_affaires,

  -- Quantité totale de produits vendus
  SUM(dc.quantite) AS quantite_vendue,

  -- Nombre de commandes distinctes
  COUNT(DISTINCT c.id_commande) AS nb_commandes

-- Table des commandes (avec date, statut, etc.)
FROM {{ ref('facts_commandes') }} c

-- Jointure avec les détails des commandes pour accéder à la quantité et au produit commandé
JOIN {{ ref('dim_details_commandes') }} dc 
  ON c.id_commande = dc.id_commande

-- Jointure avec la table des produits pour récupérer le nom et le prix
JOIN {{ ref('dim_produits') }} p 
  ON dc.id_details_produits = p.id_produit

-- Jointure avec la table des promotions pour déterminer les dates de début et fin de promo
JOIN {{ ref('dim_promotions') }} pr 
  ON pr.id_produit = p.id_produit
JOIN {{ ref('dim_date') }} dt1 ON dt1.id_date = pr.id_date_debut
JOIN {{ ref('dim_date') }} dt2 ON dt2.id_date = pr.id_date_fin
JOIN {{ ref('dim_date') }} dt3 ON dt3.id_date = c.id_date_commande
-- Filtrage sur les commandes comprises dans la période de 7 jours avant à 7 jours après la promo
WHERE
  dt3.date BETWEEN DATE_SUB(dt1.date, INTERVAL 7 DAY) 
                      AND DATE_ADD(dt2.date, INTERVAL 7 DAY)

  -- Exclusion des commandes annulées ou annulées en anglais
  AND LOWER(c.statut_commande) NOT IN ('annulée', 'cancelled')

-- Agrégation par période d’analyse et par produit
GROUP BY periode_analyse, p.produit

-- Tri par produit, puis ordre logique des périodes (avant → pendant → après → hors)
ORDER BY 
  p.produit,
  CASE periode_analyse
    WHEN 'Avant promo' THEN 1
    WHEN 'Pendant promo' THEN 2
    WHEN 'Après promo' THEN 3
    ELSE 4
  END