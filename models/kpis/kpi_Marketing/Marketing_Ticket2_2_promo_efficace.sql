-- ------------------------------------------------------------------------------
-- Analyse d'efficacité des promotions (par type, valeur, période)
-- Objectif : Identifier les promotions les plus efficaces en mesurant l’augmentation
-- du chiffre d'affaires pendant la période promotionnelle par rapport à la période précédente
-- ------------------------------------------------------------------------------

-- Étape 1 : Taguer les ventes selon la période par rapport à la promotion
WITH ventes_taggees AS (
  SELECT
    dc.id_details_produits,                                -- Identifiant du produit vendu
    p.produit AS nom_produit,                              -- Nom du produit
    dc.quantite,                                           -- Quantité vendue
    p.prix,                                               -- Prix unitaire du produit
    dt3.date AS date_commande,                            -- Date réelle de la commande (via dim_date pour c.id_date_commande)
    pr.id_produit AS id_promotion,                        -- Identifiant de la promotion
    pr.type_promotion,                                    -- Type de promotion (ex: % ou fixe)
    pr.valeur_promotion,                                  -- Valeur de la promotion (ex: 20%)
    dt1.date AS date_debut_promo,                         -- Date de début de la promotion (via dim_date pour pr.id_date_debut)
    dt2.date AS date_fin_promo,                           -- Date de fin de la promotion (via dim_date pour pr.id_date_fin)

    -- Déterminer la période de la vente : avant, pendant ou après la promotion
    CASE
      WHEN dt3.date < dt1.date THEN 'avant'
      WHEN dt3.date BETWEEN dt1.date AND dt2.date THEN 'pendant'
      WHEN dt3.date > dt2.date THEN 'apres'
      ELSE NULL
    END AS periode_promo
  FROM {{ ref('dim_details_commandes') }} dc
  JOIN {{ ref('facts_commandes') }} c
    ON dc.id_commande = c.id_commande                     -- Lier les ventes aux commandes
  JOIN {{ ref('dim_produits') }} p
    ON dc.id_details_produits = p.id_produit              -- Lier les ventes aux produits
  JOIN {{ ref('dim_promotions') }} pr
    ON dc.id_details_produits = pr.id_produit             -- Lier les ventes aux promotions

  -- Ajout des jointures vers dim_date pour récupérer les vraies dates
  JOIN {{ ref('dim_date') }} dt1
    ON dt1.id_date = pr.id_date_debut                      -- Date de début promo
  JOIN {{ ref('dim_date') }} dt2
    ON dt2.id_date = pr.id_date_fin                        -- Date de fin promo
  JOIN {{ ref('dim_date') }} dt3
    ON dt3.id_date = c.id_date_commande                    -- Date de la commande

  -- Fenêtre de 15 jours avant et après la période de promotion
  WHERE dt3.date BETWEEN DATE_SUB(dt1.date, INTERVAL 15 DAY)
                    AND DATE_ADD(dt2.date, INTERVAL 15 DAY)
)

-- Étape 2 : Agréger les ventes par période (avant, pendant, après) pour chaque promotion
, resume_par_promo AS (
  SELECT
    id_promotion,
    type_promotion,
    valeur_promotion,
    periode_promo,
    SUM(quantite * prix) AS ca_total,
    SUM(quantite) AS volume_total
  FROM ventes_taggees
  WHERE periode_promo IS NOT NULL
  GROUP BY id_promotion, type_promotion, valeur_promotion, periode_promo
)

-- Étape 3 : Transformer les lignes en colonnes (pivot)
, pivot AS (
  SELECT
    id_promotion,
    type_promotion,
    valeur_promotion,
    MAX(CASE WHEN periode_promo = 'avant' THEN ca_total END) AS ca_avant,
    MAX(CASE WHEN periode_promo = 'pendant' THEN ca_total END) AS ca_pendant,
    MAX(CASE WHEN periode_promo = 'apres' THEN ca_total END) AS ca_apres
  FROM resume_par_promo
  GROUP BY id_promotion, type_promotion, valeur_promotion
)

-- Étape 4 : Calcul de l’augmentation en % : ((CA pendant - CA avant) / CA avant)
SELECT
  id_promotion,
  type_promotion,
  valeur_promotion,
  ROUND(ca_avant, 2) AS ca_avant,
  ROUND(ca_pendant, 2) AS ca_pendant,
  ROUND(ca_apres, 2) AS ca_apres,
  ROUND(
    SAFE_DIVIDE(ca_pendant - ca_avant, ca_avant) * 100, 2
  ) AS uplift_pct
FROM pivot
WHERE ca_avant IS NOT NULL AND ca_pendant IS NOT NULL
ORDER BY uplift_pct DESC