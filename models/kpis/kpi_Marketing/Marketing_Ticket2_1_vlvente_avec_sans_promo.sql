  -- ===============================================================================
-- Analyse du chiffre d'affaires (CA) total et moyen par jour selon les promotions
-- ===============================================================================
-- Permet de :
-- 1. Construire la table dim_details_commandes à partir du staging stg_details_commandes
-- 2. Distinguer les ventes réalisées "en promotion" ou "hors promotion"
-- 3. Calculer le chiffre d'affaires total pour chaque type de vente
-- 4. Compter le nombre de jours distincts où les ventes ont eu lieu
-- 5. Déterminer le CA moyen par jour pour les ventes en promo et hors promo
-- 6. Comparer les performances via un ratio de CA total et un ratio de CA journalier
-- ===============================================================================

-- Étape 0 : Construction de la table dim_details_commandes à partir du staging
WITH dim_details_commandes AS (
  SELECT
    ROW_NUMBER() OVER (ORDER BY id_commande, id_details_produits) AS id_produit, -- Identifiant unique artificiel
    quantite,                                                  -- Quantité vendue
    emballage_special,                                         -- Info sur emballage spécial
    id_commande,                                              -- Référence à la commande
    id_details_produits                         -- Référence produit (clé étrangère)
  FROM {{ ref('dim_details_commandes') }}
),

-- Étape 1 : Marquer chaque vente comme "en_promo" ou "hors_promo"
ventes_taggees AS (
  SELECT
    ddc.id_details_produits,                      -- Identifiant du produit vendu
    p.id_produit AS id_produit_reel,                       -- ID réel du produit depuis la table produits
    ddc.quantite,                                  -- Quantité vendue
    p.prix,                                       -- Prix unitaire du produit
    dt1.date AS date_commande,                              -- Date de la commande
    dt2.date,                                -- Date de début de la promotion (si applicable)
    dt3.date,                                  -- Date de fin de la promotion (si applicable)
    CASE
      WHEN pr.id_produit IS NOT NULL
           AND dt1.date BETWEEN dt2.date AND dt3.date
      THEN 'en_promo'                             -- Si la commande est pendant une promo
      ELSE 'hors_promo'                           -- Sinon, hors promo
    END AS promo_statut                           -- Statut promotionnel de la vente
  FROM dim_details_commandes ddc
  JOIN {{ ref('facts_commandes') }} c
    ON ddc.id_commande = c.id_commande            -- Association avec la commande principale
  JOIN {{ ref('dim_date')}} dt1 ON c.id_date_commande = dt1.id_date
  JOIN {{ ref('dim_produits') }} p
    ON ddc.id_details_produits = p.id_produit              -- Association avec les infos produit
  LEFT JOIN {{ ref('dim_promotions') }} pr
    ON ddc.id_details_produits = pr.id_produit     -- Jointure avec les promotions (si existantes)
  JOIN {{ ref('dim_date')}} dt2 ON pr.id_date_debut = dt2.id_date
  JOIN {{ ref('dim_date')}} dt3 ON pr.id_date_fin = dt3.id_date
),

-- Étape 2 : Résumer le CA total, le nombre de jours distincts et le CA moyen par jour
resume_ca AS (
  SELECT
    promo_statut,                                                  -- Statut "en_promo" ou "hors_promo"
    ROUND(SUM(quantite * prix), 2) AS chiffre_affaires_total,      -- CA total pour ce statut
    COUNT(DISTINCT date_commande) AS nb_jours_vente,               -- Nombre de jours distincts de vente
    ROUND(SUM(quantite * prix) / COUNT(DISTINCT date_commande), 2) AS ca_moyen_par_jour
                                                                    -- CA moyen par jour pour ce statut
  FROM ventes_taggees
  GROUP BY promo_statut
),

-- Étape 3 : Transformer les lignes "en_promo" et "hors_promo" en colonnes
pivot_ca AS (
  SELECT
    MAX(CASE WHEN promo_statut = 'en_promo' THEN chiffre_affaires_total END) AS ca_promo,
    MAX(CASE WHEN promo_statut = 'hors_promo' THEN chiffre_affaires_total END) AS ca_hors_promo,
    MAX(CASE WHEN promo_statut = 'en_promo' THEN ca_moyen_par_jour END) AS ca_moyen_jour_promo,
    MAX(CASE WHEN promo_statut = 'hors_promo' THEN ca_moyen_par_jour END) AS ca_moyen_jour_hors_promo
  FROM resume_ca
)

-- Étape 4 : Affichage final avec ratios comparatifs
SELECT
  ca_promo,                                      -- Chiffre d'affaires total pendant les promotions
  ca_hors_promo,                                 -- Chiffre d'affaires total hors promotions
  ca_moyen_jour_promo,                           -- CA moyen par jour pendant les promotions
  ca_moyen_jour_hors_promo,                      -- CA moyen par jour hors promotion
  ROUND(SAFE_DIVIDE(ca_promo, ca_hors_promo), 2) AS ratio_ca_total_promo_vs_hors,
                                                 -- Ratio CA total : promo / hors promo
  ROUND(SAFE_DIVIDE(ca_moyen_jour_promo, ca_moyen_jour_hors_promo), 2) AS ratio_ca_moyen_jour_promo_vs_hors
                                                 -- Ratio CA journalier : promo / hors promo
FROM pivot_ca