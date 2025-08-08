-- Analyse des produits en déclin : baisse continue du chiffre d'affaires mensuel
-- --------------------------------------------------------------------------------------
-- Permet d’identifier les produits dont le chiffre d’affaires (CA) mensuel diminue de manière continue sur les six derniers mois.
-- Elle combine des jointures temporelles, des fonctions analytiques et une détection de tendance négative soutenue.
-- --------------------------------------------------------------------------------------

-- Étape 1 : Calcul du chiffre d'affaires mensuel par produit
WITH ventes_par_mois AS ( 
  SELECT
    dc.id_details_produits, 
    p.id_produit,
    p.categorie,
    p.prix,
    DATE_TRUNC(dt.date, MONTH) AS mois,  -- Extraction du mois de commande
    ROUND(SUM(dc.quantite * p.prix), 2) AS ca_mensuel                      -- CA = quantité × prix
  FROM {{ ref('dim_details_commandes') }} dc 
  JOIN {{ ref('facts_commandes') }} c 
    ON dc.id_commande = c.id_commande 
  JOIN {{ ref('dim_date') }} dt
    ON c.id_date_commande = dt.id_date
  JOIN {{ ref('dim_produits') }} p 
    ON dc.id_details_produits = p.id_produit
  GROUP BY dc.id_details_produits, p.id_produit,p.categorie,DATE_TRUNC(dt.date, MONTH), p.prix
), 

-- Étape 2 : Création d'une table complète avec tous les mois pour chaque produit
ventes_completes AS (
  SELECT 
    p.id_produit,                                    -- Identifiant du produit
    p.categorie,                             -- Catégorie du produit
    c.mois,                                  -- Mois considéré
    COALESCE(v.ca_mensuel, 0) AS ca_mensuel  -- CA du produit pour le mois (0 si aucune vente)
  FROM {{ ref('dim_produits') }} p
  CROSS JOIN (
    SELECT DISTINCT 
      DATE_TRUNC(dt.date, MONTH) AS mois 
    FROM {{ ref('facts_commandes') }} co
    JOIN {{ ref('dim_date')}} dt
    ON co.id_date_commande = dt.id_date
  ) c
  LEFT JOIN ventes_par_mois v 
    ON p.id_produit  = v.id_produit AND c.mois = v.mois
), 

-- Étape 3 : Ajout des 5 mois précédents pour chaque produit (fenêtre glissante)
avec_variations AS ( 
  SELECT *, 
    LAG(ca_mensuel, 1) OVER (PARTITION BY id_produit ORDER BY mois) AS m1, 
    LAG(ca_mensuel, 2) OVER (PARTITION BY id_produit ORDER BY mois) AS m2, 
    LAG(ca_mensuel, 3) OVER (PARTITION BY id_produit ORDER BY mois) AS m3, 
    LAG(ca_mensuel, 4) OVER (PARTITION BY id_produit ORDER BY mois) AS m4, 
    LAG(ca_mensuel, 5) OVER (PARTITION BY id_produit ORDER BY mois) AS m5 
  FROM ventes_completes
), 

-- Étape 4 : Filtrage des produits en déclin (6 mois de baisse consécutive)
ventes_produits_en_declin AS ( 
  SELECT * 
  FROM avec_variations 
  WHERE 
    ca_mensuel < m1 AND 
    m1 < m2 AND 
    m2 < m3 AND 
    m3 < m4 AND 
    m4 < m5 
)

-- Résultat final : liste des produits en déclin
SELECT * 
FROM ventes_produits_en_declin