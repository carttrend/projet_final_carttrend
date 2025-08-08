-- Top 3 produits et catégories préférés par tranche d'âge
-- ------------------------------------------------------------------------------
-- Permet d'identifie les produits et catégories les plus achetés pour chaque tranche d'âge des clients, en se basant sur la quantité vendue.
-- Permet de comprendre les préférences d'achat selon les segments démographiques.
-- ------------------------------------------------------------------------------

WITH base AS ( 
  SELECT 
    CASE 
      WHEN cl.age BETWEEN 18 AND 34 THEN '18-34'              -- Jeunes adultes
      WHEN cl.age BETWEEN 35 AND 54 THEN '35-54'              -- Adultes
      WHEN cl.age BETWEEN 55 AND 60 THEN '55-60'              -- Pré-seniors
      ELSE '60+'                                              -- Seniors
    END AS tranche_age, 
    p.categorie,                                              -- Catégorie du produit
    p.produit,                                                -- Nom du produit
    SUM(dc.quantite) AS quantite_vendue,                       -- Quantité totale vendue
    ROUND(SUM(dc.quantite * p.prix), 2) AS ca                  -- Chiffre d'affaires généré
  FROM {{ ref('dim_details_commandes') }} dc 
  JOIN {{ ref('facts_commandes') }} c 
    ON dc.id_commande = c.id_commande 
  JOIN {{ ref('dim_produits') }} p 
    ON dc.id_details_produits = p.id_produit
  JOIN {{ ref('dim_clients') }} cl 
    ON c.id_client = cl.id_client 
  GROUP BY tranche_age, p.categorie, p.produit
), 

-- Agrégation par catégorie et par produit avec classement
ranked AS (
  SELECT
    tranche_age, 
    nom,
    type,
    quantite_vendue,
    chiffre_affaires,
    ROW_NUMBER() OVER (PARTITION BY tranche_age, type ORDER BY quantite_vendue DESC) AS rang
  FROM (
    -- Agrégation par catégorie
    SELECT 
      tranche_age,
      categorie AS nom,
      'categorie' AS type,
      SUM(quantite_vendue) AS quantite_vendue,
      SUM(ca) AS chiffre_affaires
    FROM base
    GROUP BY tranche_age, categorie
    
    UNION ALL
    
    -- Agrégation par produit
    SELECT 
      tranche_age,
      produit AS nom,
      'produit' AS type,
      SUM(quantite_vendue) AS quantite_vendue,
      SUM(ca) AS chiffre_affaires
    FROM base
    GROUP BY tranche_age, produit
  )
)

-- Extraction du top 3 pour chaque tranche d’âge et type
SELECT * 
FROM ranked 
WHERE rang <= 3
ORDER BY tranche_age, type, rang