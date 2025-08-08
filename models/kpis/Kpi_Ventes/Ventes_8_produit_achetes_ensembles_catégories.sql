-- -------------------------------------------------------------------------------
-- Analyse des associations fréquentes entre produits achetés ensemble
-- -------------------------------------------------------------------------------

WITH commandes_produits AS (
  SELECT
    id_commande,
    id_details_produits AS id_produit  -- Correction ici pour identifier le produit
  FROM {{ ref('dim_details_commandes') }}
),

produits_frequents AS (
  SELECT
    id_produit,
    COUNT(DISTINCT id_commande) AS nb_achats
  FROM commandes_produits
  GROUP BY id_produit
),

cooccurrences AS (
  SELECT
    cp1.id_produit AS produit_1,
    cp2.id_produit AS produit_2,
    COUNT(*) AS nb_achats_ensemble
  FROM commandes_produits cp1
  JOIN commandes_produits cp2
    ON cp1.id_commande = cp2.id_commande
   AND cp1.id_produit < cp2.id_produit
  GROUP BY produit_1, produit_2
  HAVING nb_achats_ensemble > 1
),

produits_nommes AS (
  SELECT
    id_produit AS id,
    produit
  FROM {{ ref('dim_produits') }}
),

total_cmds AS (
  SELECT COUNT(DISTINCT id_commande) AS total
  FROM commandes_produits
)

SELECT
  p1.produit AS nom_produit_1,
  p2.produit AS nom_produit_2,
  c.nb_achats_ensemble,
  f1.nb_achats AS nb_achats_p1,
  f2.nb_achats AS nb_achats_p2,

  ROUND(c.nb_achats_ensemble / f1.nb_achats, 3) AS confidence_p1_to_p2,

  ROUND(
    (c.nb_achats_ensemble / f1.nb_achats) /
    (f2.nb_achats / total.total),
    3
  ) AS lift

FROM cooccurrences c
JOIN produits_frequents f1 ON c.produit_1 = f1.id_produit
JOIN produits_frequents f2 ON c.produit_2 = f2.id_produit
JOIN produits_nommes p1 ON c.produit_1 = p1.id
JOIN produits_nommes p2 ON c.produit_2 = p2.id
CROSS JOIN total_cmds total

ORDER BY lift DESC