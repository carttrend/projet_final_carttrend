-- =======================================================================================
-- Analyse de corrélation : pics de mentions sur les réseaux sociaux vs performances ventes
-- =======================================================================================
-- Objectif :
-- 1. Identifier les jours où les mentions explosent sur les réseaux sociaux (pics),
-- 2. Comparer le volume de ventes entre ces jours et les jours "normaux",
-- 3. Évaluer s’il existe une corrélation potentielle entre mentions et ventes.
-- =======================================================================================

-- Étape 1 : Calcul des volumes de mentions par jour
WITH mentions_stats AS (
  SELECT
    d.date AS date_post,               -- Date du post (via dim_date)
    COUNT(*) AS total_mentions         -- Nombre total de mentions ce jour-là
  FROM {{ ref('facts_posts') }} f
  JOIN {{ ref('dim_date') }} d ON d.id_date = f.id_date
  GROUP BY d.date
),

-- Étape 2 : Calcul des statistiques globales (moyenne et écart-type)
stats_calc AS (
  SELECT
    AVG(total_mentions) AS moyenne_mentions,          -- Moyenne journalière des mentions
    STDDEV(total_mentions) AS ecart_type_mentions      -- Écart-type des mentions par jour
  FROM mentions_stats
),

-- Étape 3 : Identification des jours avec pic de mentions (au-delà de 2 écarts-types)
mentions_avec_pic AS (
  SELECT
    m.date_post,
    m.total_mentions,
    CASE
      WHEN m.total_mentions > s.moyenne_mentions + 2 * s.ecart_type_mentions THEN 1
      ELSE 0
    END AS pic_mention                                 -- 1 = jour pic, 0 = jour normal
  FROM mentions_stats m
  CROSS JOIN stats_calc s
),

-- Étape 4 : Agrégation des ventes par jour
ventes_par_jour AS (
  SELECT
    d.date AS date_commande,                          -- Date de la commande (via dim_date)
    SUM(dc.quantite) AS volume_ventes                  -- Quantité totale vendue ce jour
  FROM {{ ref('dim_details_commandes') }} dc
  JOIN {{ ref('facts_commandes') }} c
    ON dc.id_commande = c.id_commande
  JOIN {{ ref('dim_date') }} d
    ON d.id_date = c.id_date_commande
  GROUP BY d.date
),

-- Étape 5 : Fusion des données de mentions et de ventes
data_combinee AS (
  SELECT
    m.date_post AS date,
    m.total_mentions,
    m.pic_mention,
    COALESCE(v.volume_ventes, 0) AS volume_ventes     -- Ventes ce jour (0 si aucune)
  FROM mentions_avec_pic m
  LEFT JOIN ventes_par_jour v
    ON m.date_post = v.date_commande
)

-- Étape 6 : Résumé final – comparaison jours avec pic vs jours normaux
SELECT
  pic_mention,                                        -- 1 = jour avec pic, 0 = jour sans pic
  COUNT(*) AS nb_jours,                               -- Nombre total de jours dans chaque catégorie
  ROUND(AVG(total_mentions), 2) AS mentions_moyennes, -- Moyenne des mentions
  ROUND(AVG(volume_ventes), 2) AS ventes_moyennes     -- Moyenne des ventes
FROM data_combinee
GROUP BY pic_mention
ORDER BY pic_mention DESC