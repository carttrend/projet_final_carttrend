-- ============================================================================================
-- Analyse de l’impact des pics de mentions sur les réseaux sociaux sur les performances ventes
-- ============================================================================================
-- Objectif :
-- Cette analyse vise à identifier les jours avec un pic anormal de mentions sociales (volume élevé)
-- et à mesurer s’il existe une différence significative du volume de ventes entre ces jours et les autres.
-- Cela permet d’évaluer une éventuelle corrélation entre activité sociale et ventes.
-- ============================================================================================

-- Étape 1 : Calcul du nombre total de mentions par jour
WITH mentions_stats AS (
  SELECT
    d1.date AS date_jour,                  -- Date (issue de dim_date)
    SUM(f.volume_mentions) AS total_mentions -- Volume total de mentions ce jour-là
  FROM {{ ref('facts_posts') }} f
  JOIN {{ ref('dim_date') }} d1 ON d1.id_date = f.id_date

  GROUP BY d1.date
  

),

-- Étape 2 : Calcul de la moyenne et de l’écart-type des mentions journalières
stats_calc AS (
  SELECT
    AVG(total_mentions) AS moyenne_mentions,           -- Moyenne des mentions par jour
    STDDEV(total_mentions) AS ecart_type_mentions      -- Écart-type des mentions
  FROM mentions_stats
),

-- Étape 3 : Identification des jours "pic de mentions"
mentions_avec_pic AS (
  SELECT
    d1.date,
    m.total_mentions,
    CASE
      WHEN m.total_mentions > s.moyenne_mentions + 2 * s.ecart_type_mentions THEN 1
      ELSE 0
    END AS pic_mention                                 -- 1 si pic détecté, 0 sinon
  FROM mentions_stats m
  JOIN {{ ref('dim_date') }} d1 ON d1.date = m.date_jour
  CROSS JOIN stats_calc s
),

-- Étape 4 : Calcul du volume total de ventes par jour
ventes_par_jour AS (
  SELECT
    d.date AS date_jour,                               -- Date de la commande
    SUM(dc.quantite) AS volume_ventes                  -- Volume total vendu ce jour
  FROM {{ ref('dim_details_commandes') }} dc
  JOIN {{ ref('facts_commandes') }} c ON dc.id_commande = c.id_commande
  JOIN {{ ref('dim_date') }} d ON c.id_date_commande = d.id_date
  GROUP BY d.date
),

-- Étape 5 : Jointure des données mentions + ventes sur la date
data_combinee AS (
  SELECT
    d1.date,
    m.total_mentions,
    m.pic_mention,
    COALESCE(v.volume_ventes, 0) AS volume_ventes      -- Remplace NULL par 0 si pas de ventes
  FROM mentions_avec_pic m
  JOIN {{ ref('dim_date') }} d1 ON m.date = d1.date
  LEFT JOIN ventes_par_jour v ON d1.date = v.date_jour

)

-- Étape 6 : Agrégation finale par type de jour (pic ou non)
SELECT
  pic_mention,                             -- 1 = jour de pic de mentions, 0 = jour normal
  COUNT(*) AS nb_jours,                    -- Nombre de jours dans chaque catégorie
  ROUND(AVG(total_mentions), 2) AS mentions_moyennes,  -- Moyenne des mentions
  ROUND(AVG(volume_ventes), 2) AS ventes_moyennes      -- Moyenne des ventes
FROM data_combinee
GROUP BY pic_mention
ORDER BY pic_mention DESC