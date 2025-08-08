-- ----------------------------------------------------------------------------------------
-- Étude de corrélation approximative entre pics de mentions et satisfaction client
-- ----------------------------------------------------------------------------------------
-- Objectif :
-- 1) Calculer la moyenne et écart-type des mentions quotidiennes
-- 2) Identifier les jours "pic" où mentions > moyenne + 2 * écart-type
-- 3) Comparer la satisfaction moyenne lors des jours de pic et autres jours
-- ----------------------------------------------------------------------------------------

WITH mentions_stats AS (
  SELECT
    d.date AS date_post,                      -- Date normalisée depuis la dimension date
    COUNT(*) AS total_mentions                 -- Nombre total de mentions enregistrées ce jour-là
  FROM {{ ref('facts_posts') }} f
  JOIN {{ ref('dim_date') }} d ON d.id_date = f.id_date  -- Jointure sur la dimension date
  GROUP BY d.date                             -- Regroupe les données par date standardisée
),

stats_calc AS (
  SELECT
    AVG(total_mentions) AS moyenne_mentions,       -- Moyenne des mentions quotidiennes
    STDDEV(total_mentions) AS ecart_type_mentions   -- Écart-type des mentions quotidiennes
  FROM mentions_stats                              -- Calcul basé sur mentions_stats
),

mentions_avec_pic AS (
  SELECT
    m.date_post,                          -- Date des mentions
    m.total_mentions,                     -- Nombre total de mentions ce jour
    CASE
      WHEN m.total_mentions > s.moyenne_mentions + 2 * s.ecart_type_mentions THEN 1  -- Pic si mentions > moyenne + 2*écart-type
      ELSE 0                             -- Sinon pas de pic
    END AS pic_mention                    -- Indicateur binaire (1 = pic, 0 = pas pic)
  FROM mentions_stats m
  CROSS JOIN stats_calc s                  -- Ajoute moyenne et écart-type pour comparaison
),

satisfaction_par_jour AS (
  SELECT
    d.date AS date,                       -- Date normalisée depuis dim_date
    ROUND(AVG(s.note_client), 2) AS note_moyenne  -- Note moyenne client arrondie
  FROM {{ ref('dim_satisfaction') }} s
  JOIN {{ ref('facts_commandes') }} c
    ON s.id_commande = c.id_commande      -- Association des notes aux commandes
  JOIN {{ ref('dim_date') }} d ON d.id_date = c.id_date_commande  -- Jointure sur dim_date pour date standardisée
  GROUP BY d.date                         -- Regroupement par date standardisée
),

data_combinee AS (
  SELECT
    m.date_post AS date,                  -- Date des mentions (standardisée)
    m.total_mentions,                     -- Total des mentions
    m.pic_mention,                       -- Indicateur pic mentions (1 ou 0)
    COALESCE(s.note_moyenne, NULL) AS note_moyenne  -- Note moyenne client (NULL si absente)
  FROM mentions_avec_pic m
  LEFT JOIN satisfaction_par_jour s ON m.date_post = s.date  -- Fusion des mentions et notes sur la date
)

SELECT
  pic_mention,                           -- 0 = jour normal, 1 = jour pic de mentions
  COUNT(*) AS nb_jours,                  -- Nombre de jours par catégorie
  ROUND(AVG(total_mentions), 2) AS mentions_moyennes,  -- Moyenne mentions par catégorie
  ROUND(AVG(note_moyenne), 2) AS satisfaction_moyenne  -- Moyenne satisfaction client par catégorie
FROM data_combinee
GROUP BY pic_mention                     -- Regroupement par indicateur de pic
ORDER BY pic_mention DESC                -- Affiche en premier les jours avec pic