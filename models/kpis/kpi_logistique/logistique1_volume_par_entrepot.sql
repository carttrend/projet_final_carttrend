-- Volume total traité par entrepôt
SELECT 
  c.id_entrepot,              -- Identifiant de l'entrepôt
  d.localisation,             -- Localisation géographique de l'entrepôt
  SUM(c.volume_traite) AS volume_total_traite       -- Total du volume traité
FROM {{ ref('facts_entrepots_machine') }} AS c
JOIN {{ ref('dim_entrepots') }} AS d
  ON c.id_entrepot = d.id_entrepot
GROUP BY c.id_entrepot, d.localisation
ORDER BY SUM(volume_traite) DESC