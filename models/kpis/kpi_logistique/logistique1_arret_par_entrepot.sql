-- Temps d'arrêt total par entrepôt
SELECT 
  c.id_entrepot,
  d.localisation,
  SUM(c.temps_d_arret) as temps_d_arret_total
FROM {{ ref('facts_entrepots_machine') }} AS c
JOIN {{ ref('dim_entrepots') }} AS d
  ON c.id_entrepot = d.id_entrepot
GROUP BY c.id_entrepot, d.localisation
ORDER BY SUM(c.temps_d_arret) DESC