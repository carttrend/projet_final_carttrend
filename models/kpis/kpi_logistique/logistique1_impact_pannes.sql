-- Impact des pannes sur les volumes traités et identification des machines les plus souvent en panne
SELECT 
  c.id_machine,
  d.localisation,
  COUNT(CASE 
           WHEN c.etat_machine IN ('En panne', 'En maintenance') 
           THEN c.temps_d_arret 
        END) AS nombre_de_pannes,
  SUM(CASE 
        WHEN c.etat_machine IN ('En panne', 'En maintenance') 
        THEN c.temps_d_arret 
        ELSE 0 
      END) AS temps_total_panne_ou_maintenance,
  SUM(c.volume_traite) AS volume_total_traite
FROM {{ ref('facts_entrepots_machine') }} AS c
JOIN {{ ref('dim_entrepots') }} AS d
  ON c.id_entrepot = d.id_entrepot
GROUP BY c.id_machine, d.localisation
ORDER BY temps_total_panne_ou_maintenance DESC