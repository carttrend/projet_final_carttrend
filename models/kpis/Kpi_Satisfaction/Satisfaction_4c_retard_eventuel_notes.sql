SELECT
  FORMAT_DATE('%Y-%m', dc.date) AS mois_commande,
  CASE 
    WHEN LOWER(c.statut_commande) = 'en transit' 
         AND dl.date < CURRENT_DATE()
    THEN 'Retard probable'
    WHEN LOWER(c.statut_commande) = 'annulée'
    THEN 'Annulée'
    ELSE 'Dans les délais ou terminée'
  END AS situation_livraison,
  ROUND(AVG(s.note_client), 2) AS note_moyenne,
  COUNT(*) AS nb_commandes
FROM {{ ref('facts_commandes') }} c
JOIN {{ ref('dim_satisfaction') }} s
  ON c.id_commande = s.id_commande
JOIN {{ ref('dim_date') }} dc
  ON c.id_date_commande = dc.id_date
JOIN {{ ref('dim_date') }} dl
  ON c.id_date_livraison = dl.id_date
WHERE s.note_client IS NOT NULL
GROUP BY mois_commande, situation_livraison
ORDER BY mois_commande, situation_livraison