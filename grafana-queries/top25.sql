SELECT 
  CONCAT(expansion, concat(' - S', cast(season as varchar(10)))) AS season_label,
  p750 as "Mythic Plus Score"
FROM 
  raiderio_prod_table 
ORDER BY 
  full_season;