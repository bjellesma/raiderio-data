SELECT 
  CONCAT(expansion, concat(' - S', cast(season as varchar(10)))) AS season_label,
  population as "Total # of Players",
  p999_population as "Population in Percentile",
  p999 as "Mythic Plus Score"
FROM 
  raiderio_prod_table 
WHERE
  full_season NOT IN (1,2)
ORDER BY 
  full_season;