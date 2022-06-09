SELECT nombre, 
	   sexo, 
	   direccion, 
	   email, 
	   birth_date, 
	   university, 
	   inscription_date, 
	   career, 
	   "location"
FROM jujuy_utn
WHERE university LIKE 'universidad tecnológica nacional'
AND CAST(inscription_date as DATE) BETWEEN CAST('2020/09/01' as DATE) AND CAST('2021/02/01' as DATE)
ORDER BY inscription_date