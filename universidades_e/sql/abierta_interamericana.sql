SELECT 
	univiersities,
	carrera,
	CAST(inscription_dates as DATE),
	names,
	sexo,
	fechas_nacimiento,
	direcciones,
	email 
FROM 
	rio_cuarto_interamericana 
WHERE 
	cast(inscription_dates as DATE) BETWEEN '2020-09-01' AND '2021-02-01' 
AND 
	univiersities = '-universidad-abierta-interamericana';