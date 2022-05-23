SELECT 
	universidad, 
	carrerra, 
	fechaiscripccion, 
	nombrre, 
	sexo,
	nacimiento, 
	codgoposstal, 
	direccion, 
	eemail
FROM 
	moron_nacional_pampa 
WHERE 
	to_date(fechaiscripccion,'DD-MM-YYYY') BETWEEN '2020-09-01' AND '2021-02-01' 
AND 
	universidad='Universidad nacional de la pampa';
    