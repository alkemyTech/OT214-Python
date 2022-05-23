SELECT name, 
		sexo, 
		fecha_nacimiento, 
		direccion, 
		codigo_postal, 
		correo_electronico, 
		universidad, 
		carrera, 
		fecha_de_inscripcion   
FROM flores_comahue
WHERE universidad = 'UNIV. NACIONAL DEL COMAHUE' 
AND CAST (fecha_de_inscripcion AS DATE) BETWEEN '01/SEP/2020'  AND '01/FEB/2021'