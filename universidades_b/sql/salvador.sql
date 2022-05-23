SELECT nombre, 
		sexo, 
		fecha_nacimiento, 
		direccion, 
		localidad, 
		email, 
		universidad, 
		carrera, 
		fecha_de_inscripcion
FROM salvador_villa_maria
WHERE universidad = 'UNIVERSIDAD_DEL_SALVADOR' 
AND CAST (fecha_de_inscripcion AS DATE) BETWEEN '01/SEP/2020'  AND '01/FEB/2021'