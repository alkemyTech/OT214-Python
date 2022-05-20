SELECT "names", 
		sexo, 
		direcciones, 
		correos_electronicos, 
		birth_dates, 
		universidad, 
		fecha_de_inscripcion, 
		careers, 
		codigo_postal
FROM palermo_tres_de_febrero
WHERE universidad LIKE 'universidad_nacional_de_tres_de_febrero'
AND CAST(fecha_de_inscripcion as DATE) BETWEEN CAST('01/Aug/20' as DATE) AND CAST('01/FEB/21' as DATE)
ORDER BY CAST(fecha_de_inscripcion as DATE)