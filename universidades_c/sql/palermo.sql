SELECT universidad,
       careers,
       fecha_de_inscripcion,
       names,
       sexo,
       birth_dates,
       codigo_postal,
       direcciones,
       correos_electronicos
FROM palermo_tres_de_febrero
WHERE universidad LIKE '_universidad_de_palermo'
AND TO_DATE(fecha_de_inscripcion,'DD/MON/YY') BETWEEN '01/Sep/20' AND '01/Feb/21'