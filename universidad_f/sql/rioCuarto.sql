
SELECT univiersities, 
       carrera, 
       inscription_dates, 
       names, 
       sexo, 
       fechas_nacimiento, 
       localidad, 
       direcciones, 
       email 

FROM rio_cuarto_interamericana
WHERE univiersities LIKE 'Universidad-nacional-de-río-cuarto' AND TO_DATE(inscription_dates, 'YY/Mon/DD') BETWEEN TO_DATE('20/Sep/01', 'YY/Mon/DD') AND TO_DATE('21/Feb/01', 'YY/Mon/DD');
