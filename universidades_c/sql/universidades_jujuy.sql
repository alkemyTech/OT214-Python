SELECT university,
       career,
       inscription_date,
       nombre,
       sexo,
       birth_date,
       location,
       direccion,
       email
FROM jujuy_utn 
WHERE university LIKE 'universidad nacional de jujuy'
AND inscription_date BETWEEN '2020/09/01' AND '2021/02/01'