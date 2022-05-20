SELECT universidad, 
       carrerra, 
       fechaiscripccion, 
       nombrre, 
       sexo, 
       nacimiento, 
       codgoposstal, 
       direccion, 
       eemail
FROM moron_nacional_pampa
WHERE universidad LIKE 'Universidad de morón' AND TO_DATE(fechaiscripccion, 'DD/MM/YYYY') BETWEEN TO_DATE('01/09/2020', 'DD/MM/YYYY') AND TO_DATE('01/02/2021', 'DD/MM/YYYY');
