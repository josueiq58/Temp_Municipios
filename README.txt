Hay dos módulos de data_processor, se comparte la vesión que corre el promedio de temperatura y precipitación de las últimas dos horas, pero 
el otro módulo considera la segunda versión del proyecto compartida con las siguientes obervaciones:Se consideró para el promedio un intervalo de 2 dias en vez
de 2 horas porque los datos por hora no contaban con tmax y tmin. Esto aplica para la segunda versión del proyecto compartida. 

Se deja en Current y en Historico una muestra de las salidas consideradas. 

La respuesta de la API a veces no es correcta, hay que reejecutar para obtener los archivos comprimidos correctamente. Para intentar solventar
esta oportunidad, se diseño un ciclo con el cual damos hasta 5 opotunidades al sitio para que nos genere la respuesta. 

Observarán que el modulo logger contiene la opción de manejo de errores, por falta de tiempo no se implementó esta parte, pero correría como 
parte de las mejoras al proceso para la siguiente versión. Adicional, se propone la necesidad de manejar un scheduler para evitar la dependencia
manual en la ejecución. Una propuesta final sería incluir un modulo de notificaicones del proceso, que te mande ya sea una notificación de ejecución exitosa
o en por el contrario cuando exista un error.

Finalmente, como parte de la entrega se buscaría conocer el producto final que consume la data con la finalidad de dejar salidas que sean optimas 
y directamente consumibles. 


