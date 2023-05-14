from pydantic import BaseModel
from fastapi import FastAPI, Path, Query, status, HTTPException, Body
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import numpy as np
from gtts import gTTS
import asyncio
import requests
import json
import pandas as pd
import datetime

app = FastAPI()

#/recommendations/<ADV>/<Modelo>
#Esta entrada devuelve un JSON en dónde se indican las recomendaciones del
#día para el adv y el modelo en cuestión.

@app.get("/recommendations/{ADV}/{Modelo}")
def advertiser(ADV: str, Modelo: str):
	if(Modelo == "top_ctr"):
		resultados = pd.read_csv('/home/pepino/Desktop/Maestría Udesa/Segundo Año/Programación Avanzada para grandes volúmenes de datos/TP/resultados_top_ctr.csv', header = 0)
	
	elif(Modelo == "top_product"):
		resultados = resultados = pd.read_csv('/home/pepino/Desktop/Maestría Udesa/Segundo Año/Programación Avanzada para grandes volúmenes de datos/TP/resultados_top_product.csv', header = 0)

	filtrado = resultados[(resultados['advertiser_id'] == ADV) & (resultados['log'] == resultados['log'].max())]
	return {"advertiser_id": ADV,
	 	    "modelo" : Modelo,
		    "product_id": filtrado['product_id']}

#/stats/
#Esta entrada devuelve un JSON con un resumen de estadísticas sobre las
#recomendaciones a determinar por ustedes. Algunas opciones posibles:
#● Cantidad de advertisers 
#● Advertisers que más varían sus recomendaciones por día
#● Estadísticas de coincidencia entre ambos modelos para los diferentes
#  advs.

# Agregaría el advertiser que tuvo más tiempo seguido recomendando un mismo producto
# El producto más recomendado hasta el momento
# Los productos que sólo recomendo el modelo 1
# Los productos que sólo recomendo el modelo 2

@app.get("/stats/")
def root():
	# Esto lo haría sobre la tabla acumulada, porque justamente son las estadísticas que vas teniendo a medida que vas acumulando data
	resultados_totales_top_ctr = pd.read_csv('/home/pepino/Desktop/Maestría Udesa/Segundo Año/Programación Avanzada para grandes volúmenes de datos/TP/resultados_totales_top_ctr.csv', header = 0)
	resultados_totales_top_product = pd.read_csv('/home/pepino/Desktop/Maestría Udesa/Segundo Año/Programación Avanzada para grandes volúmenes de datos/TP/resultados_totales_top_product.csv', header = 0)

	# Hacemos las estadísticas

	# Unimos, pero me va a generar 20 rows de más por cada producto
	union = resultados_totales_top_ctr.merge(resultados_totales_top_product, on = ['log', 'advertiser_id'], suffixes=['_top_ctr', '_top_product'])

	# Hago un agrupamiento por advertiser y cuento el número de coincidencias
	conteo_coincidencias = union.groupby(['advertiser_id','log']).apply(lambda x: (x['product_id_top_ctr'] == x['product_id_top_product']).sum())

	# Calculo la coincidencia por cada advertiser: (coincidencias / coincidencias_posibles) %
	porcentaje_coincidencia = conteo_coincidencias / 20 * 100

	# Armamos unas base con las coincidencias
	porcentajes_adv_log = porcentaje_coincidencia.reset_index()
	porcentajes_adv_log.rename(columns = {0:'percen'}, inplace = True)

	# (1) Hacemos la coincidencia promedio por advertiser
	porcen_agroup = porcentajes_adv_log.groupby('advertiser_id')['percen'].mean().reset_index()

	# (2) Hacemos los desvíos de las coincidencias
	std_agroup = porcentajes_adv_log.groupby('advertiser_id')['percen'].std().reset_index()
	std_agroup.rename(columns = {'percen':'std'}, inplace = True)

	# (3) Agarramos los 5 advertisers con mayor porcentaje de coincidencia promedio entre ambos modelos
	adv_con_mayor_coincidencia = porcen_agroup.reset_index().sort_values(by = 'percen', ascending = False).reset_index()[['advertiser_id','percen']].head(5)

	# (4) Agarramos los 5 advertisers con mayor variabilidad de coincidencia promedio entre ambos modelos
	adv_con_mayor_variabilidad = std_agroup.reset_index().sort_values(by = 'std', ascending = False).reset_index()[['advertiser_id','std']].head(5)

	# (5) Productos únicos: Vemos los productos que sólo son recomendados para un modelo y no para el otro
	productos_top_ctr = set(resultados_totales_top_ctr['product_id'].unique())
	productos_top_product = set(resultados_totales_top_product['product_id'].unique())

	# Productos exclusivos de TopCTR
	exclusivos_top_ctr = list(productos_top_ctr - productos_top_product)
	
	# Productos exclusivos de TopProduct
	exclusivos_top_product = list(productos_top_product - productos_top_ctr)

	# (6) 10 Producto más recomendado históricamente, independientemente del modelo
	recomendaciones_totales = pd.concat([resultados_totales_top_ctr, resultados_totales_top_product], ignore_index=True)
	mas_repetidos_hist = recomendaciones_totales.groupby('product_id')['product_id'].value_counts().reset_index().sort_values(by = 'count', ascending = False)

	# Renombramos y preparamos para guardar la salida
	mas_repetidos_hist.rename(columns = {'count':'repeticiones'}, inplace = True)
	mas_repetidos_hist = mas_repetidos_hist.head(10).reset_index()[['product_id','repeticiones']]

	# (7) 10 Producto más recomendado en los últimos 7 días, independientemente del modelo
	fechas_ordenadas = sorted(list(recomendaciones_totales['log'].unique()), reverse = True)[0:7]

	recomendaciones_totales_ult_7dias = recomendaciones_totales[recomendaciones_totales['log'].isin(fechas_ordenadas)]
	mas_repetidos_ult_7dias = recomendaciones_totales_ult_7dias.groupby('product_id')['product_id'].value_counts().reset_index().sort_values(by = 'count', ascending = False)

	# Renombramos y preparamos para guardar la salida
	mas_repetidos_ult_7dias.rename(columns = {'count':'repeticiones'}, inplace = True)
	mas_repetidos_ult_7dias = mas_repetidos_ult_7dias.head(10).reset_index()[['product_id','repeticiones']]

	# (8) Producto que estuvo más días seguidos recomendado
	
	# Caso del modelo de TopCTR
	# Nos quedamos con lo que son las fechas y el producto, eliminando duplicados en la base
	base_productos_dia_ctr = resultados_totales_top_ctr[['log','product_id']].drop_duplicates()
	base_productos_dia_ctr['date'] = pd.to_datetime(base_productos_dia_ctr['log']) # Lo casteamos a datetime para que podamos hacer lo que serían los delta de fechas

	# Acomodamos para poder sacar las diferencias 
	base_productos_dia_ctr = base_productos_dia_ctr.sort_values(by = 'date')
	groups = base_productos_dia_ctr.groupby('product_id')
	base_productos_dia_ctr['diff'] = groups['date'].diff()

	# Casteamos a númerico para que después sea más fácil hacer las operaciones
	base_productos_dia_ctr['diff'] = base_productos_dia_ctr['diff'].astype('timedelta64[ns]').dt.days

	# Obtenemos lo que es el producto que tuvo mayor cantidad de días consecutivos como producto recomendado
	producto_con_mas_dias_continuo_top_ctr = base_productos_dia_ctr[base_productos_dia_ctr['diff'] == 1].groupby('product_id').agg({'diff':'count'}).idxmax()[0]

	# Caso del modelo de TopProduct
	# Nos quedamos con lo que son las fechas y el producto, eliminando duplicados en la base
	base_productos_dia_prod = resultados_totales_top_product[['log','product_id']].drop_duplicates()
	base_productos_dia_prod['date'] = pd.to_datetime(base_productos_dia_prod['log']) # Lo casteamos a datetime para que podamos hacer lo que serían los delta de fechas

	# Acomodamos para poder sacar las diferencias 
	base_productos_dia_prod = base_productos_dia_prod.sort_values(by = 'date')
	groups = base_productos_dia_prod.groupby('product_id')
	base_productos_dia_prod['diff'] = groups['date'].diff()

	# Casteamos a númerico para que después sea más fácil hacer las operaciones
	base_productos_dia_prod['diff'] = base_productos_dia_prod['diff'].astype('timedelta64[ns]').dt.days

	# Obtenemos lo que es el producto que tuvo mayor cantidad de días consecutivos como producto recomendado
	producto_con_mas_dias_continuo_top_prod = base_productos_dia_prod[base_productos_dia_prod['diff'] == 1].groupby('product_id').agg({'diff':'count'}).idxmax()[0]


	# Resultados
	return {"Porcentaje de coincidencia promedio entre modelos": np.round(porcen_agroup['percen'].mean(),2),
	 	"5 Advertisers con mayor coincidencia promedio": adv_con_mayor_coincidencia,
		"5 Advertisers con mayor variabilidad promedio de recomendaciones": adv_con_mayor_variabilidad,
		"Cantidad de productos recomendados exclusivamente para TopCTR": len(exclusivos_top_ctr),
		"Productos exclusivos de TopCTR": exclusivos_top_ctr,
		"Cantidad de productos recomendados exclusivamente para TopProduct": len(exclusivos_top_product),
		"Productos exclusivos de TopProduct": exclusivos_top_product,
		"10 Productos más recomendados históricamente, independientemente del modelo": mas_repetidos_hist.to_dict(),
		"10 Productos más recomendados en últimos 7 días, independientemente del modelo": mas_repetidos_ult_7dias.to_dict(),
		"Productos que se recomendó más tiempo seguido con el modelo de TopCTR": producto_con_mas_dias_continuo_top_ctr,
		"Productos que se recomendó más tiempo seguido con el modelo de TopProduct": producto_con_mas_dias_continuo_top_prod}

#/history/<ADV>/
# Esta entrada devuelve un JSON con todas las recomendaciones para el
# advertiser pasado por parámetro en los últimos 7 días.

@app.get("/history/{ADV}/")
def advertiser(ADV: str):
	# Levantamos los resultados de cada modelo
	# TopCTR
	resultados_top_ctr = pd.read_csv('/home/pepino/Desktop/Maestría Udesa/Segundo Año/Programación Avanzada para grandes volúmenes de datos/TP/resultados_totales_top_ctr.csv', header = 0)
	resultados_top_ctr = resultados_top_ctr[(resultados_top_ctr['advertiser_id'] == ADV)]
	
	# TopProduct
	resultados_top_product = pd.read_csv('/home/pepino/Desktop/Maestría Udesa/Segundo Año/Programación Avanzada para grandes volúmenes de datos/TP/resultados_totales_top_product.csv', header = 0)
	resultados_top_product = resultados_top_product[(resultados_top_product['advertiser_id'] == ADV)]

	# Compactamos por el advertiser que se requiere
	resultados_adv = pd.concat([resultados_top_ctr, resultados_top_product], ignore_index = True)

	# Agarramos los últimos 7 días
	fechas_ordenadas = sorted(list(resultados_adv['log'].unique()), reverse = True)[0:7]

	# Filtramos los productos recomendados para el advertiser en esos días
	filtrado = resultados_adv[resultados_adv['log'].isin(fechas_ordenadas)]['product_id'].unique()

	return {"advertiser_id": ADV,
	 	    "product_id": list(filtrado)}
