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

@app.get("/stats/")
def root():
	resultados_top_ctr = pd.read_csv('/home/pepino/Desktop/Maestría Udesa/Segundo Año/Programación Avanzada para grandes volúmenes de datos/TP/resultados_top_ctr.csv', header = 0)
	resultados_top_product = pd.read_csv('/home/pepino/Desktop/Maestría Udesa/Segundo Año/Programación Avanzada para grandes volúmenes de datos/TP/resultados_top_product.csv', header = 0)

	# Hacemos las estadísticas
	# Cantidad de adversiter por modelo
	adv_top_ctr  = resultados_top_ctr['advertiser_id'].nunique()
	adv_top_prod = resultados_top_product['advertiser_id'].nunique()

	# Coincidencia
	return {"# Advertisers - Top CTR": adv_top_ctr,
	 		"# Advertisers - Top Product": adv_top_prod,}

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
	resultados_adv = pd.concat([resultados_top_ctr, resultados_top_product], ignore_index=True)

	# Agarramos los últimos 7 días
	fechas_ordenadas = sorted(list(resultados_adv['log'].unique()), reverse = True)[0:7]

	# Filtramos los productos recomendados para el advertiser en esos días
	filtrado = resultados_adv[resultados_adv['log'].isin(fechas_ordenadas)]['product_id'].unique()

	return {"advertiser_id": ADV,
	 	    "product_id": list(filtrado)}
