from machine import Pin, ADC
import machine
import time
import utime
from time import sleep
import network
import urequests
import random
import json

def fecha_artificial(muestra):
    """
    Retorna la fecha formateada 'yy-mm-dd hh-mm-ss' para la muestra indicada.
    La primera muestra se asigna a partir del 19-03-2025 00:00:00 y cada
    muestra posterior aumenta 2 horas.
    """
    inicio = utime.mktime((2025, 3, 19, 0, 0, 0, 0, 0))
    timestamp = inicio + 7200 * (muestra - 1)  # 2 horas = 7200 segundos
    t = utime.localtime(timestamp)
    return "{:02d}-{:02d}-{:02d} {:02d}-{:02d}-{:02d}".format(t[0] % 100, t[1], t[2], t[3], t[4], t[5])

def enviar_datos(url_firestore, data):
    """
    Prepara y envía los datos a Firestore en formato JSON.
    """
    json_data = {
        "fields": {
            "Monedas100": {"integerValue": str(data["Monedas100"])},
            "Monedas200": {"integerValue": str(data["Monedas200"])},
            "Monedas500": {"integerValue": str(data["Monedas500"])},
            "Monedas1000": {"integerValue": str(data["Monedas1000"])},
            "PesoCaja100": {"integerValue": str(data["PesoCaja100"])},
            "PesoCaja200": {"integerValue": str(data["PesoCaja200"])},
            "PesoCaja500": {"integerValue": str(data["PesoCaja500"])},
            "PesoCaja1000": {"integerValue": str(data["PesoCaja1000"])},
            "ErrorClasificacion": {"doubleValue": data["ErrorClasificacion"]},
            "Fecha": {"stringValue": data["Fecha"]}
        }
    }
    
    response = urequests.post(url_firestore, json=json_data)
    print("Datos enviados a Firestore:")
    print(data)
    print("Código de respuesta:", response.status_code)
    print("Respuesta del servidor:", response.text)
    response.close()

def main():
    # Configuración de la conexión WiFi
    ssid = "POCO F5"
    password = "mypoderes"
    
    print("Intentando conexión WiFi...")
    wlan = network.WLAN(network.STA_IF)
    wlan.active(True)
    wlan.connect(ssid, password)
    
    while not wlan.isconnected():
        time.sleep(1)
    print("Conexión establecida:", wlan.ifconfig())
    
    # URL del servidor donde se enviarán los datos (API REST)
    url = "https://firestore.googleapis.com/v1/projects/conteomonedas-2a1e5/databases/(default)/documents/ConteoMonedas"
    
    # Valores base y de incremento para los datos simulados
    base_m100 = 0
    base_m200 = 0
    base_m500 = 0
    base_m1000 = 0
    base_p100 = 100
    base_p200 = 100
    base_p500 = 100
    base_p1000 = 100

    step_m100 = 1
    step_m200 = 1
    step_m500 = 1
    step_m1000 = 1
    step_p100 = 10
    step_p200 = 10
    step_p500 = 10
    step_p1000 = 10

    # Generar y enviar 100 muestras
    for muestra in range(1, 101):
        # Ruido para monedas y peso
        ruido_m100 = random.randint(-1, 1)
        ruido_m200 = random.randint(-1, 1)
        ruido_m500 = random.randint(-1, 1)
        ruido_m1000 = random.randint(-1, 1)
        
        ruido_p100 = random.randint(-5, 5)
        ruido_p200 = random.randint(-5, 5)
        ruido_p500 = random.randint(-5, 5)
        ruido_p1000 = random.randint(-5, 5)
        
        data_artificial = {
            "Monedas100": base_m100 + step_m100 * muestra + ruido_m100,
            "Monedas200": base_m200 + step_m200 * muestra + ruido_m200,
            "Monedas500": base_m500 + step_m500 * muestra + ruido_m500,
            "Monedas1000": base_m1000 + step_m1000 * muestra + ruido_m1000,
            "PesoCaja100": base_p100 + step_p100 * muestra + ruido_p100,
            "PesoCaja200": base_p200 + step_p200 * muestra + ruido_p200,
            "PesoCaja500": base_p500 + step_p500 * muestra + ruido_p500,
            "PesoCaja1000": base_p1000 + step_p1000 * muestra + ruido_p1000,
            "ErrorClasificacion": random.uniform(0, 0.3),  # Error como porcentaje entre 0 y 0.3
            "Fecha": fecha_artificial(muestra)
        }
        
        enviar_datos(url, data_artificial)
        # Se espera 1 segundo entre envíos en esta simulación (en la práctica, el intervalo es de 2 horas)
        time.sleep(1)

if __name__ == "__main__":
    print("Iniciando generación de datos escalonados...")
    main()
