from machine import Pin, enable_irq, disable_irq, idle, ADC
import machine
import time
import utime
from time import sleep
import network
import urequests
import random
import json


class HX711:
    def __init__(self, dout, pd_sck, gain=128):

        self.pSCK = Pin(pd_sck , mode=Pin.OUT)
        self.pOUT = Pin(dout, mode=Pin.IN, pull=Pin.PULL_DOWN)
        self.pSCK.value(False)

        self.GAIN = 0
        self.OFFSET = 0
        self.SCALE = 1
        
        self.time_constant = 0.1
        self.filtered = 0

        self.set_gain(gain);

    def set_gain(self, gain):
        if gain is 128:
            self.GAIN = 1
        elif gain is 64:
            self.GAIN = 3
        elif gain is 32:
            self.GAIN = 2

        self.read()
        self.filtered = self.read()
        print('Gain & initial value set')
    
    def is_ready(self):
        return self.pOUT() == 0

    def read(self):
        # wait for the device being ready
        while self.pOUT() == 1:
            idle()

        # shift in data, and gain & channel info
        result = 0
        for j in range(24 + self.GAIN):
            state = disable_irq()
            self.pSCK(True)
            self.pSCK(False)
            enable_irq(state)
            result = (result << 1) | self.pOUT()

        # shift back the extra bits
        result >>= self.GAIN

        # check sign
        if result > 0x7fffff:
            result -= 0x1000000

        return result

    def read_average(self, times=3):
        sum = 0
        for i in range(times):
            sum += self.read()
        return sum / times

    def make_average(self, times=40):
        sum = 0
        for i in range(times):
            sum += self.read()
        ssum = (sum / times) /1000
        return '%.1f' %(ssum)
        
    def read_lowpass(self):
        self.filtered += self.time_constant * (self.read() - self.filtered)
        return self.filtered

    def get_value(self, times=3):
        return self.read_average(times) - self.OFFSET

    def get_units(self, times=3):
        return self.get_value(times) / self.SCALE

    def tare(self, times=15):
        sum = self.read_average(times)
        self.set_offset(sum)

    def set_scale(self, scale):
        self.SCALE = scale

    def set_offset(self, offset):
        self.OFFSET = offset

    def set_time_constant(self, time_constant = None):
        if time_constant is None:
            return self.time_constant
        elif 0 < time_constant < 1.0:
            self.time_constant = time_constant

    def power_down(self):
        self.pSCK.value(False)
        self.pSCK.value(True)

    def power_up(self):
        self.pSCK.value(False)
        
class HCSR04:
    """
    Driver to use the untrasonic sensor HC-SR04.
    The sensor range is between 2cm and 4m.
    The timeouts received listening to echo pin are converted to OSError('Out of range')
    """
    # echo_timeout_us is based in chip range limit (400cm)
    def __init__(self, trigger_pin, echo_pin, echo_timeout_us=500*2*30):
        """
        trigger_pin: Output pin to send pulses
        echo_pin: Readonly pin to measure the distance. The pin should be protected with 1k resistor
        echo_timeout_us: Timeout in microseconds to listen to echo pin. 
        By default is based in sensor limit range (4m)
        """
        self.echo_timeout_us = echo_timeout_us
        # Init trigger pin (out)
        self.trigger = Pin(trigger_pin, mode=Pin.OUT, pull=None)
        self.trigger.value(0)

        # Init echo pin (in)
        self.echo = Pin(echo_pin, mode=Pin.IN, pull=None)

    def _send_pulse_and_wait(self):
        """
        Send the pulse to trigger and listen on echo pin.
        We use the method `machine.time_pulse_us()` to get the microseconds until the echo is received.
        """
        self.trigger.value(0) # Stabilize the sensor
        time.sleep_us(5)
        self.trigger.value(1)
        # Send a 10us pulse.
        time.sleep_us(10)
        self.trigger.value(0)
        try:
            pulse_time = machine.time_pulse_us(self.echo, 1, self.echo_timeout_us)
            return pulse_time
        except OSError as ex:
            if ex.args[0] == 110: # 110 = ETIMEDOUT
                raise OSError('Out of range')
            raise ex

    def distance_mm(self):
        """
        Get the distance in milimeters without floating point operations.
        """
        pulse_time = self._send_pulse_and_wait()

        # To calculate the distance we get the pulse_time and divide it by 2 
        # (the pulse walk the distance twice) and by 29.1 becasue
        # the sound speed on air (343.2 m/s), that It's equivalent to
        # 0.34320 mm/us that is 1mm each 2.91us
        # pulse_time // 2 // 2.91 -> pulse_time // 5.82 -> pulse_time * 100 // 582 
        mm = pulse_time * 100 // 582
        return mm

    def distance_cm(self):
        """
        Get the distance in centimeters with floating point operations.
        It returns a float
        """
        pulse_time = self._send_pulse_and_wait()
        cms = (pulse_time / 2) / 29.1
        return cms


# Configuración del sensor de presión (galga) con HX711
# Nota: Necesitarás una biblioteca específica para HX711, aquí usamos una simulación con ADC
DOUT_PIN = 4  # GPIO para datos del HX711
SCK_PIN = 5   # GPIO para reloj del HX711
pressure_adc = ADC(Pin(36))  # Si conectas directamente a un ADC del ESP32
pressure_adc.atten(ADC.ATTN_11DB)  # Configurar para rango completo (0-3.3V)

# Configuración del sensor de proximidad LT1062
sensor_fc51 = Pin(27, Pin.IN)  # Conectar la salida digital del FC-51 al pin 39

# Factores de conversión
# Estos valores deben ser calibrados según tu configuración específica
PRESSURE_FACTOR = 0.0150    # Factor para convertir lectura ADC a kg
PROXIMITY_FACTOR = 0.01     # Factor para convertir lectura ADC a cm

Monedas100= 0
Monedas200= 0
Monedas500= 0
Monedas1000= 0

def fecha_actual():
    # Obtiene la fecha/hora local
    ahora = utime.localtime()
    # Formatea a 'yy-mm-dd hh-mm-ss'
    # tm[0] -> año completo (p. ej. 2025), por eso usamos (tm[0] % 100) para los dos últimos dígitos
    return "{:02d}-{:02d}-{:02d} {:02d}-{:02d}-{:02d}".format(
        ahora[0] % 100, ahora[1], ahora[2], ahora[3], ahora[4], ahora[5]
    )

def enviar_datos(url_firestore,data):
    """
    Genera datos artificiales y los envía a la base de datos de Firestore.
    Este proceso ETL simula la extracción (generación de datos), 
    la transformación (formateo según Firestore) y la carga (envío vía API REST).
    """
    
    # Preparar el JSON de acuerdo a la estructura que Firestore espera
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
            "ErrorClasificacion": {"stringValue": data["ErrorClasificacion"]},
            "Fecha": {"stringValue": data["Fecha"]}
        }
    }
    
    # Realiza la solicitud POST a Firestore
    response = urequests.post(url_firestore, json=json_data)
    print("Datos enviados a Firestore:")
    print(data)
    print("Código de respuesta:", response.status_code)
    print("Respuesta del servidor:", response.text)
    response.close()

def read_pressure():
    """Lee el valor de presión de la galga"""
    # En una implementación real con HX711, usarías la biblioteca correspondiente
    raw_value = pressure_adc.read()
    # Convertir a kg (requiere calibración)
    pressure = raw_value * PRESSURE_FACTOR
    return pressure

def read_CantidadMonedas(mms):
    grosor = 2
    Cantidad =mms/grosor
    return Cantidad

def read_ClasificacionMonedas(tiempo):
    velocidad = 0.1
    diametro = (tiempo * velocidad) / 1000
    global Monedas100, Monedas200, Monedas500, Monedas1000 
    if diametro == 20.3:
        Moneda100 += 1
        return 100
    elif diametro == 22.4:
        Moneda200 += 1
        return 200
    elif diametro == 23.7:
        Moneda500 += 1
        return 500
    elif diametro == 26.7:
        Moneda1000 += 1
        return 1000
    else:
        return 0

def main():
    
    # Configuración de la conexión WiFi
    ssid = "POCO F5"          
    password = "mypoderes"  
    
    print("Intentado Conexion")
    # Inicializa el módulo WiFi en modo estación (STA)
    wlan = network.WLAN(network.STA_IF)
    wlan.active(True)
    wlan.connect(ssid, password)

    # Espera hasta que se conecte a la red
    while not wlan.isconnected():
        time.sleep(1)
    print("Conexión establecida:", wlan.ifconfig())
    
    # URL del servidor donde se enviarán los datos (API REST)
    url = "https://firestore.googleapis.com/v1/projects/conteomonedas-2a1e5/databases/(default)/documents/ConteoMonedas"
    
    sensor = HCSR04(trigger_pin=12, echo_pin=14, echo_timeout_us=10000)
    
    # Configuración de pines según tu hardware
    hx = HX711(dout=4, pd_sck=5)

    # Realizar tara para compensar el offset
    print("Realizando tara, asegúrate de que la celda esté sin carga...")
    hx.tare()  
    print("Tara completada.")

    # Ajusta la escala con el factor obtenido tras calibrar con un peso conocido.
    # Supongamos que determinaste que 200 unidades corresponden a 1 kg.
    hx.set_scale(200)
    
    while True:
        try:
            input("Presione Enter para medir")
            #pressure = read_pressure()
            proximity = sensor_fc51.value()
            
            print("Esperando Moneda")
            
            while sensor_fc51.value():
                pass
            
            inicio = utime.ticks_ms()
            print("Moneda encontrada Moneda")
            
            while not sensor_fc51.value():
                pass
            
            
            fin = utime.ticks_ms()
            
            tiempo = utime.ticks_diff(fin, inicio)
            
            read_ClasificacionMonedas(tiempo)
            
            Moneda = read_ClasificacionMonedas(tiempo)
                
            # Mostrar resultados
            #print("Presión (kg): {:.2f}".format(pressure))
            print("Usted ingreso una moneda de: {:.2f}".format(Moneda))
            
            print("-" * 30)
            
            utime.sleep(1)
            
            Cantidad = read_CantidadMonedas(sensor.distance_mm())
            
            peso = hx.get_units(times=3)
            
            print("Usted ingreso una moneda de: {:.2f}".format(Cantidad))
            
            print("la caja pesa en gramos: {:.2f}".format(peso))
            
            print("-" * 30)
            
        except Exception as e:
            print("Error:", e)
            time.sleep(1)

# Iniciar el programa
if __name__ == "__main__":
    print("Iniciando lectura de sensores...")
    main()
    
