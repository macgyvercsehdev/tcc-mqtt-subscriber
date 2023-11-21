from application.configs.broker_configs import mqtt_broker_configs
import paho.mqtt.client as mqtt
from datetime import datetime, timedelta
import random

mqtt_client = mqtt.Client('publisher-py')


lista = []
dia_da_semana = ["segunda", "terça", "quarta", "quinta", "sexta", "sábado", "domingo"]

for dia in range(10):
    id_equipamento = '00:00:00:00:00:00'
    temperatura = round(random.uniform(20, 35), 1)
    data = datetime.strptime("2023-11-18 00:00:00", "%Y-%m-%d %H:%M:%S") + timedelta(days=dia)
    vazao_litro_acumulada = random.uniform(0, 90)

    dados = f'{id_equipamento};{temperatura};{data};{dia_da_semana[random.randint(0, 6)]};{vazao_litro_acumulada}'
    lista.append(dados)

mqtt_client.connect(mqtt_broker_configs["host"], 1883)

for i in lista:
    mqtt_client.publish(mqtt_broker_configs["topic"], i)