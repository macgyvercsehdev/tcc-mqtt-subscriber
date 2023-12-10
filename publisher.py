from application.configs.broker_configs import mqtt_broker_configs
import paho.mqtt.client as mqtt
from datetime import datetime, timedelta
import random

mqtt_client = mqtt.Client('publisher-py')


lista = []
dia_da_semana = ["segunda", "terça", "quarta", "quinta", "sexta", "sábado", "domingo"]

for dia in range(10):
    id_equipamento = 'simulador'
    temperatura = round(random.uniform(20, 35), 1)
    umidade = round(random.uniform(20, 35), 1)
    data = datetime.strptime("9/11/2023 22:50:00", "%d/%m/%Y %H:%M:%S") + timedelta(minutes=dia)
    vazao_litro_acumulada = round(random.uniform(20, 35), 1)
    data = data.strftime("%d/%m/%Y %H:%M:%S")
    dados = f'{id_equipamento};{temperatura};{umidade};{data};{dia_da_semana[random.randint(0, 6)]};{vazao_litro_acumulada}'
    lista.append(dados)

mqtt_client.connect(mqtt_broker_configs["host"], 1883)

for i in lista:
    mqtt_client.publish(mqtt_broker_configs["topic"], i)