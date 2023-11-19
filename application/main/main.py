from time import sleep
from application.configs.broker_configs import mqtt_broker_configs
from .mqtt_connection.mqtt_client_connection import MqttClientConnection

def start():
    mqtt_client_connection = MqttClientConnection(
        mqtt_broker_configs["host"],
        mqtt_broker_configs["port"],
        mqtt_broker_configs["client_name"],
        mqtt_broker_configs["keep_alive"],
    )
    mqtt_client_connection.start_connection()

    while True:
        sleep(0.001) # Feito somente para a sessão não cair
        pass