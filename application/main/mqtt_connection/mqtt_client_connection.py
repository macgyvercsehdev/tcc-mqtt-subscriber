from application.main.mqtt_connection.callbacks import on_connect, on_message, on_subscribe
from application.configs.broker_configs import mqtt_broker_configs
import paho.mqtt.client as mqtt


class MqttClientConnection:
    def __init__(self, host, port, client_name, keep_alive=60): # Método construtor
        self.__host = host
        self.__port = port
        self.__client_name = client_name
        self.__keep_alive = keep_alive
        self.__mqtt_client = None

    def start_connection(self):
        mqtt_client = mqtt.Client(self.__client_name)
        
        #callbacks
        mqtt_client.on_connect = on_connect
        mqtt_client.on_subscribe = on_subscribe
        mqtt_client.on_message = on_message

        mqtt_client.connect(self.__host, self.__port, self.__keep_alive)
        self.__mqtt_client = mqtt_client
        self.__mqtt_client.loop_start()

    def end_connection(self):
        try:
            self.__mqtt_client.loop_stop()
            self.__mqtt_client.disconnect()
            return True
        except Exception as e:
            print(e)
            return False
