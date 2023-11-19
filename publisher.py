from application.configs.broker_configs import mqtt_broker_configs
import paho.mqtt.client as mqtt

mqtt_client = mqtt.Client('publisher-py')

mqtt_client.connect(mqtt_broker_configs["host"], 1883)

mqtt_client.publish(mqtt_broker_configs["topic"], '20.5;2022-01-01 00:00:00;segunda;12.0;1.0')

