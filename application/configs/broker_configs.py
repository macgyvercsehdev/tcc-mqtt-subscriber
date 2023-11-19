from decouple import config

mqtt_broker_configs = {
    "host": config("MQTT_HOST"),
    "port": config("MQTT_PORT", 1883, cast=int),
    "client_name": config("MQTT_CLIENT_NAME"),
    "keep_alive": config("MQTT_KEEP_ALIVE", 60),
    "topic": config("MQTT_TOPIC"),
}