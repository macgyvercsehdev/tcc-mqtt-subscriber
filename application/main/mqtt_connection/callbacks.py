from datetime import datetime
from application.configs.broker_configs import mqtt_broker_configs
from application.configs.mongo import conectar_mongo


def enviar_para_banco(dic):
    db = conectar_mongo()
    try:
        db.mqtt.insert_one(dic)
    except Exception as e:
        print(e)

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f'Conectado com sucesso: {client}')
        client.subscribe(mqtt_broker_configs["topic"])
    else:
        print("Bad connection Returned code=", rc)


def on_subscribe(client, userdata, mid, granted_qos):
    print("Inscrito no tópico: {}".format(mqtt_broker_configs["topic"]))


def on_message(client, userdata, msg):
    print(msg.topic + " " + str(msg.payload))

    payload = msg.payload.decode().split(";")
    
    dic = {
        "temperatura": float(payload[0]),
        "data": datetime.strptime(payload[1], "%Y-%m-%d %H:%M:%S"),
        "dia_da_semana": payload[2],
        "vazao_litro": float(payload[3]),
        "vazao_litro_acumulada": float(payload[4]),
    }
    
    print(dic)
    
    # TODO: Enviar o dicionário para o servidor de banco de dados
    enviar_para_banco(dic)