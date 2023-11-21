from application.configs.broker_configs import mqtt_broker_configs
from application.configs.mongo import conectar_mongo
from datetime import datetime, timedelta


def enviar_para_banco(dic):
    db = conectar_mongo()
    try:
        db.mqtt.insert_one(dic)
    except Exception as e:
        print(e)

def consumo_diario(dia_atual):
    db = conectar_mongo()
    
    dia_anterior = datetime.strptime(dia_atual, "%Y-%m-%d %H:%M:%S") - timedelta(days=1)
    dia_atual = datetime.strptime(dia_atual, "%Y-%m-%d %H:%M:%S")

    dia_atual = datetime(dia_atual.year, dia_atual.month, dia_atual.day, dia_atual.hour, dia_atual.minute, dia_atual.second)
    dia_anterior = datetime(dia_anterior.year, dia_anterior.month, dia_anterior.day)

    response = list(db.mqtt.aggregate([
        {
            '$match': {
                'data': {
                    '$gte': dia_anterior, 
                    '$lt': dia_atual
                }
            }
        }, {
            '$sort': {
                'data': -1
            }
        }, {
            '$group': {
                '_id': {
                    'dia': {
                        '$dateToString': {
                            'format': '%d-%m-%Y', 
                            'date': '$data'
                        }
                    }
                }, 
                'vazao_litro_acumulada': {
                    '$last': '$vazao_litro_acumulada'
                }
            }
        }, {
            '$project': {
                '_id': 0, 
                'dia': '$_id.dia', 
                'vazao_litro_acumulada': 1
            }
        }
    ]))

    return response

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
        "id_equipamento": payload[0],
        "temperatura": float(payload[1]),
        "data": datetime.strptime(payload[2], "%Y-%m-%d %H:%M:%S"),
        "dia_da_semana": payload[3],
        "vazao_litro_acumulada": float(payload[4]),
    }

    res = consumo_diario(dic["data"].strftime("%Y-%m-%d %H:%M:%S"))
    consumo = 0

    if not res:
        consumo = 0
    else:
        consumo = res[0]["vazao_litro_acumulada"]

    dic["consumo_diario"] = abs(consumo - dic["vazao_litro_acumulada"])

    # TODO: Enviar o dicionário para o servidor de banco de dados
    enviar_para_banco(dic)
