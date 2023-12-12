from application.repository.tarifa_repository import calcular_agua_esgoto
from application.configs.broker_configs import mqtt_broker_configs
from application.repository.pipeline_repository import (
    pipeline_repository,
    pipeline_mensal_repository
)
from application.configs.mongo import conectar_mongo
from datetime import datetime, timedelta
from typing import List

def enviar_para_banco(dic):
    """
    Função para enviar dados para o banco de dados.

    Args:
        dic (dict): O dicionário contendo os dados a serem inseridos.

    Returns:
        None

    Raises:
        Exception: Se ocorrer um erro durante a inserção dos dados.

    """
    db = conectar_mongo()  # Conectar ao banco de dados
    try:
        db.mqtt.insert_one(dic)  # Inserir dados na coleção
    except Exception as e:
        print(e)  # Imprimir a mensagem de erro

def consumo_30_dias(dia_mes_atual: str) -> List[dict]:
    """
    Recupera dados de consumo diário do MongoDB com base na data fornecida.

    Args:
        dia_mes_atual (str): A data atual no formato "%Y-%m-%d %H:%M:%S".

    Returns:
        List[dict]: Uma lista de dados de consumo recuperados do MongoDB.
    """
    
    # Conectar ao MongoDB
    db = conectar_mongo()

    # Converter a string da data atual para objeto datetime
    dia_mes_atual = datetime.strptime(dia_mes_atual, "%Y-%m-%d %H:%M:%S")

    # Calcular o dia anterior
    dia_mes_anterior = dia_mes_atual - timedelta(days=30)

    # Obter apenas a parte da data do dia anterior
    dia_mes_anterior = datetime(dia_mes_anterior.year, dia_mes_anterior.month, dia_mes_anterior.day)

    # Criar o pipeline para a agregação do MongoDB
    pipeline = pipeline_mensal_repository(dia_mes_anterior)

    # Executar consulta de agregação no MongoDB
    response = list(db.mqtt.aggregate(pipeline))

    return response


def consumo_diario(dia_atual: str) -> List[dict]:
    """
    Recupera dados de consumo mensal do MongoDB com base na data fornecida.

    Args:
        dia_atual (str): A data atual no formato "%Y-%m-%d %H:%M:%S".

    Returns:
        List[dict]: Uma lista de dados de consumo recuperados do MongoDB.
    """
    
    # Conectar ao MongoDB
    db = conectar_mongo()
    
    # Converter a string da data atual para objeto datetime
    dia_atual = datetime.strptime(dia_atual, "%Y-%m-%d %H:%M:%S")
    
    # Calcular o dia anterior
    dia_anterior = dia_atual - timedelta(days=1)
    
    # Obter apenas a parte da data da data atual
    dia_atual = datetime(dia_atual.year, dia_atual.month, dia_atual.day)
    
    # Obter apenas a parte da data do dia anterior
    dia_anterior = datetime(dia_anterior.year, dia_anterior.month, dia_anterior.day)
    
    # Criar o pipeline para a agregação do MongoDB
    pipeline = pipeline_repository(dia_anterior, dia_atual)
    
    # Executar consulta de agregação no MongoDB
    response = list(db.mqtt.aggregate(pipeline))
    
    return response

def on_connect(client, userdata, flags, rc):
    """
    Função de retorno chamada quando o cliente se conecta com sucesso ao corretor MQTT.

    Args:
        client (mqtt.Client): A instância do cliente MQTT.
        userdata: Os dados definidos pelo usuário passados para o cliente.
        flags: Sinais de resposta enviados pelo corretor MQTT.
        rc (int): O código de resultado da conexão.

    Returns:
        None

    """
    if rc == 0:
        # Conexão bem-sucedida
        print(f'Conectado com sucesso: {client}')
        client.subscribe(mqtt_broker_configs["topic"])
    else:
        # Conexão ruim
        print("Conexão ruim. Código retornado=", rc)

def on_subscribe(client, userdata, mid, granted_qos):
    """
    Função de retorno chamada quando o cliente se inscreve em um tópico.

    Args:
        client (mqtt.Client): A instância do cliente MQTT.
        userdata: Os dados privados do usuário definidos no cliente MQTT.
        mid: O ID da mensagem de inscrição.
        granted_qos: A lista de níveis de QoS concedidos para as assinaturas solicitadas.
    """
    print("Inscrito no tópico: {}".format(mqtt_broker_configs["topic"]))

def on_message(client, userdata, msg):
    """
    Lidar com mensagens recebidas do corretor MQTT.

    Args:
        client: A instância do cliente MQTT.
        userdata: Os dados do usuário fornecidos ao iniciar o cliente MQTT.
        msg: A mensagem recebida do corretor MQTT.

    Returns:
        None
    """
    # Imprimir o tópico e o conteúdo da mensagem
    print(msg.topic + " " + str(msg.payload))

    if not msg.payload:
        print("Payload vazio")
        return 

    # Dividir o conteúdo em uma lista de valores
    payload = msg.payload.decode().split(";")

    # Converter a string de data para um objeto datetime
    data = None
    try:
        data = datetime.strptime(payload[3], "%d/%m/%Y %H:%M:%S")
    except:
        data = None

    # Criar um dicionário com os dados da mensagem
    dic = {
        "id_equipamento": payload[0],
        "temperatura": float(payload[1]),
        "umidade": float(payload[2]),
        "data": data,
        "dia_da_semana": payload[4],
        "vazao_litro_acumulada": float(payload[5]),
    }

    # Se a data não for válida, imprimir o dicionário e enviá-lo para o banco de dados
    if not data:
        print(dic)
        enviar_para_banco(dic)
        return

    # Obter o consumo diário para a data fornecida
    res = consumo_diario(dic["data"].strftime("%Y-%m-%d %H:%M:%S"))

    # Calcular a diferença entre o fluxo acumulado atual e anterior
    consumo = 0
    if not res:
        consumo = 0
    else:
        consumo = res[0]["vazao_litro_acumulada"]

    dic["consumo_diario"] = abs(dic["vazao_litro_acumulada"] - consumo)

    # Obter o consumo mensal para a data fornecida
    res_mensal = consumo_30_dias(dic["data"].strftime("%Y-%m-%d %H:%M:%S"))

    # Calcular a diferença entre o fluxo acumulado atual e anterior
    consumo = 0
    tarifa = 0
    if not res_mensal:
        consumo = 0
    else:
        consumo = res_mensal[0]["vazao_litro_acumulada"]

    tarifa, consumo_mensal = calcular_agua_esgoto(consumo, dic["vazao_litro_acumulada"])
    
    dic["consumo_mensal"] = consumo_mensal
    dic["tarifa"] = tarifa

    # Imprimir o dicionário e enviá-lo para o banco de dados
    print(dic)
    enviar_para_banco(dic)
