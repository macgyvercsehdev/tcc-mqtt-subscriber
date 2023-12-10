from datetime import datetime


def pipeline_repository(dia_anterior, dia_atual):
    return [
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
    ]

def pipeline_mensal_repository(mes_anterior):
    
    final_mes_anterior = datetime(mes_anterior.year, mes_anterior.month, mes_anterior.day, 23, 59, 59)

    return [
        {
            '$match': {
                'data': {
                    '$gte': mes_anterior,
                    '$lt': final_mes_anterior
                }
            }
        }, {
            '$sort': {
                'data': -1
            }
        }, {
            '$group': {
                '_id': 1, 
                'vazao_litro_acumulada': {
                    '$max': '$vazao_litro_acumulada'
                }
            }
        }, {
            '$project': {
                '_id': 0, 
                'vazao_litro_acumulada': '$vazao_litro_acumulada'
            }
        }
    ]