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