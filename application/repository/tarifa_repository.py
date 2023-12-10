def calcular_agua_esgoto(consumo_mensal, vazao_litro_acumulada):
    """
    Calcula o valor da conta de água e esgoto com base no consumo mensal e na vazão acumulada.

    Args:
        consumo_mensal (float): Consumo mensal em litros.
        vazao_litro_acumulada (float): Vazão acumulada em litros.

    Returns:
        float: O valor a ser pago da conta de água e esgoto.
    """
    tarifaMinima = 35.85
    tarifaSegundaFaixa = 5.62
    tarifaTerceiraFaixa = 14
    tarifaQuartaFaixa = 15.43
    valorPagar = 0
    valorFaixa = 0

    if consumo_mensal > vazao_litro_acumulada:
        consumo_mensal = vazao_litro_acumulada / 1000
    else:
        consumo_mensal = (vazao_litro_acumulada - consumo_mensal) / 1000

    if consumo_mensal <= 10:
        valorPagar = tarifaMinima
    elif consumo_mensal > 10 and consumo_mensal <= 20:
        valorFaixa = consumo_mensal - 10
        valorPagar = tarifaMinima + valorFaixa*tarifaSegundaFaixa
    elif consumo_mensal > 20 and consumo_mensal <= 50:
        valorFaixa = consumo_mensal - 20
        valorPagar = tarifaMinima + 10*tarifaSegundaFaixa + valorFaixa*tarifaTerceiraFaixa
    elif consumo_mensal > 50:
        valorFaixa = consumo_mensal - 50
        valorPagar = tarifaMinima + 10*tarifaSegundaFaixa + 30*tarifaTerceiraFaixa + valorFaixa*tarifaQuartaFaixa

    valor = valorPagar * 2

    return valor
