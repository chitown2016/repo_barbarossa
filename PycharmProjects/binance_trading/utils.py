
from binance.client import Client as Clnt

api_key = "gIfjG8ZVlXRMelif2D3aqPTYiYn9z10qPCFWj9715QXsu3Yd5IZNA0dd5ZFaxrZO"
api_secret = "ayKp3nz3tNXEcH5o0CUdB0S1WIsUM1s5D01hYVX6EssybC8H8OzzsPbBOxX3unR4"

def get_binance_client(**kwargs):

    return Clnt(api_key, api_secret)