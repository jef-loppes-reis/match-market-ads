"""---"""
from os import system
from json import dumps
from time import sleep
from re import findall

from ecomm import MLInterface
from httpx import Client, ReadTimeout, ConnectTimeout
from pandas import read_feather, Series
from tqdm import tqdm
from rich import print as rprint

from utils.replace_char_spec import ReplaceCaract


class PegarInfosAnuncioApi:
    """Obejeto para requisitar atributos da API MercadoLivre.

    Returns:
        _type_: _description_
    """

    _atributos: dict[str, None] = {
        'gtin': None,
        'mpn': None,
        'sku': None,
        'oem': None,
        'marca': None,
        'numero_original': None,
        'produto': None,
    }

    _base_url = "https://api.mercadolibre.com/items/%s/?include_attributes=all"

    def __init__(self, ml_interface: MLInterface) -> None:
        self._headers: dict[str, str] = ml_interface._headers()

    def pegar_infos_api(self, lista_mlbs: list[str]) -> list[dict]:
        """Metodo para requistar informacoes da API,
        passando como parametro o codigo MLB do anuncio.

        Args:
            lista_mlbs (list[str]): Lista de MLBs.

        Returns:
            list[dict]: Lista de reponse em JSON.
        """
        lista_res: list = []
        with Client() as client:
            for mlb in tqdm(iterable=lista_mlbs,
                            desc='Pegando informacoes na API: ',
                            colour='blue'):
                tentativas: int = 0
                while True:
                    if tentativas > 9:
                        rprint({mlb: f'Tentativas: {tentativas}'})
                        break
                    try:
                        _res = client.get(
                            url=self._base_url%mlb,
                            headers=self._headers
                        )
                        if _res.status_code in [429, 500]:
                            rprint(
                                {
                                    mlb: f'status_code: {_res.status_code} | tentativas: {tentativas}'
                                }
                            )
                            sleep(1)
                            tentativas += 1
                            continue
                        if _res.status_code in [400, 404]:
                            rprint(
                                self._base_url%mlb,
                                _res.json()
                            )
                        lista_res.append(dumps(_res.json()))
                        break
                    except ReadTimeout:
                        break
                    except ConnectionError:
                        sleep(10)
                        continue
                    except ConnectTimeout:
                        rprint({mlb: f'TimeOut | tentativas: {tentativas}'})
                        sleep(10)
                        continue
        return lista_res

    def pegar_mlb_url(self, lista_url: list[str]) -> str | None:
        """Metodo para pegar o codigo MLB de uma URL. Faz um split com referencia do texto MLB.

        Args:
            lista_url (list[str]): Lista da URLs.

        Returns:
            list[str]: Lista de MLBs.
        """
        lista_url: list[str] = Series(lista_url).to_list()
        for url in lista_url:
            cod_mlb: str = findall(pattern=r'MLB-\d+|MLB\d+', string=url)
            print(cod_mlb)
            if len(cod_mlb) > 0:
                return f'{cod_mlb[0].replace('-','')}'




    def replace_caracteres(self, text: str) -> str:
        """Metodo para pegar um texto e tirar todos os caracteres especiais como acentos.

        Args:
            text (str): Texto com os caracteries especias.

        Returns:
            str: Texto limpo.
        """
        _repalce_text: ReplaceCaract = ReplaceCaract(text)
        _repalce_text.remover_acentos()
        _repalce_text.remover_caracteres_especiais()
        return _repalce_text.get_texto_limpo()

    def pegar_atributos_necessarios(self, _list_atributos: dict) -> dict:
        """Metodo para pegar as chaves necessarias do JSON de requisicoes do MLB.
        Ou seja, ele so pega o que a gente precisa.

        Args:
            _list_atributos (list[dict]): Lista de dicionarios, com as requisicoes da API.

        Returns:
            _type_: Atributos buscados.
        """
        _key: dict = {}
        self._atributos.update({
            'produto': _list_atributos.get('title')
        })
        if _list_atributos.get('attributes') is None:
            for key, _ in self._atributos.items():
                self._atributos.update({key: None})
            return self._atributos.update()
        for _key in _list_atributos.get('attributes'):
            match _key.get('id'):
                case 'MPN':
                    self._atributos.update(
                        {
                            'mpn': self.replace_caracteres(
                                _key['value_name']).upper()
                        }
                    )
                case 'OEM':
                    self._atributos.update(
                        {
                            'oem': self.replace_caracteres(
                                _key['value_name']).upper()
                        }
                    )
                case 'GTIN':
                    self._atributos.update(
                        {
                            'gtin': self.replace_caracteres(
                                _key['value_name']).upper()
                        }
                    )
                case 'PART_NUMBER':
                    self._atributos.update(
                        {
                            'numero_original': self.replace_caracteres(
                                _key['value_name']).upper()
                        }
                    )
                case 'SELLER_SKU':
                    self._atributos.update(
                        {
                            'sku': self.replace_caracteres(
                                _key['value_name']).upper()
                        }
                    )
                case 'BRAND':
                    self._atributos.update(
                        {
                            'marca': self.replace_caracteres(
                                _key['value_name']).upper()
                        }
                    )
                case _:
                    ...
        return self._atributos
