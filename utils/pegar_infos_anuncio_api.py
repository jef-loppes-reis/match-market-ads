"""---"""
from os import system
from json import dumps
from time import sleep
from re import findall
from random import randrange

from ecomm import MLInterface
from httpx import Client, ReadTimeout, ConnectTimeout, Response
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

    def __init__(self, ml_interface: MLInterface) -> None:
        self._headers: dict[str, str] = ml_interface.headers()
        self._base_url: str = "https://api.mercadolibre.com"

    def pegar_infos_api(self, lista_mlbs: list[str]) -> list[dict]:
        """Metodo para requistar informacoes da API,
        passando como parametro o codigo MLB do anuncio.

        Args:
            lista_mlbs (list[str]): Lista de MLBs.

        Returns:
            list[dict]: Lista de reponse em JSON.
        """
        lista_res: list = []
        with Client(base_url=self._base_url) as client:
            for mlb in tqdm(iterable=lista_mlbs,
                            desc='Pegando informacoes na API: ',
                            colour='blue'):
                for __attempts in range(10):
                    __time_range: int = 0
                    try:
                        _res: Response = client.get(
                            url=f'/items/{mlb}/?include_attributes=all',
                            headers=self._headers
                        )
                        _status_code: int =  _res.status_code
                        if _res.is_server_error:
                            __time_range: int = randrange(5, 15)
                            rprint(f'[yellow]Aviso: Erro de servidor [{_status_code}]. Tentando novamente em {__time_range}s...[/yellow]')
                            sleep(__time_range)
                            continue
                        if _res.is_client_error:
                            if _status_code == 429:
                                __time_range: int = randrange(3, 10)
                                rprint(f'[yellow]Aviso: Maximo de requisicoes [{_status_code}]. Tentando novamente em {__time_range}s...[/yellow]')
                                sleep(__time_range)
                                continue
                            rprint(f'[red]Erro: Corpo JSON [{_status_code}].[/red]')
                            rprint(f'[yellow]URL: /items/{mlb}/?include_attributes=all')
                            rprint(_res.json())
                            raise ValueError()
                        lista_res.append(dumps(_res.json()))
                        break
                    except ReadTimeout as error:
                        __time_range: int = randrange(10, 30)
                        rprint(f'[yellow]Aviso: Tempo limite de conexao, {error}. Tentando novamente em {__time_range}s...[/yellow]')
                        continue
                    except ConnectionError as error:
                        __time_range: int = randrange(10, 30)
                        rprint(f'[yellow]Aviso: Erro de conexao, {error}. Tentando novamente em {__time_range}s...[/yellow]')
                        continue
                rprint(f'[red]Erro: Numero de tentativas exedido, total {__attempts}.[/red]')
                raise ValueError()
        return lista_res

    def pegar_mlb_url(self, lista_url: list[str]) -> str:
        """Metodo para pegar o codigo MLB de uma URL. Faz um split com referencia do texto MLB.

        Args:
            lista_url (list[str]): Lista da URLs.

        Returns:
            list[str]: Lista de MLBs.
        """
        lista_url: list[str] = Series(lista_url).to_list()
        for url in lista_url:
            cod_mlb: str = findall(pattern=r'MLB-\d+|MLB\d+', string=url)
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

    def check_compatibilities(self, lista_mlbs: list[str]) -> list[bool]:
        lista_res: list[bool] = []
        with Client() as client:
            for mlb in tqdm(iterable=lista_mlbs,
                            desc='Checando compatibilidades.:',
                            colour='yellow',
                            total=len(lista_mlbs)):
                for __attempts in range(10):
                    try:
                        _res_comp: Response = client.get(
                            url=f'https://api.mercadolibre.com/items/{mlb}/compatibilities',
                            headers=self._headers
                        )
                        _status_code: int = _res_comp.status_code
                        __time_range: int = 0
                        if _res_comp.is_client_error:
                            if _status_code == 429:
                                __time_range: int = randrange(3, 10)
                                rprint(f'[yellow]Aviso: Maximo de requisicoes [{_status_code}]. Tentando novamente em {__time_range}s...[/yellow]')
                                sleep(__time_range)
                                continue
                            rprint(f'[red]Erro: Corpo JSON [{_status_code}].[/red]')
                            rprint(f'[yellow]URL: /items/{mlb}/compatibilities')
                            rprint(_res_comp.json())
                            raise ValueError()
                        if _res_comp.is_success:
                            lista_res.append(
                                bool(_res_comp.json().get('products'))
                            )
                            break
                    except ReadTimeout as error:
                        __time_range: int = randrange(10, 30)
                        rprint(f'[yellow]Aviso: Tempo limite de conexao, {error}. Tentando novamente em {__time_range}s...[/yellow]')
                        continue
                    except ConnectionError as error:
                        __time_range: int = randrange(10, 30)
                        rprint(f'[yellow]Aviso: Erro de conexao, {error}. Tentando novamente em {__time_range}s...[/yellow]')
                        continue
                rprint(f'[red]Erro: Numero de tentativas exedido, total {__attempts}.[/red]')
                raise ValueError()
        return lista_res
