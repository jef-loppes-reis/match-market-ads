"""---"""
from os import system
from json import dumps
from time import sleep
from re import findall
from random import randint

from ecomm import MLInterface
from httpx import Client, Response, ReadTimeout, HTTPStatusError
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
        self._ml_interface: MLInterface = ml_interface
        self._headers: dict[str, str] = ml_interface.headers()
        self._base_url: str = "https://api.mercadolibre.com"

    def pegar_infos_api(self, lista_mlbs: list[str]) -> list[dict]:
        """
        Método para requisitar informações da API, passando como parâmetro o código MLB do anúncio.

        Args:
            lista_mlbs (list[str]): Lista de MLBs.

        Returns:
            list[dict]: Lista de respostas em JSON.
        """
        lista_res: list = []
        with Client() as client:
            for mlb in tqdm(iterable=lista_mlbs,
                            desc='Pegando informações na API:',
                            colour='blue'):
                for attempt in range(1, 11):  # Tentativas de 1 a 10
                    try:
                        _res: Response = client.get(
                            url=f'{self._base_url}/items/{mlb}/?include_attributes=all',
                            headers=self._headers,
                            timeout=None  # Defina um timeout para evitar travamentos
                        )
                        if _res.is_success:
                            lista_res.append(_res.json())
                            break  # Sai do loop de tentativas em caso de sucesso
                        elif _res.is_server_error:
                            delay = randint(5, 15)
                            rprint(f'[yellow]Aviso: Erro de servidor [{_res.status_code}]. Tentativa {attempt}/10, tentando novamente em {delay}s...[/yellow]')
                            sleep(delay)
                        elif _res.is_client_error:
                            rprint(f'[red]Erro cliente [{_res.status_code}].[/red]')
                            match _res.status_code:
                                case 429:  # Too Many Requests
                                    delay = randint(3, 10)
                                    rprint(f'[yellow]Aviso: Limite de requisições atingido. Tentativa {attempt}/10, tentando novamente em {delay}s...[/yellow]')
                                    sleep(delay)
                                case 401:
                                    delay = randint(3, 10)
                                    rprint(f'[yellow]Aviso: Token vencido. Tentativa {attempt}/10, tentando novamente em {delay}s...[/yellow]')
                                    sleep(delay)
                                    self._ml_interface.refresh_token()
                                    self._headers: dict[str, str] = self._ml_interface.headers()
                                case _:
                                    rprint(f'[red]Erro: Corpo JSON inválido ou outro erro [{_res.status_code}].[/red]')
                                    rprint(f'[yellow]URL: {_res.url}[/yellow]')
                                    rprint(_res.json())
                                    raise ValueError(f"Erro cliente não recuperável: {_res.status_code}")
                        else:
                            rprint(f'[red]Erro desconhecido: [{_res.status_code}][/red]')
                    except ReadTimeout as error:
                        delay = randint(10, 30)
                        rprint(f'[yellow]Aviso: Tempo limite de conexão, {error}. Tentativa {attempt}/10, tentando novamente em {delay}s...[/yellow]')
                        sleep(delay)
                    except ConnectionError as error:
                        delay = randint(10, 30)
                        rprint(f'[yellow]Aviso: Erro de conexão, {error}. Tentativa {attempt}/10, tentando novamente em {delay}s...[/yellow]')
                        sleep(delay)
                    except Exception as e:
                        rprint(f'[red]Erro inesperado: {e}[/red]')
                        break
                else:  # Executado se todas as tentativas falharem
                    rprint(f'[red]Erro: Número de tentativas excedido para MLB {mlb}.[/red]')
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
        """
        Método para verificar compatibilidades de uma lista de MLBs via API.

        Args:
            lista_mlbs (list[str]): Lista de códigos MLB.

        Returns:
            list[bool]: Lista indicando se cada MLB possui compatibilidades.
        """
        lista_res: list[bool] = []
        with Client() as client:
            for mlb in tqdm(
                iterable=lista_mlbs,
                desc='Checando compatibilidades:',
                colour='yellow',
                total=len(lista_mlbs)
            ):
                for attempt in range(1, 11):  # Tentativas de 1 a 10
                    try:
                        _res_comp: Response = client.get(
                            url=f'https://api.mercadolibre.com/items/{mlb}/compatibilities',
                            headers=self._headers,
                            timeout=10  # Timeout explícito para evitar travamentos
                        )
                        
                        if _res_comp.is_success:
                            lista_res.append(
                                bool(_res_comp.json().get('products', False))  # Garante que 'products' existe
                            )
                            break  # Sai do loop de tentativas após sucesso
                        elif _res_comp.is_client_error:
                            if _res_comp.status_code == 429:  # Too Many Requests
                                delay = randint(3, 10)
                                rprint(f'[yellow]Aviso: Limite de requisições atingido. Tentativa {attempt}/10, tentando novamente em {delay}s...[/yellow]')
                                sleep(delay)
                            else:
                                rprint(f'[red]Erro cliente [{_res_comp.status_code}]. URL: {_res_comp.url}[/red]')
                                rprint(_res_comp.json())
                                raise ValueError(f"Erro cliente não recuperável: {_res_comp.status_code}")
                        elif _res_comp.is_server_error:
                            delay = randint(5, 15)
                            rprint(f'[yellow]Aviso: Erro de servidor [{_res_comp.status_code}]. Tentativa {attempt}/10, tentando novamente em {delay}s...[/yellow]')
                            sleep(delay)
                    except ReadTimeout as error:
                        delay = randint(10, 30)
                        rprint(f'[yellow]Aviso: Tempo limite de conexão, {error}. Tentativa {attempt}/10, tentando novamente em {delay}s...[/yellow]')
                        sleep(delay)
                    except ConnectionError as error:
                        delay = randint(10, 30)
                        rprint(f'[yellow]Aviso: Erro de conexão, {error}. Tentativa {attempt}/10, tentando novamente em {delay}s...[/yellow]')
                        sleep(delay)
                    except Exception as e:
                        rprint(f'[red]Erro inesperado: {e}[/red]')
                        break
                else:  # Executado se todas as tentativas falharem
                    rprint(f'[red]Erro: Número de tentativas excedido para MLB {mlb}.[/red]')
                    lista_res.append(False)  # Adiciona False em caso de falha completa
            return lista_res
