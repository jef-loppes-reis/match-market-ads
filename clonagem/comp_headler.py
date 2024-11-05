"""---"""
from time import sleep
from typing import List, Dict, Union, Tuple

from httpx import Client, Response
from rich import print as rprint


class CompatibilidadeHandler:
    def __init__(self, base_url: str, headers: dict):
        self._base_url: str = base_url
        self._headers: dict = headers

    def _retry_request(self,
                       client: Client,
                       url: str,
                       retries: int = 5,
                       delay: int = 3) -> Response:
        """
        Função auxiliar para realizar a lógica de retry em caso de erro 429 ou 500.

        _extended_summary_

        Args:
            client (_type_): Objeto HTTP para GET, POST e etc.
            url (str): Endpoint de requisicoes.
            retries (int, optional): Numero de tentativas. Defaults to 5.
            delay (int, optional): Tempo de espera. Defaults to 3.

        Raises:
            ValueError: Retorna um erro de limite de riquisicoes.

        Returns:
            Response: Response do objeto.
        """
        for _ in range(retries):
            response: Response = client.get(url, headers=self._headers)
            if response.status_code not in [429, 500]:
                return response
            sleep(delay)
        raise ValueError(f"Max retries exceeded for URL: {url}")

    def get_comp(self, lista_comp: dict) -> dict:
        """
        get_comp Função fictícia que retorna as compatibilidades processadas

        _extended_summary_

        Args:
            lista_comp (dict): Dicionario de compatibilidades de um anuncio.

        Returns:
            dict: Dicionario padrao para cadastro de compatibilidades.
        """
        return {'id': lista_comp.get('catalog_product_id')}

    def post_compatibilidades(self,
                              payload: dict,
                              http_client: Client, 
                              item_id_novo: str) -> Tuple[int, Response]:
        """
        post_compatibilidades Função fictícia que posta compatibilidades

        _extended_summary_

        Args:
            payload (dict): Corpo do dicionario de cadastro.
            http_client (_type_): Objeto HTTP para requisicao.
            item_id_novo (str): Codigo do anuncio Mercado Livre.

        Returns:
            Tuple[int, Response]: Status Code, Objeto de requisicao.
        """
        response: Response = http_client.post(
            f'/{item_id_novo}/compatibilities', json=payload, headers=self._headers)
        return response.status_code, response

    def compatibilidades(self,
                         item_id_ml_clone: str,
                         item_id_novo: str) -> Union[int, Dict]:
        """
        compatibilidades Obtém compatibilidades de um item e posta essas compatibilidades para um novo item.

        _extended_summary_

        Args:
            item_id_ml_clone (str): Codigo do anuncio de clonagem [Anuncio a ser clonado].
            item_id_novo (str): Codigo do anucnio, da copia do antigo anuncio.

        Returns:
            Union[int, Dict]: _description_
        """
        url = f'/{item_id_ml_clone}/compatibilities'
        _lista_aplicacoes: List[Dict] = []

        with Client(base_url=self._base_url) as client:
            # Realiza a requisição com retry
            _res_compatibilidades: Response = self._retry_request(client, url)

            # Verifica se há produtos na resposta
            products = _res_compatibilidades.json().get('products', [])
            if not products:
                return 0

            # Prepara a lista de compatibilidades
            for product in products:
                _lista_aplicacoes.append(self.get_comp(lista_comp=product))

            # Cria compatibilidades para o novo item
            created_compatibilities_count: Tuple[int, Response] = self.post_compatibilidades(
                payload={'products': _lista_aplicacoes},
                http_client=client,
                item_id_novo=item_id_novo
            )

            # Trata diferentes cenários de resposta
            status_code, response = created_compatibilities_count
            if status_code == 200:
                return response.json().get('created_compatibilities_count')
            if status_code == 400:
                return response, {'products': _lista_aplicacoes}
            return created_compatibilities_count
