"""---"""
from concurrent.futures import ProcessPoolExecutor
from time import sleep
from random import randint
from ecomm import MLInterface
from pandas import DataFrame
from httpx import Client, ReadTimeout, Response
from tqdm import tqdm
from rich import print as rprint


class DbPhotos:
    """
     _summary_

    Returns
    -------
    _type_
        _description_
    """
    __default_columns = [
        'item_id',
        'photo_id',
        'num_pos',
        'size',
        'max_size',
        'photo_url',
        'brand',
    ]

    def __init__(self, name_loja: str) -> None:
        self.__ml: MLInterface = MLInterface(1)
        self._name_loja: str = name_loja
        self._headers: dict = self.__ml.headers()

    def get_infos(self, item_id: str) -> dict:
        """Recupera informações das fotos da API para um ID de item específico."""
        with Client() as client:
            for tentativas in range(15):
                try:
                    response: Response = client.get(
                        url=f'https://api.mercadolibre.com/items/{item_id}/?include_attributes=all',
                        headers=self._headers
                    )
                    if response.is_success:
                        return self.parse_response(response)
                    if response.status_code in [429, 500]:
                        # Limite de taxa ou erro de servidor
                        sleep(randint(1, 5))
                except ReadTimeout:
                    rprint(f"Tempo limite atingido para item_id {item_id} | TANTATIVAS = {tentativas}/15.\nTentando novamente...")
                    sleep(randint(10, 20))
            return self.empty_response()  # Retorna dados vazios se todas as tentativas falharem

    def parse_response(self, response: Response) -> dict:
        """Analisa a resposta da API e extrai as informações relevantes das fotos."""
        data: dict = response.json()
        return {
            'item_id': data.get('id'),
            'list_photos_id': [photo.get('id') for photo in data.get('pictures', [])],
            'list_size_photos': [photo.get('size') for photo in data.get('pictures', [])],
            'list_max_size_photo': [photo.get('max_size') for photo in data.get('pictures', [])],
            'list_url': [photo.get('url') for photo in data.get('pictures', [])],
            'brand': self._name_loja
        }

    @staticmethod
    def empty_response() -> dict:
        """Retorna uma estrutura padrão de resposta vazia."""
        return {
            'item_id': None,
            'list_photos_id': [],
            'list_size_photos': [],
            'list_max_size_photo': [],
            'list_url': [],
            'brand': None
        }

    def main(self, item_ids: list[str]) -> None:
        df: DataFrame = DataFrame(columns=self.__default_columns)

        with ProcessPoolExecutor() as executor:
            results: list = list(tqdm(executor.map(self.get_infos, item_ids),
                                total=len(item_ids),
                                desc="Processando itens")
                            )

            for future in results:
                if not future.get('item_id'):
                    rprint("[yellow]Aviso: Falha ao recuperar dados para um item.[/yellow]")
                    continue

                for pos, (photo_id, size, max_size, url) in enumerate(
                    zip(
                        future['list_photos_id'],
                        future['list_size_photos'],
                        future['list_max_size_photo'],
                        future['list_url']
                    ), start=1
                ):
                    # ! Metodo do DataFrame "_append" esta obsoleto, tentar encontrar outra forma de utilizar.
                    df = df._append({
                        'item_id': future['item_id'],
                        'photo_id': photo_id,
                        'num_pos': pos,
                        'size': size,
                        'max_size': max_size,
                        'photo_url': url,
                        'brand': self._name_loja.upper()
                    }, ignore_index=True)

        rprint(df)


if __name__ == '__main__':
    from os import path
    from pandas import read_excel

    db_photos = DbPhotos('indisa')

    lista_mlb: list[str] = (
            read_excel(
                path.join(
                    path.dirname(__file__),
                    # '../data/planilhas_primeiro_processo/'
                    f'../total_produtos_indisa.xlsx'
                ), dtype=str
            )
            .fillna(0)
            .query('clona == "1"')['mlb']
            .to_list()
        )

    db_photos.main(item_ids=lista_mlb)
