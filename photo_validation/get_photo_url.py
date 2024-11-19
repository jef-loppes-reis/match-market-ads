"""---"""
from os import path, makedirs

from httpx import Client, Response
from rich import print as rprint
from ecomm import MLInterface
from tqdm import tqdm


class GetImgFile:
    """
     _summary_
    """

    def __init__(self, nome_loja: str) -> None:
        self._nome_loja: str = nome_loja
        self._headers_ml: dict = MLInterface(1).headers()
        self._url_get_infos_mlb: str = 'https://api.mercadolibre.com/items/%s?include_attributes=all'
        self._lista_mlb: list[str] = []
        self._lista_photo_url_mlb: list[dict] = []

    def pegar_lista_url_api(self, lista_mlbs: list[str]):
        with Client() as client:
            for _mlb in tqdm(lista_mlbs, desc='Listando urls: ',
                             total=len(lista_mlbs), colour='blue'):
                res_api: dict = client.get(
                    url=self._url_get_infos_mlb % _mlb,
                    headers=self._headers_ml,
                    timeout=None
                ).json()
                self._lista_photo_url_mlb.append(
                    {
                        'mlb': _mlb,
                        'photos': res_api.get('pictures')
                    }
                )

    def get_img_url(self):
        _path_out_photos: str = path.join(
            path.dirname(__file__), 'out_files_photos')

        makedirs(_path_out_photos, exist_ok=True)
        makedirs(path.join(_path_out_photos, self._nome_loja), exist_ok=True)

        with Client() as client:
            for mlb_items in tqdm(self._lista_photo_url_mlb,
                                  desc='Baixando fotos: ',
                                  total=len(self._lista_photo_url_mlb),
                                  colour='green'):
                cont: int = 0  # Interacao com o id da foto refente ao MLB.
                photos: list = mlb_items.get('photos')
                if photos is None:
                    continue
                for photo_url in photos:
                    res_html: Response = client.get(
                        url=photo_url.get('url').replace('D_', 'D_NQ_NP_2X_'),
                        timeout=None,
                    )
                    _code_photo: str = f'{mlb_items.get('mlb')}_PHOTOID{
                        photo_url.get('id')}'
                    _dir_photo = f'{
                        _path_out_photos
                    }/{self._nome_loja}/{_code_photo}_{cont}.jpg'
                    with open(_dir_photo, 'wb') as img:
                        img.write(res_html.content)
                    cont += 1


if __name__ == '__main__':
    from pandas import read_excel, DataFrame

    rprint('[yellow]\nDigite o nome da loja: [/yellow]')

    NOME_LOJA = input().lower()
    NAME_FILE_BASE: str = 'total_produtos_'
    PATH_FILE: str = path.dirname(__file__)

    download_img = GetImgFile(NOME_LOJA)

    if not path.exists(path.join(PATH_FILE, f'../{NAME_FILE_BASE}{NOME_LOJA}.xlsx')):
        raise ValueError(f'O excel [{NAME_FILE_BASE}{NOME_LOJA}.xlsx] nao existe !')

    table: DataFrame = read_excel(
        path.join(PATH_FILE, f'../{NAME_FILE_BASE}{NOME_LOJA}.xlsx'),
        dtype=str
    )

    rprint(table)

    table = table.fillna('0').query('clona == "1"')

    lista_mlb: list[str] = table.mlb.to_list()

    rprint(f'\nTotal de MLB: {len(lista_mlb)}')
    download_img.pegar_lista_url_api(lista_mlbs=lista_mlb)
    download_img.get_img_url()
