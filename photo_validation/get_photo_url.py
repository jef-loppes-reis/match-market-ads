from os import path, mkdir

from httpx import Client, Response
from rich import print as pprint
from ecomm import MLInterface
from tqdm import tqdm

class GetImgFile:

    _url_get_infos_mlb: str = 'https://api.mercadolibre.com/items/%s?include_attributes=all'
    _lista_mlb: list[str] = []
    _lista_photo_url_mlb: list[dict] = []

    def __init__(self, nome_loja: str) -> None:
        self._headers_ml: dict = MLInterface(1)._headers()
        self._nome_loja: str = nome_loja

    def pegar_lista_url_api(self, lista_mlb: list[str]):
        with Client() as client:
            for _mlb in tqdm(lista_mlb, desc='Listando urls: ', total=len(lista_mlb), colour='blue'):
                res_api: dict = client.get(
                    url=self._url_get_infos_mlb % _mlb,
                    headers=self._headers_ml,
                    timeout=None
                ).json()
                self._lista_photo_url_mlb.append({'mlb': _mlb, 'photos': res_api.get('pictures')})

    def get_img_url(self):
        _path_out_photos = path.join(
            path.dirname(__file__), 'out_files_photos')

        if not path.exists(path.join(_path_out_photos, self._nome_loja)):
            mkdir(path.join(_path_out_photos, self._nome_loja))

        with Client() as client:
            for mlb_items in tqdm(self._lista_photo_url_mlb,
                                  desc='Baixando fotos: ',
                                  total=len(self._lista_photo_url_mlb),
                                  colour='green'):
                cont: int = 0 # Interacao com o id da foto refente ao MLB.
                for photo_url in mlb_items.get('photos'):
                    res_html: Response = client.get(
                        url=photo_url.get('url').replace('D_', 'D_NQ_NP_2X_'),
                        timeout=None,
                    )
                    _code_photo: str = f'{mlb_items.get('mlb')}_PHOTOID{photo_url.get('id')}'
                    _dir_photo = f'{
                        _path_out_photos
                    }/{self._nome_loja}/{_code_photo}_{cont}.jpg'
                    with open(_dir_photo, 'wb') as img:
                        img.write(res_html.content)
                    cont += 1


if __name__ == '__main__':
    from pandas import read_excel
    download_img = GetImgFile()
    lista_mlb = read_excel('../data/monroe.xlsx').fillna(0).query(
        'clona == 1')['mlb'].to_list()
    download_img.pegar_lista_url_api(
        lista_mlb=lista_mlb)
    download_img.get_img_url()
