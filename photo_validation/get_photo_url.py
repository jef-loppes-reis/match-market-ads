"""---"""
from os import path, mkdir

from httpx import Client, Response
from rich import print as rprint
from ecomm import MLInterface
from tqdm import tqdm

class GetImgFile:

    _url_get_infos_mlb: str = 'https://api.mercadolibre.com/items/%s?include_attributes=all'
    _lista_mlb: list[str] = []
    _lista_photo_url_mlb: list[dict] = []

    def __init__(self, nome_loja: str) -> None:
        self._headers_ml: dict = MLInterface(1).headers()
        self._nome_loja: str = nome_loja

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

        if not path.exists(path.join(_path_out_photos, self._nome_loja)):
            mkdir(path.join(_path_out_photos, self._nome_loja))

        with Client() as client:
            for mlb_items in tqdm(self._lista_photo_url_mlb,
                                  desc='Baixando fotos: ',
                                  total=len(self._lista_photo_url_mlb),
                                  colour='green'):
                cont: int = 0 # Interacao com o id da foto refente ao MLB.
                photos: list = mlb_items.get('photos')
                if photos is None:
                    continue
                for photo_url in photos:
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

    rprint('Digite o nome da loja: ')
    NOME_LOJA = input().lower()
    if not path.exists(f'../data/planilhas_primeiro_processo/{NOME_LOJA}.xlsx'):
        rprint(f'[bright_yellow]Ops, nao existe uma planilha de primeira conferencia com esse nome [blue]{NOME_LOJA}.xlsx[/blue][/bright_yellow]')
        # raise ValueError('Planilha nao encontrada!')
    download_img = GetImgFile(NOME_LOJA)

    lista_mlb: list[str] = (
        read_excel(
            path.join(
                path.dirname(__file__),
                # '../data/planilhas_primeiro_processo/'
                f'../total_produtos_{NOME_LOJA}.xlsx'
            ), dtype=str
        )
        .fillna(0)
        .query('clona == "1"')['mlb']
        .to_list()
    )
    rprint(f'\nTotal de MLB: {len(lista_mlb)}')
    download_img.pegar_lista_url_api(lista_mlbs=lista_mlb)
    download_img.get_img_url()
