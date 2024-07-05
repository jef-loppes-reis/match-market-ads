"""

Returns:
    _type_: _description_
"""
from os import path, mkdir, system
from concurrent.futures import ProcessPoolExecutor
from json import loads, dumps

from pandas import DataFrame
import bs4
from httpx import Client
from tqdm import tqdm
from rich import print as pprint
from ecomm import MLInterface

from modules.anuncios_loja_oficial import AnunciosLojaOficial
from modules.informacoes_loja_oficial import InfosLojaOficial
from modules.pegar_infos_anuncio_api import PegarInfosAnuncioApi
from modules.siac_fuzzy import SiacFuzzy


class Main:

    _columns_default = {
        "lista_infos_mlb": "str",
        "lista_att_necessarios": "str",
        "mlb": "str",
        "gtin_ml": "str",
        "gtin_siac": "str",
        "gtin_fuzzy": "float",
        "mpn_ml": "str",
        "mpn_siac": "str",
        "mpn_fuzzy": "float",
        "sku_ml": "str",
        "sku_siac": "str",
        "sku_fuzzy": "float",
        "numero_original_ml": "str",
        "numero_original_siac": "str",
        "numero_original_fuzzy": "str",
        "marca_ml": "str",
        "marca_siac": "str",
        "marca_fuzzy": "float",
        # "oem": "str",
        # "oem_siac": "str",
        # "oem_fuzzy": "float",
        # "oem_marca_ml": "str",
        # "oem_marca_siac": "str",
        # "oem_marca_fuzzy": "float",
        "lista_url_anuncios": "str",
        "lista_mlb": "str",
        "soma_fuzzy": "float"
    }

    _df_lojas_oficiais = DataFrame(
        columns=[
            "nome_loja_oficial",
            "url_loja_oficial",
            "lista_url_anuncios",
            "lista_mlbs"
        ]
    )

    _df_infos_mlb = DataFrame()

    _siac_fuzzy = None

    def __init__(self, site_lojas_oficiais) -> None:
        self._oficial_stores_website: bs4.BeautifulSoup = site_lojas_oficiais
        self._ml: MLInterface = MLInterface

    def select_seller(self, user_input: str):
        _nome_condicao = user_input in self._df_lojas_oficiais[
            'nome_loja_oficial'].str.upper().to_list()

        _url_condicao = user_input in self._df_lojas_oficiais[
            'url_loja_oficial'].str.upper().to_list()

        return (_nome_condicao, _url_condicao)

    def infos_lojas_oficiais(self):

        _informacoes_loja_oficial: InfosLojaOficial = InfosLojaOficial(
            self._oficial_stores_website
        )

        _lista_informacoes_loja_oficial: list[str] = _informacoes_loja_oficial.get_infos_seller(
        )

        self._df_lojas_oficiais.loc[:,
                                    'nome_loja_oficial'] = _lista_informacoes_loja_oficial[0]
        self._df_lojas_oficiais.loc[:,
                                    'url_loja_oficial'] = _lista_informacoes_loja_oficial[1]

        while True:
            try:
                entrada: str = input(
                    'Digite o nome da loja ou a url: ').upper()
                condicoes: tuple[bool] = self.select_seller(entrada)
                if True in condicoes:
                    break
                system('cls')
                pprint('Nao tenho esse vendedor !\n')
            except KeyboardInterrupt as e:
                pprint('Programa finalizado.')
                raise ValueError() from e

        with Client() as _client:
            if condicoes[0]:
                _url_loja_oficial = self._df_lojas_oficiais[
                    self._df_lojas_oficiais['nome_loja_oficial'].str.upper(
                    ) == entrada
                ]['url_loja_oficial'].values[0]
            if condicoes[1]:
                _url_loja_oficial = self._df_lojas_oficiais[
                    self._df_lojas_oficiais['url_loja_oficial'].str.upper(
                    ) == entrada
                ]['_url_loja_oficial'].values[0]

            _site_loja_oficial: bs4.BeautifulSoup = bs4.BeautifulSoup(
                _client.get(_url_loja_oficial,
                            timeout=None).content, 'html.parser'
            )

            self._df_lojas_oficiais = self._df_lojas_oficiais.query(
                'url_loja_oficial == @_url_loja_oficial').reset_index(drop=True).copy()

            self._df_lojas_oficiais.at[0, 'lista_url_anuncios'] = AnunciosLojaOficial(
                _site_loja_oficial).pegar_link_anuncios()

    def informacaoes_anuncios_api(self):
        _ml_interface: MLInterface = self._ml(1)
        _df: DataFrame = self._df_lojas_oficiais

        _idx = _df.index.values[0]

        _df['lista_mlbs'] = None
        _df['lista_infos_mlb'] = None
        _df['lista_att_necessarios'] = None

        _df.at[_idx, 'lista_mlbs'] = [PegarInfosAnuncioApi(
            _ml_interface).pegar_mlb_url(url_mlb)[0] for url_mlb in _df.loc[_idx, 'lista_url_anuncios']]

        _df.at[_idx, 'lista_infos_mlb'] = PegarInfosAnuncioApi(
            _ml_interface).pegar_infos_api(_df.loc[_idx, 'lista_mlbs']
                                           )

        _df.at[_idx, 'lista_att_necessarios'] = [
            dumps(PegarInfosAnuncioApi(
                _ml_interface).pegar_atributos_necessarios(
                    loads(atts))) for atts in _df.loc[_idx, 'lista_infos_mlb']
        ]

        self._df_infos_mlb: DataFrame = _df.copy()

    def get_infos_fuzzy(self):

        _df = SiacFuzzy(self._df_infos_mlb,
                        self._columns_default).created_new_dataframe()

        with ProcessPoolExecutor(max_workers=3) as executor:
            try:
                for future in tqdm(executor.map(
                    SiacFuzzy(self._df_infos_mlb,
                              self._columns_default).fuzzy_results,
                    _df['gtin_ml'],
                    _df['mpn_ml'],
                    _df['sku_ml'],
                    _df['numero_original_ml'],
                    _df['marca_ml'],
                    _df.index
                ), desc='Verificando matchs Ml/SIAC: ', total=len(_df)):
                    _df.at[future[6], 'gtin_siac'] = future[0][0]
                    _df.at[future[6], 'gtin_fuzzy'] = future[0][1]

                    _df.at[future[6], 'mpn_siac'] = future[1][0]
                    _df.at[future[6], 'mpn_fuzzy'] = future[1][1]

                    _df.at[future[6], 'sku_siac'] = future[2][0]
                    _df.at[future[6], 'sku_fuzzy'] = future[2][1]

                    _df.at[future[6], 'numero_original_siac'] = future[3][0]
                    _df.at[future[6], 'numero_original_fuzzy'] = future[3][1]

                    _df.at[future[6], 'marca_siac'] = future[4][0]
                    _df.at[future[6], 'marca_fuzzy'] = future[4][1]

                    _df.loc[future[6], 'soma_fuzzy'] = future[5]
            except KeyError as e:
                print('Erro!')
                print(e)
            finally:
                _df.sort_values('soma_fuzzy', ascending=False).to_feather(
                    'df_resultado_fuzzy')
                _df.sort_values('soma_fuzzy', ascending=False).to_excel(
                    'df_resultado_fuzzy.xlsx')


if __name__ == "__main__":

    PATH_HERE = path.dirname(__file__)

    with open('./data/html/Lojas oficiais.html', 'r', encoding='utf-8') as fp:
        site_lojas_oficiais_driver = fp.read()

    site_lojas_oficiais_driver: bs4.BeautifulSoup = bs4.BeautifulSoup(
        site_lojas_oficiais_driver,
        'html.parser'
    )

    if not path.exists(path.join(PATH_HERE, 'temp')):
        mkdir([path.join(PATH_HERE, 'temp')])

    main: Main = Main(site_lojas_oficiais_driver)
    main.infos_lojas_oficiais()
    main.informacaoes_anuncios_api()
    main.get_infos_fuzzy()
