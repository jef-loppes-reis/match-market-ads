"""

Returns:
    _type_: _description_
"""
from os import path, mkdir, system
from concurrent.futures import ProcessPoolExecutor
from json import loads, dumps

import bs4
from pandas import DataFrame, Series
from httpx import Client
from tqdm import tqdm
from rich import print as rprint
from ecomm import MLInterface

from utils.anuncios_loja_oficial import AnunciosLojaOficial
from utils.pegar_infos_anuncio_api import PegarInfosAnuncioApi
from utils.siac_fuzzy import SiacFuzzy


class Main:
    """_summary_

    Raises:
        ValueError: _description_

    Returns:
        _type_: _description_
    """

    _columns_default: dict[str, str] = {
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
        "mpn_marca_ml": "str",
        "mpn_marca_siac": "str",
        "mpn_marca_fuzzy": "float",
        "lista_url_anuncios": "str",
        "lista_mlb": "str",
        "soma_fuzzy": "float",
        "clona": "str"
    }
    _nome_loja = None
    _df_lojas_oficiais: DataFrame = DataFrame(
        columns=[
            "nome_loja_oficial",
            "url_loja_oficial",
            "lista_url_anuncios",
            "lista_mlbs"
        ]
    )

    _df_infos_mlb = DataFrame()

    _siac_fuzzy = None

    def __init__(self) -> None:
        self._ml: MLInterface = MLInterface

    def infos_lojas_oficiais(self):
        entrada = None
        _url_loja_oficial = None
        rprint('[bright_yellow]Digite o nome da loja:[/bright_yellow]')
        self._nome_loja: str = input().upper()
        rprint('[bright_yellow]Digite o link da pagina de anuncios:[/bright_yellow]')
        entrada: str = input()
        _url_loja_oficial = entrada

        with Client() as _client:

            _site_loja_oficial: bs4.BeautifulSoup = bs4.BeautifulSoup(
                _client.get(_url_loja_oficial, timeout=None).content,
                'html.parser'
            )

            _lista_link_anuncios_seller: list[str] = AnunciosLojaOficial(
                _site_loja_oficial).pegar_link_anuncios()

            self._df_lojas_oficiais['lista_url_anuncios'] = Series(
                _lista_link_anuncios_seller)

    def informacaoes_anuncios_api(self):
        """Método para fazer as requisições de todas as informações dos anúncios da página oficial.
        """
        _ml_interface: MLInterface = self._ml(1)
        _df: DataFrame = self._df_lojas_oficiais

        # _idx: int = _df.index.values[0]

        _df['lista_mlbs'] = None
        _df['lista_infos_mlb'] = None
        _df['lista_att_necessarios'] = None

        _df.loc[:, 'lista_mlbs'] = Series([
            PegarInfosAnuncioApi(_ml_interface)
            .pegar_mlb_url(url_mlb) for url_mlb in _df.loc[:, 'lista_url_anuncios']
        ])

        _df.loc[:, 'lista_infos_mlb'] = Series(
            PegarInfosAnuncioApi(_ml_interface)
            .pegar_infos_api(_df.loc[:, 'lista_mlbs'].to_list())
        )

        _df.at[:, 'lista_att_necessarios'] = Series(
            [
                dumps(PegarInfosAnuncioApi(_ml_interface)
                      .pegar_atributos_necessarios(loads(atts))
                      ) for atts in _df.loc[:, 'lista_infos_mlb']
            ]
        )

        # _df.to_excel('teste_informacoes_anuncios.xlsx')

        self._df_infos_mlb: DataFrame = _df.reset_index(drop=True).copy()

    def get_infos_fuzzy(self):
        """Metodo para criar uma lista de matchs com a biblioteca Fuzzy. Comparando os produtos
        do Mercado Livre com os produtos do SIAC.
        """
        _path_fuzzys: str = f'./fuzzys/{self._nome_loja}'
        if not path.exists(_path_fuzzys):
            mkdir(_path_fuzzys)

        _df: DataFrame = SiacFuzzy(
            self._df_infos_mlb,
            self._columns_default
        ).created_new_dataframe()

        _siac_fuzzy: SiacFuzzy = SiacFuzzy(
            self._df_infos_mlb,
            self._columns_default
        )

        with ProcessPoolExecutor(max_workers=12) as executor:
            try:
                for future in tqdm(executor.map(_siac_fuzzy.fuzzy_results,
                                                _df['gtin_ml'],
                                                _df['mpn_ml'],
                                                _df['sku_ml'],
                                                _df['numero_original_ml'],
                                                _df['marca_ml'],
                                                _df['mpn_marca_ml'],
                                                _df.index
                                                ),
                                   desc='Verificando matchs Ml/SIAC: ',
                                   colour='green', total=len(_df)
                                   ):
                    _df.at[future.get('df_index'), 'gtin_siac'] = future.get(
                        'gtin_siac')
                    _df.at[future.get('df_index'), 'gtin_fuzzy'] = future.get(
                        'score_gtin_before')

                    _df.at[future.get('df_index'), 'mpn_siac'] = future.get(
                        'mpn_siac')
                    _df.at[future.get('df_index'), 'mpn_fuzzy'] = future.get(
                        'score_mpn_before')

                    _df.at[future.get('df_index'), 'sku_siac'] = future.get(
                        "sku_siac")
                    _df.at[future.get('df_index'), 'sku_fuzzy'] = future.get(
                        'score_sku_before')

                    _df.at[future.get('df_index'), 'numero_original_siac'] = future.get(
                        'num_orig_siac')
                    _df.at[future.get('df_index'), 'numero_original_fuzzy'] = future.get(
                        'score_num_orig_before')

                    _df.at[future.get('df_index'), 'marca_siac'] = future.get(
                        'marca_siac')
                    _df.at[future.get('df_index'), 'marca_fuzzy'] = future.get(
                        'score_marca_before')

                    _df.at[future.get('df_index'), 'mpn_marca_siac'] = future.get(
                        'mpn_marca_siac')
                    _df.at[future.get('df_index'), 'mpn_marca_fuzzy'] = future.get(
                        'score_mpn_marca_before')

                    _df.loc[future.get('df_index'), 'soma_fuzzy'] = future.get(
                        'sum_scores')

                # _df = _df.loc[:, 2:]
            except KeyError as e:
                print('Erro!')
                print(e)
            finally:
                _df.sort_values('soma_fuzzy', ascending=False).to_feather(
                    f'{_path_fuzzys}/df_{self._nome_loja}_fuzzy'
                )
                _df.sort_values('soma_fuzzy', ascending=False).to_excel(
                    f'{_path_fuzzys}/df_{self._nome_loja}_fuzzy.xlsx'
                )


if __name__ == "__main__":

    PATH_HERE = path.dirname(__file__)

    with open('./data/html/Lojas oficiais.html', 'r', encoding='utf-8') as fp:
        site_lojas_oficiais_driver = fp.read()

    site_lojas_oficiais_driver: bs4.BeautifulSoup = bs4.BeautifulSoup(
        site_lojas_oficiais_driver,
        'html.parser'
    )

    if not path.exists(path.join(PATH_HERE, 'temp')):
        mkdir(path.join(PATH_HERE, 'temp'))

    main: Main = Main()
    main.infos_lojas_oficiais()
    main.informacaoes_anuncios_api()
    main.get_infos_fuzzy()
