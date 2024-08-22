"""

Returns:
    _type_: _description_
"""
from os import path, mkdir, listdir
from concurrent.futures import ProcessPoolExecutor
from json import loads, dumps

import bs4
from pandas import DataFrame, Series, read_feather, concat
from httpx import Client
from tqdm import tqdm
from rich import print as rprint
from ecomm import MLInterface

from utils.anuncios_loja_oficial import AnunciosLojaOficial
from utils.pegar_infos_anuncio_api import PegarInfosAnuncioApi
from utils.siac_fuzzy import SiacFuzzy
from utils.get_fuzzy_grupo import GetFuzzyGrupos


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
        "lista_tag_mais_vendido": "str",
        "lista_tag_avaliacao": "str",
        "gtin_ml": "str",
        "gtin_siac": "str",
        "gtin_fuzzy": "float",
        "produto_ml": "str",
        "produto_siac": "str",
        "produto_fuzzy": "str",
        "mpn_ml": "str",
        "mpn_siac": "str",
        "mpn_fuzzy": "float",
        "sku_ml": "str",
        "sku_siac": "str",
        "sku_fuzzy": "float",
        "sku_certo": "str",
        "numero_original_ml": "str",
        "numero_original_siac": "str",
        "numero_original_fuzzy": "str",
        "marca_ml": "str",
        "marca_siac": "str",
        "marca_fuzzy": "float",
        "mpn_marca_ml": "str",
        "mpn_marca_siac": "str",
        "mpn_marca_fuzzy": "float",
        "lista_url_anuncios": "str",
        "lista_mlb": "str",
        "soma_fuzzy": "float",
        "clona": "str"
    }
    _nome_loja = None
    _nome_linha = None
    _df_lojas_oficiais: DataFrame = DataFrame(
        columns=[
            "nome_loja_oficial",
            "url_loja_oficial",
            "lista_url_anuncios",
            "lista_tag_mais_vendido",
            "lista_tag_avaliacao",
            "lista_vendas",
            "lista_mlbs"
        ]
    )

    _df_infos_mlb = DataFrame()

    _siac_fuzzy = None
    _url_loja_oficial = None

    def __init__(self) -> None:
        self._ml: MLInterface = MLInterface

    def reset_df_infos(self):
        self._df_infos_mlb: DataFrame = DataFrame().copy()
        self._df_lojas_oficiais: DataFrame = DataFrame(
            columns=[
                "nome_loja_oficial",
                "url_loja_oficial",
                "lista_url_anuncios",
                "lista_tag_mais_vendido",
                "lista_tag_avaliacao",
                "lista_vendas",
                "lista_mlbs"
            ]
        ).copy()

    def infos_lojas_oficiais(self, loja: str, linha: str, url: str):

        self._nome_loja: str = loja
        self._nome_linha: str = linha
        self._url_loja_oficial: str = url

        with Client() as _client:

            rprint({'Link loja': self._url_loja_oficial})
            _site_loja_oficial: bs4.BeautifulSoup = bs4.BeautifulSoup(
                _client.get(self._url_loja_oficial, timeout=None).content,
                'html.parser'
            )

            _an_loja_oficial: AnunciosLojaOficial = AnunciosLojaOficial(
                _site_loja_oficial)

            _lista_link_anuncios_seller: dict[str, str | float] = _an_loja_oficial.pegar_link_anuncios()

            self._df_lojas_oficiais['lista_url_anuncios'] = Series(
                _lista_link_anuncios_seller.get('lista_link_anuncios')
            )
            self._df_lojas_oficiais['lista_tag_mais_vendido'] = Series(
                _lista_link_anuncios_seller.get('lista_tag_mais_vendido')
            )
            self._df_lojas_oficiais['lista_tag_avaliacao'] = Series(
                _lista_link_anuncios_seller.get('lista_tag_avaliacao')
            )
            self._df_lojas_oficiais['lista_vendas'] = Series(
                _lista_link_anuncios_seller.get('lista_vendas')
            )
            self._df_lojas_oficiais['lista_mlbs'] = Series(
                _lista_link_anuncios_seller.get('lista_mlbs')
            )


    def informacaoes_anuncios_api(self):
        """Método para fazer as requisições de todas as informações dos anúncios da página oficial.
        """
        _ml_interface: MLInterface = self._ml(1)
        _df: DataFrame = self._df_lojas_oficiais

        # _idx: int = _df.index.values[0]

        # _df['lista_mlbs'] = None
        _df['lista_infos_mlb'] = None
        _df['lista_att_necessarios'] = None

        # _lista_mlb_res: list[str] = [
        #     PegarInfosAnuncioApi(_ml_interface)
        #     .pegar_mlb_url(url_mlb) for url_mlb in _df.loc[:, 'lista_url_anuncios']
        # ]
        # _df.loc[:, 'lista_mlbs'] = Series(_lista_mlb_res)

        _lista_infos_mlb_res: list[dict] = (
            PegarInfosAnuncioApi(_ml_interface)
            .pegar_infos_api(_df.loc[:, 'lista_mlbs'].to_list())
        )
        _df.loc[:, 'lista_infos_mlb'] = Series(_lista_infos_mlb_res)
        rprint(_df)

        _lista_att_necessarios_res: list[dict] = []
        for atts in _df.loc[:, 'lista_infos_mlb']:
            dict_atts: dict = PegarInfosAnuncioApi(
                _ml_interface).pegar_atributos_necessarios(loads(atts))
            _lista_att_necessarios_res.append(dumps(dict_atts))

        _df.at[:, 'lista_att_necessarios'] = Series(_lista_att_necessarios_res)

        self._df_infos_mlb: DataFrame = (
            _df
            .reset_index(drop=True).copy()
        )

    def get_infos_fuzzy(self):
        """Metodo para criar uma lista de matchs com a biblioteca Fuzzy. Comparando os produtos
        do Mercado Livre com os produtos do SIAC.
        """
        _path_fuzzys: str = f'./fuzzys/{self._nome_loja}'
        if not path.exists(_path_fuzzys):
            mkdir(_path_fuzzys)

        _siac_fuzzy: SiacFuzzy = SiacFuzzy(
            self._df_infos_mlb,
            self._columns_default,
            './data/sql/produto_siac.sql'
        )

        _siac_fuzzy.read_db()

        _df: DataFrame = _siac_fuzzy.created_new_dataframe()

        _df.loc[:, 'lista_tag_mais_vendido'] = (
            self._df_infos_mlb.loc[:,'lista_tag_mais_vendido'].copy()
        )
        _df.loc[:, 'lista_tag_avaliacao'] = (
            self._df_infos_mlb.loc[:, 'lista_tag_avaliacao'].copy()
        )
        _df.loc[:, 'lista_vendas'] = (
            self._df_infos_mlb.loc[:, 'lista_vendas'].copy()
        )

        with ProcessPoolExecutor(max_workers=None) as executor:
            for future in tqdm(executor.map(_siac_fuzzy.fuzzy_results,
                                            _df['gtin_ml'],
                                            _df['mpn_ml'],
                                            _df['sku_ml'],
                                            _df['numero_original_ml'],
                                            _df['marca_ml'],
                                            _df['mpn_marca_ml'],
                                            _df['produto_ml'],
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
                
                _df.at[future.get('df_index'), 'produto_siac'] = future.get(
                    'produto_siac')
                _df.at[future.get('df_index'), 'produto_fuzzy'] = future.get(
                    'score_produto_before')

                _df.loc[future.get('df_index'), 'soma_fuzzy'] = future.get(
                    'sum_scores')


            _df.sort_values('soma_fuzzy', ascending=False).to_feather(
                f'{_path_fuzzys}/df_{self._nome_loja}_{self._nome_linha}_fuzzy'
            )
            _df.sort_values('soma_fuzzy', ascending=False).to_excel(
                f'{_path_fuzzys}/df_{self._nome_loja}_{self._nome_linha}_fuzzy.xlsx'
            )

    def juntar_resultados(self, marca: str) -> DataFrame:
        marca: str = marca.lower()
        __arquivos: list[str] = listdir(f'./fuzzys/{marca}')
        __df_copy: DataFrame = DataFrame()
        __here: str = path.dirname(__file__)
        for arquivo in tqdm(__arquivos, desc='Juntando resultados: '):
            if not arquivo.endswith('xlsx'):
                _df: DataFrame = read_feather(path.join(
                    __here, 'fuzzys', marca, arquivo)).copy()
                __df_copy: DataFrame = concat([__df_copy, _df])
        return __df_copy


if __name__ == "__main__":

    from re import sub

    PATH_HERE = path.dirname(__file__)

    if not path.exists(path.join(PATH_HERE, 'temp')):
        mkdir(path.join(PATH_HERE, 'temp'))

    MARCA: str = 'BOSCH'

    grupos: GetFuzzyGrupos = GetFuzzyGrupos(marca=MARCA)
    main: Main = Main()

    df_grupos: DataFrame = grupos.main().head(10)
    grup: str = ''

    for grup in tqdm(df_grupos.grupo_subgrupo,
                     desc=f'Grupos da {MARCA}.: ',
                     colour='yellow', total=len(df_grupos)):
        rprint(f'\n[yellow]Grupo {grup}[/yellow]')
        _group: str = sub(r'\s', '+', grup.lower())
        main.reset_df_infos()
        try:
            main.infos_lojas_oficiais(
                loja=MARCA.lower(),
                linha=grup.lower(),
                url=f'https://lista.mercadolivre.com.br/{_group.replace(
                    '+', '-')}_Loja_bosch-autopecas'
            )
            main.informacaoes_anuncios_api()
            main.get_infos_fuzzy()
        except Exception as e:
            rprint(e)
            continue

    main.juntar_resultados(marca=MARCA).to_excel(
        f'total_produtos_{MARCA.lower()}.xlsx')
