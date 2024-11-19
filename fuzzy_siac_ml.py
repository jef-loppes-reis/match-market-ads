"""

Returns:
    _type_: _description_
"""
from os import path, mkdir, listdir, makedirs
from concurrent.futures import ProcessPoolExecutor
from json import loads, dumps

import bs4
from pandas import DataFrame, Series, read_feather, concat, isna
from httpx import Client
from tqdm import tqdm
from rich import print as rprint
from ecomm import MLInterface

from utils.anuncios_loja_oficial import AnunciosLojaOficial
from utils.pegar_infos_anuncio_api import PegarInfosAnuncioApi
from utils.siac_fuzzy import SiacFuzzy
from utils.get_fuzzy_grupo import GetFuzzyGrupos


class Main:
    """
     Classe principal para chamar todos os paramentros
     e retornar a planilha de produtos.

    Returns
    -------
    _type_
        _description_
    """
    # Lista de colunas da planilha final.
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
    _nome_loja: str = None
    _nome_linha: str = None
    _df_lojas_oficiais: DataFrame = DataFrame(
        columns=[
            "nome_loja_oficial",
            "url_loja_oficial",
            "lista_url_anuncios",
            "lista_tag_mais_vendido",
            "lista_tag_avaliacao",
            "lista_vendas",
            "lista_mlbs",
            "compat"
        ]
    )

    _df_infos_mlb = DataFrame()

    _siac_fuzzy = None
    _url_loja_oficial = None

    def __init__(self) -> None:
        self._ml: MLInterface = MLInterface

    def reset_df_infos(self):
        """
        reset_df_infos Reseta os parametro do DataFrame final.
        """
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
        """
        infos_lojas_oficiais Funcao para atribuir valor no data frame,
        listando das as informacoes de cada anuncio da pagina.

        Parameters
        ----------
        loja : str
            Nome da loja oficial.
        linha : str
            Linha do produto.
        url : str
            URL da pagina oficial.
        """
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

            _lista_link_anuncios_seller: dict[str, str | float] = (
                _an_loja_oficial.pegar_link_anuncios()
            )

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

        _df['lista_infos_mlb'] = None
        _df['lista_att_necessarios'] = None
        _df['compat'] = None

        _lista_infos_mlb_res: list[dict] = (
            PegarInfosAnuncioApi(_ml_interface)
            .pegar_infos_api(_df.loc[:, 'lista_mlbs'].to_list())
        )
        _df.loc[:, 'lista_infos_mlb'] = Series(_lista_infos_mlb_res)

        _df.loc[:, 'compat'] = (
            PegarInfosAnuncioApi(_ml_interface)
            .check_compatibilities(_df.loc[:, 'lista_mlbs'])
        )

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
        rprint(self._df_infos_mlb)

    def get_infos_fuzzy(self):
        """Método para criar uma lista de matchs com a biblioteca Fuzzy,
        comparando os produtos do Mercado Livre com os produtos do SIAC.
        """
        _path_fuzzys: str = f'./fuzzys/{self._nome_loja}'

        makedirs('./fuzzys', exist_ok=True)
        makedirs(_path_fuzzys, exist_ok=True)

        # Inicializando o objeto SiacFuzzy
        _siac_fuzzy: SiacFuzzy = SiacFuzzy(
            self._df_infos_mlb,
            self._columns_default,
            './data/sql/produto_siac.sql'
        )
        _siac_fuzzy.read_db()

        # Criando novo dataframe e copiando as colunas
        _df: DataFrame = _siac_fuzzy.created_new_dataframe()
        columns_to_copy: list[str] = [
            'lista_tag_mais_vendido',
            'lista_tag_avaliacao',
            'lista_vendas',
            'compat'
        ]
        _df[columns_to_copy] = self._df_infos_mlb[columns_to_copy].copy()

        # Realizando o processamento paralelo e aplicando fuzzy matching
        with ProcessPoolExecutor() as executor:
            # Cria uma lista de futures.
            futures = [executor.submit(
                _siac_fuzzy.fuzzy_results,
                gtin_ml,
                mpn_ml,
                sku_ml,
                numero_original_ml,
                marca_ml,
                mpn_marca_ml,
                produto_ml,
                index
            )
            for gtin_ml, mpn_ml, sku_ml, numero_original_ml,
                marca_ml, mpn_marca_ml, produto_ml, index in zip(
                    _df['gtin_ml'],
                    _df['mpn_ml'],
                    _df['sku_ml'],
                    _df['numero_original_ml'],
                    _df['marca_ml'],
                    _df['mpn_marca_ml'],
                    _df['produto_ml'],
                    _df.index
                )]
            # Coletando resultados dos futures e atualizando o DataFrame
            for future in tqdm(iterable=futures,
                               desc='Verificando matchs Ml/SIAC.: ',
                               colour='green',
                               total=len(_df)):
                result: dict = future.result()
                df_index: int = result.get('df_index')
                for col_suffix, key in zip(
                    ['gtin', 'mpn', 'sku', 'numero_original', 'marca', 'mpn_marca', 'produto'],
                    ['gtin', 'mpn', 'sku', 'num_orig', 'marca', 'mpn_marca', 'produto']
                ):
                    _df.at[df_index, f'{col_suffix}_siac'] = result.get(f'{key}_siac')
                    _df.at[df_index, f'{col_suffix}_fuzzy'] = result.get(f'score_{key}_before')

                _df.at[df_index, 'soma_fuzzy'] = result.get('sum_scores')
        # Salvando DataFrame ordenado
        sorted_df: DataFrame = _df.sort_values('soma_fuzzy', ascending=False)
        sorted_df.to_feather(f'{_path_fuzzys}/df_{self._nome_loja}_{self._nome_linha}_fuzzy')
        sorted_df.to_excel(f'{_path_fuzzys}/df_{self._nome_loja}_{self._nome_linha}_fuzzy.xlsx')

    def filter_top(self, df: DataFrame):
        df.loc[:, 'soma_fuzzy'] = df.loc[
            :, 'soma_fuzzy'].str.replace('%','').astype(float)
        df.loc[:, 'lista_tag_avaliacao'] = df.loc[
            :, 'lista_tag_avaliacao'].fillna(0).astype(float)
        df.loc[:, 'lista_vendas'] = df.loc[
            :, 'lista_vendas'].fillna(0).astype(int)
        __df_results: DataFrame = DataFrame()
        for mpn_unico in tqdm(df.query('~mpn_ml.duplicated()')['mpn_ml']):
            if not isna(mpn_unico):
                df_filter_copy: DataFrame = df.query('mpn_ml == @mpn_unico').copy()
                df_filter_copy: DataFrame = df_filter_copy.sort_values([
                    'lista_vendas',
                    'soma_fuzzy',
                    'lista_tag_mais_vendido',
                    'lista_tag_avaliacao'],
                    ascending=False
                ).reset_index(drop=True)
                __df_results = concat(
                    [
                        __df_results,
                        df_filter_copy.loc[[0]]
                    ]
                )
        return __df_results

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
        return self.filter_top(__df_copy)


if __name__ == "__main__":

    from re import sub

    PATH_HERE = path.dirname(__file__)

    if not path.exists(path.join(PATH_HERE, 'temp')):
        mkdir(path.join(PATH_HERE, 'temp'))

    MARCA: str = 'VALEO' # De acordo com o nome do SIAC.
    NOME_LOJA: str = 'valeo'

    grupos: GetFuzzyGrupos = GetFuzzyGrupos(marca=MARCA)
    main: Main = Main()

    df_grupos: DataFrame = grupos.main()
    df_grupos = df_grupos.copy()

    grup: str = ''
    for grup in tqdm(iterable=df_grupos.grupo_subgrupo,
                     desc=f'Grupos da {MARCA}.: ',
                     colour='yellow',
                     total=len(df_grupos)
                ):
        rprint(f'\n\t[yellow]{grup}[/yellow]')
        _group: str = sub(r'\s', '+', grup.lower())
        main.reset_df_infos()
        try:
            main.infos_lojas_oficiais(
                loja=MARCA.lower(),
                linha=grup.lower(),
                url=f'https://lista.mercadolivre.com.br/{_group.replace(
                    '+', '-')}_Loja_{NOME_LOJA}'
            )
            main.informacaoes_anuncios_api()
            main.get_infos_fuzzy()
        except Exception as e:
            rprint(e)
            raise

    main.juntar_resultados(marca=MARCA).to_excel(
        f'total_produtos_{MARCA.lower()}.xlsx')
