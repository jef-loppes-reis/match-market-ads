"""---"""
from json import load
from os import path, mkdir
from re import sub
from typing import Union

from aplicacao import Aplicacao
from clone import Clonador
from ecomm import MLInterface
from httpx import Response
from rich import print as rprint
from rich.progress import Progress
from pandas import (
    read_excel,
    read_feather,
    read_csv,
    merge,
    isna,
    DataFrame,
    options
)

from comp_headler import CompatibilidadeHandler

options.display.max_columns = None


class UsoClonagem:
    """
     _summary_

    _extended_summary_

    Returns:
        _type_: _description_
    """

    __aplic: Aplicacao = Aplicacao()

    __caminho_fotos: str = '../photo_validation/out_files_photos/%s'

    __df_photos: DataFrame = DataFrame()
    __df_clona_genuino: DataFrame = DataFrame()
    __df_clona_copy: DataFrame = DataFrame()
    __df_clonados: DataFrame = DataFrame(
        columns=[
            'item_id_genuino',
            'item_id',
            'corpo_json',
            'compatibilidades',
            'descricao',
        ]
    )
    __df_ultima_clonagem: DataFrame = DataFrame()

    with open(file='./data/sale_terms.json', mode='r', encoding='utf-8') as fp:
        __sales_terms: dict = load(fp)

    def __init__(self, loja: str = 'takao') -> None:
        self.__ml: MLInterface = MLInterface(1)
        self.__headers: dict = self.__ml.headers()
        self.__nome_loja: str = loja.lower()
        self.__path_loja: str = f'./out/{self.__nome_loja}'
        self.__clonagem: Clonador = Clonador(
            headers=self.__headers,
            sales_terms=self.__sales_terms
        )

    def __get_number_photo(self, lista_name_photo: str) -> list[str]:
        """
        get_number_photo Metodo para pegar o numero da foto.

        _extended_summary_

        Args:
            lista_name_photo (str): Lista de fotos.

        Returns:
            list[str]: Lista de indices.
        """
        return [name.split('_')[-1:][0].replace('.jpg', '') for name in lista_name_photo]

    def __data_photos(self):
        self.__df_photos: DataFrame = read_feather(
            f'../temp/conferencia_fotos_{self.__nome_loja}.feather')
        self.__df_photos: DataFrame = self.__df_photos.query(
            '~mlb.isna()').reset_index(drop=True).copy()
        self.__df_photos['number_photo'] = None
        self.__df_photos.loc[:, 'number_photo'] = self.__get_number_photo(
            self.__df_photos.path_file_photo.fillna('0'))

    def __data_clona_genuino(self):
        self.__df_clona_genuino = read_excel(
            f'../data/planilhas_primeiro_processo/{self.__nome_loja}.xlsx', dtype=str).head(5)
        self.__df_clona_genuino = self.__df_clona_genuino.set_axis(
            self.__df_clona_genuino.columns.str.lower(), axis=1).copy()
        self.__df_clona_genuino.loc[:, 'sku_certo'] = list(
            map(lambda x: sub(r'\s', '', str(x)), self.__df_clona_genuino.loc[:, 'sku_certo'])
        )

    def __merge_datas(self):
        self.__data_clona_genuino()
        self.__data_photos()

        self.__df_clona_copy: DataFrame = merge(
            self.__df_clona_genuino,
            self.__df_photos,
            on='mlb',
            how='left'
        )

        self.__df_clona_copy: DataFrame = self.__df_clona_copy.astype(
            {'compat': bool})

        self.__df_clona_copy: DataFrame = self.__df_clona_copy.loc[
            (self.__df_clona_copy['clona'] == "1") &
            (self.__df_clona_copy['compat']) &
            (self.__df_clona_copy['verifeid_photo'] & self.__df_clona_copy['pegar_foto'])
        ]

        self.__df_clona_copy: DataFrame = self.__df_clona_copy.sort_values(
            ['mlb', 'number_photo']
        ).reset_index(drop=True)

    def __sava_data_clonados(self):
        try:
            self.__df_ultima_clonagem: DataFrame = read_csv(
                f'./out/{self.__nome_loja}/df_clonados_{self.__nome_loja}.csv')
            self.__df_clona_copy = self.__df_clona_copy[
                ~self.__df_clona_copy['mlb'].isin(
                    self.__df_ultima_clonagem['item_id_genuino'])]
        except FileNotFoundError as e:
            rprint('[yellow]\nAlgo de errado com o arquivo ![/yellow]')
            rprint(e.__class__.__name__)

    def __created_dir(self):
        if not path.exists(self.__path_loja):
            mkdir(self.__path_loja)

    def main(self):
        self.__created_dir()
        self.__merge_datas()

        self.__clonagem.read_product_siac()

        self.__aplic.read_apl()
        self.__aplic.criar_ano_inicial_ano_final()

        with Progress() as process:
            __produtos: list[str] = self.__df_clona_copy.mlb.unique()
            __task = process.add_task("[cyan]Processando...", total=len(__produtos))
            __mlb: str = None
            __codpro: str = None
            __sku_siac: str = None
            __sku_certo: str = None
            __sku_sentinela: str = None
            __quebra_sku: list[str] = []
            __mlb_info: tuple[Response, Response] = (Response, Response)
            __retorno_cadastro: dict = {}
            for __mlb in __produtos:
                __row: DataFrame = self.__df_clona_copy.query(
                    f'mlb == "{__mlb}"').reset_index(drop=True).copy()
                __sku_siac: str = __row['sku_siac'].unique()[0]
                __sku_certo: str = __row['sku_certo'].unique()[0]

                if isna(__sku_certo) or __sku_certo == 'nan':
                    __codpro: str = self.__clonagem.df_siac.query(
                        'num_fab == @__sku_siac'
                    )['codpro'].unique()[0]
                    __sku_sentinela: str = __codpro
                else:
                    __codpro: str = self.__clonagem.convert_sku_to_codpro(
                        sku=__sku_certo
                    )[0]
                    __sku_sentinela: str = __sku_certo

                if '_' in __row.loc[0, 'sku_certo']:
                    __quebra_sku: list[str] = __row.loc[0, 'sku_certo'].split('_')[1:]
                    __kit: tuple[bool, int] = (True, len(__quebra_sku))
                else:
                    __quebra_sku: list[str] = []
                    __kit: tuple[bool, int] = (False, 0)

                try:
                    self.__clonagem.read_df_siac_filter(
                        codpro_produto=__codpro.split()
                    )
                except KeyError:
                    rprint(f'O MLB {__mlb} selecionado, nao tem dados com esse codpro {__codpro}, estou pulando ele.')
                    continue

                # __mlb_info: tuple[Response, Response] = self.__clonagem.mlb_infos(
                #     item_id_ml=__row.loc[0, 'mlb']
                # )

                self.__clonagem.gerar_payload_cadastro(
                    list_path_img=[
                        self.__caminho_fotos % f'{self.__nome_loja}/{name_photo}' for name_photo in __row['path_file_photo']
                    ],
                    sku=__sku_sentinela
                )

                self.__clonagem.corpo_clonagem.update(
                    {
                        'description': self.__aplic.criar_descricao(
                            codpro=__codpro,
                            aplicacao_veiculos=self.__aplic.get_aplicacao(
                                original=self.__clonagem.df_siac_filter.loc[0, 'num_orig']
                            ),
                            marca=self.__clonagem.df_siac_filter.loc[0, 'marca'],
                            multiplo_venda=self.__clonagem.df_siac_filter.loc[0, 'embala'],
                            kit=__kit[0],
                            num_fab=__sku_sentinela,
                            oems=self.__aplic.lista_originais(
                                self.__clonagem.df_siac_filter.loc[0, 'num_orig']
                            ),
                            titulo=self.__clonagem.corpo_clonagem.get('title')
                        )
                    }
                )

                if self.__clonagem.corpo_clonagem.get('pictures') is None:
                    rprint(f'O MLB {__mlb} selecionado, nao tem foto aprovada, estou passando ele.')
                    continue

                try:
                    __retorno_cadastro: dict = self.__ml.post_item(
                        item=self.__clonagem.corpo_clonagem
                    )
                except Exception as e:
                    rprint(e)
                    rprint(self.__clonagem.corpo_clonagem)
                    continue

                # ? Metodo de compatibilidades:

                __compati: CompatibilidadeHandler = CompatibilidadeHandler(
                    base_url='https://api.mercadolibre.com/items',
                    headers=self.__headers
                )

                __response_compati: Union[int, dict] = __compati.compatibilidades(
                    item_id_ml_clone=__mlb,
                    item_id_novo=__retorno_cadastro.get('id')
                )

                if __response_compati == 0:
                    rprint(f'\n[yellow][{__mlb}] Nao tem compatibilidades ![/yellow]')
                else:
                    rprint(f'\n[green][{__mlb}] = {__response_compati} compatibilidades cadastradas ![/green]')

                self.__sava_data_clonados()

                process.update(__task, advance=1)

if __name__ == '__main__':
    uso: UsoClonagem = UsoClonagem(
        loja='takao'
    )
    uso.main()
