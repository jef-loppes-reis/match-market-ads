from os import path, mkdir, makedirs, listdir
import re
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
    Classe principal para manipular dados de anúncios de uma loja oficial,
    utilizando comparações fuzzy com o SIAC e criando uma planilha de produtos.
    """
    _columns_default = {
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

    def __init__(self, nome_loja: str) -> None:
        self._nome_loja: str = self.__sanitize_filename(nome_loja).lower()
        self._df_infos_mlb = DataFrame()
        self._df_lojas_oficiais = DataFrame(columns=[
            "nome_loja_oficial", "url_loja_oficial", "lista_url_anuncios",
            "lista_tag_mais_vendido", "lista_tag_avaliacao", "lista_vendas", 
            "lista_mlbs", "compat"
        ])
        self._ml = MLInterface(1)

    def __sanitize_filename(self, filename: str, replacement: str = "_") -> str:
        """
        Substitui caracteres proibidos em nomes de arquivos por um caractere seguro.

        Parameters
        ----------
        filename : str
            Nome de arquivo original que precisa ser sanitizado.
        replacement : str, opcional
            Caractere usado para substituir os caracteres proibidos, por padrão "_".

        Returns
        -------
        str
            Nome de arquivo sanitizado, seguro para ser salvo em sistemas de arquivos.
        """
        # Define uma expressão regular para corresponder aos caracteres proibidos
        invalid_chars = r'[\/:*?"<>|]'
        sanitized_filename = re.sub(invalid_chars, replacement, filename)
        return sanitized_filename

    def reset_df_infos(self):
        """Reseta o DataFrame com informações dos anúncios da loja."""
        self._df_infos_mlb = DataFrame()
        self._df_lojas_oficiais = DataFrame(columns=[
            "nome_loja_oficial", "url_loja_oficial", "lista_url_anuncios",
            "lista_tag_mais_vendido", "lista_tag_avaliacao", "lista_vendas",
            "lista_mlbs"
        ])

    def infos_lojas_oficiais(self, url: str):
        """Busca e organiza informações dos anúncios na página da loja."""
        try:
            with Client() as client:
                rprint({'Link loja': url})
                page_content = client.get(url, timeout=None).content
                site_data = bs4.BeautifulSoup(page_content, 'html.parser')

                anuncios = AnunciosLojaOficial(site_data).pegar_link_anuncios()
                for col, data in anuncios.items():
                    self._df_lojas_oficiais[col] = Series(data)

        except Exception as e:
            rprint(f"Erro ao buscar informações da loja oficial: {e}")

    def informacoes_anuncios_api(self):
        """Obtém dados dos anúncios via API e popula o DataFrame."""
        try:
            self._df_lojas_oficiais['lista_infos_mlb'] = PegarInfosAnuncioApi(
                self._ml).pegar_infos_api(self._df_lojas_oficiais['lista_mlbs'].to_list())
            self._df_lojas_oficiais['compat'] = PegarInfosAnuncioApi(
                self._ml).check_compatibilities(self._df_lojas_oficiais['lista_mlbs'])

            # Processa atributos necessários
            self._df_lojas_oficiais['lista_att_necessarios'] = [
                dumps(PegarInfosAnuncioApi(self._ml).pegar_atributos_necessarios(loads(atts)))
                for atts in self._df_lojas_oficiais['lista_infos_mlb']
            ]
            self._df_infos_mlb = self._df_lojas_oficiais.reset_index(drop=True)
            rprint(self._df_infos_mlb)

        except Exception as e:
            rprint(f"Erro ao obter informações dos anúncios via API: {e}")

    def get_infos_fuzzy(self, linha_produto: str):
        """Aplica comparações fuzzy para correlacionar produtos entre sistemas."""
        fuzzy_path = f'./out/fuzzys/{self._nome_loja}'
        makedirs(fuzzy_path, exist_ok=True)

        siac_fuzzy: SiacFuzzy = SiacFuzzy(
            self._df_infos_mlb,
            self._columns_default,
            './data/sql/produto_siac.sql'
        )
        siac_fuzzy.read_db()
        df_fuzzy: DataFrame = siac_fuzzy.created_new_dataframe()

        # Copia colunas para o novo DataFrame
        columns_to_copy: list[str] = [
            'lista_tag_mais_vendido',
            'lista_tag_avaliacao',
            'lista_vendas',
            'compat'
        ]
        df_fuzzy[columns_to_copy] = self._df_infos_mlb[columns_to_copy]

        # Executa o processamento em paralelo
        with ProcessPoolExecutor() as executor:
            futures = [
                executor.submit(siac_fuzzy.fuzzy_results, gtin_ml, mpn_ml, sku_ml,
                                numero_original_ml, marca_ml, mpn_marca_ml, produto_ml, index)
                for gtin_ml, mpn_ml, sku_ml, numero_original_ml, marca_ml,
                    mpn_marca_ml, produto_ml, index in zip(
                        df_fuzzy['gtin_ml'], df_fuzzy['mpn_ml'], df_fuzzy['sku_ml'],
                        df_fuzzy['numero_original_ml'], df_fuzzy['marca_ml'],
                        df_fuzzy['mpn_marca_ml'], df_fuzzy['produto_ml'], df_fuzzy.index
                )
            ]

            for future in tqdm(futures,
                               desc='Verificando matchs Ml/SIAC:',
                               colour='green',
                               total=len(df_fuzzy)
            ):
                result = future.result()
                df_index = result['df_index']
                for col_suffix, key in zip(
                    ['gtin', 'mpn', 'sku', 'numero_original', 'marca', 'mpn_marca', 'produto'],
                    ['gtin', 'mpn', 'sku', 'num_orig', 'marca', 'mpn_marca', 'produto']
                ):
                    df_fuzzy.at[df_index, f'{col_suffix}_siac'] = result.get(f'{key}_siac')
                    df_fuzzy.at[df_index, f'{col_suffix}_fuzzy'] = result.get(f'score_{key}_before')
                df_fuzzy.at[df_index, 'soma_fuzzy'] = result.get('sum_scores')

        _linha_produto_normalize: str = self.__sanitize_filename(linha_produto).lower()

        sorted_df = df_fuzzy.sort_values('soma_fuzzy', ascending=False)
        sorted_df.to_feather(f'{fuzzy_path}/df_{self._nome_loja}_{_linha_produto_normalize}_fuzzy.feather')
        sorted_df.to_excel(f'{fuzzy_path}/df_{self._nome_loja}_{_linha_produto_normalize}_fuzzy.xlsx')

    def filter_top(self, df: DataFrame) -> DataFrame:
        """Aplica filtros no DataFrame para selecionar os melhores resultados."""
        df['soma_fuzzy'] = df['soma_fuzzy'].str.replace('%', '').astype(float)
        df['lista_tag_avaliacao'] = df['lista_tag_avaliacao'].fillna(0).astype(float)
        df['lista_vendas'] = df['lista_vendas'].fillna(0).astype(int)

        top_results = DataFrame()
        for mpn_unico in df['mpn_ml'].drop_duplicates():
            if not isna(mpn_unico):
                top_item = df[df['mpn_ml'] == mpn_unico].sort_values(
                    ['lista_vendas', 'soma_fuzzy', 'lista_tag_mais_vendido', 'lista_tag_avaliacao'],
                    ascending=False
                ).iloc[0]
                top_results = concat([top_results, top_item.to_frame().T])

        return top_results

    def juntar_resultados(self, marca: str) -> DataFrame:
        """Consolida resultados dos arquivos em um único DataFrame."""
        all_files: list[str] = [f for f in listdir(
            f'./out/fuzzys/{marca.lower()}') if f.endswith('.feather')]

        # Verifica se há arquivos para concatenar
        if not all_files:
            rprint(f"[yellow]Aviso: Nenhum arquivo encontrado na pasta './out/fuzzys/{marca.lower()}'.[/yellow]")
            return DataFrame()  # Retorna um DataFrame vazio se não houver arquivos

        dataframes: list[DataFrame] = []
        for file in all_files:
            try:
                # Tenta carregar cada arquivo e adiciona à lista
                df: DataFrame = read_feather(
                    path.join('./out/fuzzys', marca.lower(), file)
                ).copy()
                dataframes.append(df)
            except Exception as e:
                rprint(f"Erro ao ler o arquivo '{file}': {e}")

        # Verifica se algum DataFrame foi carregado com sucesso
        if not dataframes:
            rprint(f"[yellow]Aviso: Nenhum DataFrame válido carregado para a marca '{marca}'.[/yellow]")
            return DataFrame()

        combined_df: DataFrame = concat(dataframes, ignore_index=True)
        return self.filter_top(combined_df)

if __name__ == "__main__":
    from re import sub
    PATH_HERE = path.dirname(__file__)

    if not path.exists(path.join(PATH_HERE, 'temp')):
        mkdir(path.join(PATH_HERE, 'temp'))

<<<<<<< HEAD
    MARCA = 'GAUSS'
    NOME_LOJA = 'gauss'
=======
    MARCA: str = 'VALEO' # De acordo com o nome do SIAC.
    NOME_LOJA: str = 'valeo'
>>>>>>> 20179b0ff526b292d6fdfe24d7ba9760a07772a4

    grupos = GetFuzzyGrupos(marca=MARCA)
    main = Main(nome_loja=NOME_LOJA)

<<<<<<< HEAD
    for grupo in tqdm(grupos.main()['grupo_subgrupo'], desc=f'Grupos da {MARCA}:', colour='yellow'):
        rprint(f'\n\t[yellow]{grupo}[/yellow]')
        grupo_normalizado = sub(r'\s', '+', grupo.lower())
=======
    df_grupos: DataFrame = grupos.main()
    df_grupos = df_grupos.copy()
>>>>>>> 20179b0ff526b292d6fdfe24d7ba9760a07772a4

        main.reset_df_infos()

        try:
            main.infos_lojas_oficiais(
                url=f'https://lista.mercadolivre.com.br/{grupo_normalizado.replace("+", "-")}_Loja_{NOME_LOJA}'
            )
            main.informacoes_anuncios_api()
            main.get_infos_fuzzy(linha_produto=grupo)
        except Exception as e:
            rprint(f"Erro no processamento do grupo {grupo}: {e}")

    (
        main.juntar_resultados(marca=MARCA)
        .to_excel(f'./total_produtos_{NOME_LOJA.lower()}.xlsx')
    )
    # main.juntar_resultados(marca=NOME_LOJA)
