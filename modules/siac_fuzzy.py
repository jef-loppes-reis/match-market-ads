from json import loads

from pandas import DataFrame
from ecomm import Postgres
from fuzzywuzzy import fuzz
from tqdm import tqdm


class SiacFuzzy:

    _score_defualt = 59

    with Postgres() as db:
        _df_siac: DataFrame = db.query('''
                SELECT produto.produto as titulo,
                    produto.codpro as codigo_interno,
                    produto.codpro as sku,
                    produto.num_fab as mpn,
                    produto.fantasia as marca,
                    prd_gtin.cd_barras as codigo_barras,
                    original.num_orig as oem
                FROM "D-1".produto
                LEFT JOIN "D-1".prd_gtin ON produto.codpro = prd_gtin.cd_produto
                LEFT JOIN "D-1".original ON produto.num_orig = original.nu_origina
                ORDER BY dt_cadast DESC;
                ''')

    def __init__(self, df_infos_mlb: DataFrame, columns: dict) -> None:
        self._df_infos_mlb: DataFrame = df_infos_mlb
        self._padrao_colunas: dict = columns

    def created_new_dataframe(self) -> DataFrame:
        _df = DataFrame(columns=self._padrao_colunas.keys())

        # from rich import print as pprint
        # pprint(self._df_infos_mlb.lista_mlbs.values[0])
        # pprint(self._df_infos_mlb.lista_url_anuncios.values[0])

        list_atts = [loads(atts) for atts in self._df_infos_mlb.loc[0, 'lista_att_necessarios']]

        _df['lista_url_anuncios'] = self._df_infos_mlb.lista_url_anuncios.values[0]

        _df.loc[:, 'mlb'] = self._df_infos_mlb.lista_mlbs.values[0]

        _df.loc[:, 'gtin_ml'] = [id_att['gtin'] for id_att in list_atts]

        _df.loc[:, 'sku_ml'] = [id_att['sku'] for id_att in list_atts]

        _df.loc[:, 'marca_ml'] = [id_att['marca'] for id_att in list_atts]

        _df.loc[:, 'mpn_ml'] = [id_att['mpn'] for id_att in list_atts]

        _df.loc[:, 'numero_original_ml'] = [id_att['numero_original'] for id_att in list_atts]

        # _df.loc[:, 'oem_ml'] = [id_att['oem'] for id_att in list_atts]

        return _df

    def fuzzy_results(self,
                      gtin: str,
                      mpn: str,
                      sku: str,
                    numero_original: str,
                      marca: str,
                      df_index: int
                      ) -> tuple[list[str | int]]:

        similarity_score_gtin_before = 0
        similarity_score_mpn_before = 0
        similarity_score_sku_before = 0
        similarity_score_num_orig_before = 0
        similarity_score_marca_before = 0

        _gtin = None
        _mpn = None
        _sku = None
        _num_orig = None
        _marca = None

        for idx in self._df_siac.index:
            _row_siac: DataFrame = self._df_siac.loc[idx].copy()

            similarity_score_gtin_after: int = fuzz.ratio(
                gtin, _row_siac['codigo_barras'])
            similarity_score_mpn_after: int = fuzz.ratio(mpn, _row_siac['mpn'])
            similarity_score_sku_after: int = fuzz.ratio(sku, _row_siac['mpn'])
            similarity_score_num_orig_after: int = fuzz.ratio(
                numero_original, _row_siac['oem'])
            similarity_score_marca_after: int = fuzz.ratio(
                marca, _row_siac['marca'])

            if similarity_score_gtin_before < similarity_score_gtin_after:
                similarity_score_gtin_before = similarity_score_gtin_after
                _gtin = _row_siac['codigo_barras']

            if similarity_score_mpn_before < similarity_score_mpn_after:
                similarity_score_mpn_before = similarity_score_mpn_after
                _mpn = _row_siac['mpn']

            if similarity_score_sku_before < similarity_score_sku_after:
                similarity_score_sku_before = similarity_score_sku_after
                _sku = _row_siac['mpn']

            if similarity_score_num_orig_before < similarity_score_num_orig_after:
                similarity_score_num_orig_before = similarity_score_num_orig_after
                _num_orig = _row_siac['oem']

            if similarity_score_marca_before < similarity_score_marca_after:
                similarity_score_marca_before = similarity_score_marca_after
                _marca = _row_siac['marca']

            sum_sores = sum([similarity_score_gtin_before,
                                similarity_score_mpn_before,
                                similarity_score_sku_before,
                                similarity_score_num_orig_before,
                                similarity_score_marca_before])

        return (
            [_gtin, similarity_score_gtin_before],
            [_mpn, similarity_score_mpn_before],
            [_sku, similarity_score_sku_before],
            [_num_orig, similarity_score_num_orig_before],
            [_marca, similarity_score_marca_before],
            sum_sores,
            df_index
        )


if __name__ == '__main__':
    from json import load
    from concurrent.futures import ProcessPoolExecutor

    from pandas import read_feather
    from tqdm import tqdm

    df_infos_mlb = read_feather('../df_infos_mlb')

    with open('../data/colunas_data_frame_final.json', 'r', encoding='utf-8') as fp:
        padrao_colunas = load(fp)

    siac_fuzzy = SiacFuzzy(df_infos_mlb, padrao_colunas)

    df = siac_fuzzy.created_new_dataframe()

    with ProcessPoolExecutor() as executor:
        try:
            for future in tqdm(executor.map(
                siac_fuzzy.fuzzy_results,
                df['gtin'],
                df['mpn'],
                df['sku'],
                df['numero_original'],
                df['marca'],
                df.index
            ), total=len(df)):
                df.at[future[5], 'gtin_siac'] = future[0][0]
                df.at[future[5], 'gtin_fuzzy'] = future[0][1]

                df.at[future[5], 'mpn_siac'] = future[1][0]
                df.at[future[5], 'mpn_fuzzy'] = future[1][1]

                df.at[future[5], 'sku_siac'] = future[2][0]
                df.at[future[5], 'sku_fuzzy'] = future[2][1]

                df.at[future[5], 'numero_original_siac'] = future[3][0]
                df.at[future[5], 'numero_original_fuzzy'] = future[3][1]

                df.at[future[5], 'marca_siac'] = future[4][0]
                df.at[future[5], 'marca_fuzzy'] = future[4][1]
        except KeyError as e:
            print('Erro!')
            print(e)
        finally:
            df.to_feather('df_resultado_fuzzy')
            df.to_excel('df_resultado_fuzzy.xlsx')
