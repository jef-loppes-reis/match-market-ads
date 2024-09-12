from json import loads

from pandas import DataFrame
from ecomm import Postgres
from fuzzywuzzy import fuzz

class SiacFuzzy:

    _score_defualt = 59
    _df_siac: DataFrame = DataFrame

    def __init__(self, df_infos_mlb: DataFrame, columns: dict, 
                 path_data: str) -> None:
        self._df_infos_mlb: DataFrame = df_infos_mlb
        self._padrao_colunas: dict = columns
        self._path_data: str = path_data

    def read_db(self):
        with open(self._path_data, 'r', encoding='utf-8') as fp:
            with Postgres() as db:
                self._df_siac: DataFrame = db.query(fp.read())

    def created_new_dataframe(self) -> DataFrame:
        _df = DataFrame(columns=self._padrao_colunas.keys())
        id_att: dict = {}

        list_atts = [loads(atts) for atts in self._df_infos_mlb.loc[
            :, 'lista_att_necessarios']]

        _df['lista_url_anuncios'] = self._df_infos_mlb.lista_url_anuncios.copy()

        _df.loc[:, 'mlb'] = self._df_infos_mlb.lista_mlbs.copy()

        _df.loc[:, 'gtin_ml'] = [id_att.get('gtin') for id_att in list_atts]

        _df.loc[:, 'sku_ml'] = [id_att.get('sku') for id_att in list_atts]

        _df.loc[:, 'marca_ml'] = [id_att.get('marca') for id_att in list_atts]

        _df.loc[:, 'mpn_ml'] = [id_att.get('mpn') for id_att in list_atts]

        _df.loc[:, 'numero_original_ml'] = [id_att.get(
            'numero_original') for id_att in list_atts]

        _df.loc[:, 'mpn_marca_ml'] = [f'{id_att.get('mpn')}_{id_att.get(
            'marca')}' for id_att in list_atts]

        _df.loc[:, 'produto_ml'] = [id_att.get('produto') for id_att in list_atts]

        return _df

    def fuzzy_results(
            self,
            gtin: str,
            mpn: str,
            sku: str,
            numero_original: str,
            marca: str,
            mpn_marca: str,
            produto: str,
            df_index: int
        ) -> dict:

        _qtd_itens: int = 7

        similarity_score_gtin_before = 0
        similarity_score_mpn_before = 0
        similarity_score_sku_before = 0
        similarity_score_num_orig_before = 0
        similarity_score_marca_before = 0
        similarity_score_mpn_marca_before = 0
        similarity_score_produto_before = 0

        _gtin = None
        _mpn = None
        _sku = None
        _num_orig = None
        _marca = None
        _mpn_marca = None
        _produto = None

        for idx in self._df_siac.index:
            _row_siac: DataFrame = self._df_siac.loc[idx].copy()

            similarity_score_gtin_after: int = fuzz.ratio(
                gtin, _row_siac['codigo_barras']
            )
            similarity_score_mpn_after: int = fuzz.ratio(
                mpn, _row_siac['mpn']
            )
            similarity_score_sku_after: int = fuzz.ratio(
                sku, _row_siac['mpn']
            )
            similarity_score_num_orig_after: int = fuzz.ratio(
                numero_original, _row_siac['oem']
            )
            similarity_score_marca_after: int = fuzz.ratio(
                marca, _row_siac['marca']
            )
            similarity_score_mpn_marca_after: int = fuzz.ratio(
                mpn_marca, f'{_row_siac['mpn']}_{_row_siac['marca']}'
            )
            similarity_score_produto_after: int = fuzz.ratio(
                produto, _row_siac['produto']
            )

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

            if similarity_score_mpn_marca_before < similarity_score_mpn_marca_after:
                similarity_score_mpn_marca_before = similarity_score_mpn_marca_after
                _mpn_marca = f'{_row_siac['mpn']}_{_row_siac['marca']}'

            if similarity_score_produto_before < similarity_score_produto_after:
                similarity_score_produto_before = similarity_score_produto_after
                _produto = f'{_row_siac['produto']}'

        sum_sores = sum([similarity_score_gtin_before,
                            similarity_score_mpn_before,
                            similarity_score_sku_before,
                            similarity_score_num_orig_before,
                            similarity_score_marca_before,
                            similarity_score_mpn_before,
                            similarity_score_produto_before])

        return {
            'gtin_siac': _gtin,
            'score_gtin_before': f'{(similarity_score_gtin_before/100):.2%}',
            'mpn_siac': _mpn,
            'score_mpn_before': f'{(similarity_score_mpn_before/100):.2%}',
            'sku_siac': _sku,
            'score_sku_before': f'{(similarity_score_sku_before/100):.2%}',
            'num_orig_siac': _num_orig,
            'score_num_orig_before': f'{(similarity_score_num_orig_before/100):.2%}',
            'marca_siac': _marca,
            'score_marca_before': f'{(similarity_score_marca_before/100):.2%}',
            'mpn_marca_siac': _mpn_marca,
            'score_mpn_marca_before': f'{(similarity_score_mpn_marca_before/100):.2%}',
            'produto_siac': _produto,
            'score_produto_before': f'{(similarity_score_produto_before/100):.2%}',
            'sum_scores': f'{(sum_sores/_qtd_itens)/100:.2%}',
            'df_index': df_index
        }
