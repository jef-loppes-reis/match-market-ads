from json import loads

from pandas import DataFrame
from ecomm import Postgres
from fuzzywuzzy import fuzz
from tqdm import tqdm


class SiacFuzzy:

    _score_defualt = 59

    with open('../data/sql/produto_siac.sql', 'r', encoding='utf-8') as fp:
        with Postgres() as db:
            _df_siac: DataFrame = db.query(fp.read())

    def __init__(self, df_infos_mlb: DataFrame, columns: dict) -> None:
        self._df_infos_mlb: DataFrame = df_infos_mlb
        self._padrao_colunas: dict = columns

    def created_new_dataframe(self) -> DataFrame:
        _df = DataFrame(columns=self._padrao_colunas.keys())
        id_att: dict = {}

        list_atts = [loads(atts) for atts in self._df_infos_mlb.loc[
            :, 'lista_att_necessarios']]

        _df['lista_url_anuncios'] = self._df_infos_mlb.lista_url_anuncios

        _df.loc[:, 'mlb'] = self._df_infos_mlb.lista_mlbs

        _df.loc[:, 'gtin_ml'] = [id_att.get('gtin') for id_att in list_atts]

        _df.loc[:, 'sku_ml'] = [id_att.get('sku') for id_att in list_atts]

        _df.loc[:, 'marca_ml'] = [id_att.get('marca') for id_att in list_atts]

        _df.loc[:, 'mpn_ml'] = [id_att.get('mpn') for id_att in list_atts]

        _df.loc[:, 'numero_original_ml'] = [id_att.get(
            'numero_original') for id_att in list_atts]

        _df.loc[:, 'mpn_marca_ml'] = [f'{id_att.get('mpn')}_{id_att.get(
            'marca')}' for id_att in list_atts]

        return _df

    def fuzzy_results(
            self,
            gtin: str,
            mpn: str,
            sku: str,
            numero_original: str,
            marca: str,
            mpn_marca: str,
            df_index: int
        ) -> dict:

        similarity_score_gtin_before = 0
        similarity_score_mpn_before = 0
        similarity_score_sku_before = 0
        similarity_score_num_orig_before = 0
        similarity_score_marca_before = 0
        similarity_score_mpn_marca_before = 0

        _gtin = None
        _mpn = None
        _sku = None
        _num_orig = None
        _marca = None
        _mpn_marca = None

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

        sum_sores = sum([similarity_score_gtin_before,
                            similarity_score_mpn_before,
                            similarity_score_sku_before,
                            similarity_score_num_orig_before,
                            similarity_score_marca_before,
                            similarity_score_mpn_before])

        return {
            'gtin_siac': _gtin,
            'score_gtin_before': similarity_score_gtin_before,
            'mpn_siac': _mpn,
            'score_mpn_before': similarity_score_mpn_before,
            'sku_siac': _sku,
            'score_sku_before': similarity_score_sku_before,
            'num_orig_siac': _num_orig,
            'score_num_orig_before': similarity_score_num_orig_before,
            'marca_siac': _marca,
            'score_marca_before': similarity_score_marca_before,
            'mpn_marca_siac': _mpn_marca,
            'score_mpn_marca_before': similarity_score_mpn_marca_after,
            'sum_scores': sum_sores,
            'df_index': df_index
        }
