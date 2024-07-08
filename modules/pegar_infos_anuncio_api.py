"""---"""
from os import system
from json import dumps

from ecomm import MLInterface
from httpx import Client, ReadTimeout
from pandas import read_feather, Series
from tqdm import tqdm

from modules.replace_char_spec import ReplaceCaract


class PegarInfosAnuncioApi:
    """Obejeto para requisitar atributos da API MercadoLivre.

    Returns:
        _type_: _description_
    """

    _atributos: dict[str, None] = {
        'gtin': None,
        'mpn': None,
        'sku': None,
        'oem': None,
        'marca': None,
        'numero_original': None
    }

    _base_url = "https://api.mercadolibre.com/items/%s/?include_attributes=all"

    def __init__(self, ml_interface: MLInterface) -> None:
        self._headers: dict[str, str] = ml_interface._headers()

    def pegar_infos_api(self, lista_mlbs: list[str]) -> list[dict]:
        """Metodo para requistar informacoes da API,
        passando como parametro o codigo MLB do anuncio.

        Args:
            lista_mlbs (list[str]): Lista de MLBs.

        Returns:
            list[dict]: Lista de reponse em JSON.
        """
        lista_res: list = []
        with Client() as client:
            for mlb in tqdm(lista_mlbs, desc='Pegando informacoes na API: '):
                try:
                    _res = client.get(url=self._base_url%mlb, headers=self._headers)
                    lista_res.append(dumps(_res.json()))
                except ReadTimeout:
                    pass
        return lista_res

    def pegar_mlb_url(self, lista_url: list[str]) -> list[str]:
        """Metodo para pegar o codigo MLB de uma URL. Faz um split com referencia do texto MLB.

        Args:
            lista_url (list[str]): Lista da URLs.

        Returns:
            list[str]: Lista de MLBs.
        """
        list_temp: list = []
        lista_url: list[str] = Series(lista_url).fillna('').to_list()
        for x in lista_url:
            try:
                mlb_id: str = x.split('MLB-')[1].split('-')[0]
                if mlb_id.isnumeric():
                    list_temp.append(f'MLB{mlb_id}')
            except IndexError:
                pass
        return list_temp


    def replace_caracteres(self, text: str) -> str:
        """Metodo para pegar um texto e tirar todos os caracteres especiais como acentos.

        Args:
            text (str): Texto com os caracteries especias.

        Returns:
            str: Texto limpo.
        """
        _repalce_text: ReplaceCaract = ReplaceCaract(text)
        _repalce_text.remover_acentos()
        _repalce_text.remover_caracteres_especiais()
        return _repalce_text.get_texto_limpo()

    def pegar_atributos_necessarios(self, _list_atributos: list[dict]):
        """Metodo para pegar as chaves necessarias do JSON de requisicoes do MLB.
        Ou seja, ele so pega o que a gente precisa.

        Args:
            _list_atributos (list[dict]): Lista de dicionarios, com as requisicoes da API.

        Returns:
            _type_: Atributos buscados.
        """
        try:
            for key in _list_atributos.get('attributes'):
                match key['id']:
                    case 'MPN':
                        self._atributos['mpn'] = self.replace_caracteres(key['value_name']).upper()
                    case 'OEM':
                        self._atributos['oem'] = self.replace_caracteres(key['value_name']).upper()
                    case 'GTIN':
                        self._atributos['gtin'] = self.replace_caracteres(key['value_name']).upper()
                    case 'PART_NUMBER':
                        self._atributos['numero_original'] = self.replace_caracteres(key['value_name']).upper()
                    case 'SELLER_SKU':
                        self._atributos['sku'] = self.replace_caracteres(key['value_name']).upper()
                    case 'BRAND':
                        self._atributos['marca'] = self.replace_caracteres(key['value_name']).upper()
                    case _:
                        ...
        except TypeError:
            return {
                'gtin': None,
                'mpn': None,
                'sku': None,
                'oem': None,
                'marca': None,
                'numero_original': None
            }
        return self._atributos

if __name__ == '__main__':
    ml = MLInterface(1)
    pegar_infos_mlb_api = PegarInfosAnuncioApi(ml_interface=ml)
    df = read_feather('../df_lojas_oficiais').head(1)
    df['lista_mlb'] = None
    df.loc[:, 'lista_mlb'] = df.loc[:, 'lista_url_anuncios'].apply(pegar_infos_mlb_api.pegar_mlb_url)

    df['lista_infos_mlb'] = None
    df['lista_att_necessarios'] = None

    for idx in tqdm(df.index, desc='Loja oficial: '):
        row = df.loc[idx].copy()
        try:
            if len(row['lista_mlb']) > 0:
                df.at[idx, 'lista_infos_mlb'] = pegar_infos_mlb_api.pegar_infos_api(row['lista_mlb'])
            system('cls')
        except Exception as e:
            print('Funcao 1')
            print(e)
            print(idx)
            df.loc[idx, 'lista_infos_mlb'] = None

    df.at[:, 'lista_mlb'] = df.loc[:, 'lista_url_anuncios'].apply(
        pegar_infos_mlb_api.pegar_mlb_url)

    for idx in df.index:
        row = df.loc[idx].copy()
        try:
            if not row['lista_infos_mlb'] is None:
                df.at[idx, 'lista_att_necessarios'] = [
                    pegar_atributos_necessarios(atts['attributes']) for atts in row['lista_infos_mlb']
                ]
        except Exception as e:
            print('Funcao 2')
            print(e)
            print(idx)
            df.loc[idx, 'lista_att_necessarios'] = None


    df.to_feather('df_infos_mlb')
