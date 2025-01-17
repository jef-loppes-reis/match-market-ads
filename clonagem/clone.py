"""---"""
from json import dumps
from re import sub
from time import sleep

from ecomm import Postgres
from httpx import Client, Response, ReadTimeout, ReadError
from rich import print as rprint
from pandas import DataFrame, isna


class Clonador:

    _item_id_payload: dict[str, str | float] = {}
    _base_url: str = 'https://api.mercadolibre.com/items'
    _base_url_upload_photos: str = 'https://api.mercadolibre.com/pictures/items/upload'
    corpo_clonagem: dict[str, str | float] = {
        "title": None,
        "category_id": None,
        "official_store_id": None,
        "price": None,
        "available_quantity": None,
        "currency_id": "BRL",
        "buying_mode": "buy_it_now",
        "condition": "new",
        "listing_type_id": None,
        "sale_terms": None,
        "pictures": None,
        "attributes": None,
        "variations": None,
        "video_id": "e4lB8zcfDt8"
    }
    _res_mlb: dict = {}
    _res_descp: dict = {}
    df_siac: DataFrame = DataFrame()
    df_siac_filter: DataFrame = DataFrame()
    _corpo_compatibilidades_default: dict[str, str] = {
        'id': None,       # catalog_product_id
        'note': None,     # catalog_product_name
    }
    __num_tentativas: int = 0

    def __init__(self, headers: dict, sales_terms: dict):
        self._headers: dict = headers
        self._sales_terms = sales_terms

    def convert_sku_to_codpro(self, sku: str) -> None | list[str]:
        """_summary_

        Args:
            sku (str): _description_

        Returns:
            str: _description_
        """
        if isna(sku):
            return None
        list_sku: list[str] = sku.split('_')
        if len(list_sku) > 1:
            list_sku: list[str] = list_sku[1:]

        list_codpro: list[str] = []

        for sku_in_list in list_sku:
            lista_codpro: list[str] = sku_in_list.split('X')
            if len(lista_codpro) > 1:
                # n = int(codpro[0])
                codpro: str = lista_codpro[1][1:-1]
                list_codpro.append(codpro)
            else:
                # n = 1
                codpro: str = lista_codpro[0]
                list_codpro.append(codpro)
        return list_codpro

    def mlb_infos(self, item_id_ml: str) -> tuple[Response, Response]:
        with Client(base_url=self._base_url) as client:

            for __attempts_mlb in range(10):
                self._res_mlb: Response = client.get(
                    url=f'/{item_id_ml}?include_attributes=all',
                    headers=self._headers,
                    timeout=None
                )
                if self._res_mlb.is_success:
                    __flag_mlb: bool = True
                if self._res_mlb.is_client_error:
                    if self._res_mlb.status_code == 429:
                        sleep(10)
                        continue
                    raise ValueError(f'Erro de requisicao do MLB {item_id_ml}')
                if self._res_mlb.is_server_error:
                    sleep(20)
                    continue

            for __attempts_desc in range(10):
                self._res_descp: Response = client.get(
                    url=f'/{item_id_ml}/description',
                    headers=self._headers,
                    timeout=None
                )

                if self._res_descp.is_success:
                    __flag_desc: bool = True
                if self._res_descp.is_client_error:
                    if self._res_descp.status_code == 429:
                        sleep(10)
                        continue
                    raise ValueError(f'Erro de requisicao do MLB {item_id_ml}')
                if self._res_descp.is_server_error:
                    sleep(20)
                    continue

            if self._res_mlb.is_success and self._res_descp.is_success:
                rprint(self._res_mlb.json())
                rprint(self._res_descp.json())
                return (self._res_mlb, self._res_descp)
            raise ValueError(f'Numero de tentativas exedido: [MLB]: {self._res_mlb.status_code}, [DESC]: {self._res_descp.status_code}')

    def __read_product_siac(self) -> DataFrame:
        with Postgres() as db:
            with open('./data/d_1_produto.sql', 'r', encoding='utf-8') as fp:
                self.df_siac = db.query(fp.read()).copy()

    def read_df_siac_filter(self, codpro_produto: str):
        codpro_produto: str = codpro_produto[0] if isinstance(codpro_produto, list) else codpro_produto
        self.df_siac_filter: DataFrame = (self.df_siac
            .query(f'codpro == "{codpro_produto}"')
            .reset_index(drop=True).copy()
        )

    def lista_attributos(self, sku: str) -> list[dict[str, str | float]]:
        _lista_att: list[dict[str, str | float]] = []
        _lista_att_incomplete: list = [
            'SELLER_SKU',
            'GTIN'
        ]
        try:
            for att in self._res_mlb.json().get('attributes'):
                match att.get('id'):
                    case 'BRAND':
                        _lista_att.append(
                            {
                                'id': 'BRAND',
                                'name': 'Marca',
                                'value_name': self.df_siac_filter.loc[0, 'marca']
                            }
                        )
                    case 'PART_NUMBER':
                        _lista_att.append(
                            {
                                'id': 'PART_NUMBER',
                                'name': 'Número de peça',
                                'value_name': self.df_siac_filter.loc[0, 'num_fab']
                            }
                        )
                    case 'SELLER_SKU':
                        _lista_att.append(
                            {
                                'id': 'SELLER_SKU',
                                'name': 'SKU',
                                # ** Adicionar o SKU_REF_ECOMM, criar o metodo para converter o SKU_SIAC.
                                'value_name': sku
                            }
                        )
                    case 'OEM':
                        _lista_att.append(
                            {
                                'id': 'OEM',
                                'name': 'Código OEM',
                                'value_name': sub(
                                    pattern=r"[[\]']",
                                    repl='',
                                    string=str(self.df_siac_filter.loc[:,
                                        'lista_oem'].to_list())
                                )
                            }
                        )
                    case 'MPN':
                        _lista_att.append(
                            {
                                'id': 'MPN',
                                'name': 'MPN',
                                'value_name': self.df_siac_filter.loc[0, 'num_fab']
                            }
                        )
                    case 'GTIN':
                        _lista_att.append(
                            {
                                'id': 'GTIN',
                                'name': 'Código universal de produto',
                                'value_name': self.df_siac_filter.loc[0, 'gtin']
                            }
                        )
                    case _:
                        _lista_att.append(att)

            # Iteracao para conferecencias de alguns att obrigatorios
            for att_incomplete in _lista_att_incomplete:
                match att_incomplete:
                    case 'SELLER_SKU':
                        _lista_att.append(
                            {
                                'id': 'SELLER_SKU',
                                'name': 'SKU',
                                'value_name': sku
                            }
                        )
                    case 'GTIN':
                        _lista_att.append(
                            {
                                'id': 'GTIN',
                                'name': 'Código universal de produto',
                                "value_name": self.df_siac_filter.loc[0, 'gtin'],
                            }
                        )

        except AttributeError as e:
            rprint(
                '''
                [bright_yellow]Ops, você não rodou o método [/bright_yellow]"mlb_infos".
                [bright_yellow]Preciso das informações do MLB para continuar![/bright_yellow]
                ''')
            raise ValueError(
                'Variável "_res_mlb: Response" da classe (Clanador), esta vazia') from e
        return _lista_att

    def lista_id_photos(self, list_path_img: list[str]) -> list[str]:
        lista_id_imgs: list[str] = []
        try:
            with Client() as client:
                for img in list_path_img:
                    try:
                        with open(img, 'rb') as photo_file:
                            _res_photo: Response = client.post(
                                url=self._base_url_upload_photos,
                                headers=self._headers,
                                files={
                                    'file': photo_file
                                }
                            )
                            if _res_photo.json().get('id') is None:
                                continue
                            lista_id_imgs.append(
                                {'id': _res_photo.json().get('id')}
                            )
                    except FileNotFoundError:
                        rprint(f'Foto nao encontrada [{img}]')
                        continue

            return lista_id_imgs
        except FileNotFoundError as e:
            raise e
        except Exception as e:
            rprint(e)

    def gerar_payload_cadastro(self, list_path_img: list[str], sku: str, mlb: str):
        self.mlb_infos(mlb)
        for key, _ in self.corpo_clonagem.items():
            match key:
                case 'title':
                    self.corpo_clonagem.update(
                        {key: self._res_mlb.json().get(key)}
                    )
                case 'category_id':
                    self.corpo_clonagem.update(
                        {key: self._res_mlb.json().get(key)}
                    )
                case 'official_store_id':
                    self.corpo_clonagem.update(
                        {key: 5329}
                    )
                case 'available_quantity':
                    __estoque: int | None = self.df_siac_filter.loc[0, 'estoque']
                    self.corpo_clonagem.update(
                        {key: 0 if __estoque is None else int(__estoque)}
                    )
                case 'price':
                    self.corpo_clonagem.update(
                        {key: float(self.df_siac_filter.loc[0, 'p_venda'])}
                    )
                case 'listing_type_id':
                    self.corpo_clonagem.update(
                        {key: self._res_mlb.json().get(key)}
                    )
                case 'sale_terms':
                    self.corpo_clonagem.update(
                        {key: self._sales_terms}
                    )
                case 'pictures':
                    # ** Pretendo criar uma funcao para manipulas as fotos definidas para cadastro
                    # ** Subir a foto para o MercadoLivre, retornar o ID da foto e criar uma lista de IDs.
                    self.corpo_clonagem.update(
                        {key: self.lista_id_photos(list_path_img)}
                    )
                case 'variations':
                    # ** Criar um jeito que pegue as variacoes do concorrente.
                    self.corpo_clonagem.update(
                        {key: []}
                    )
                case 'attributes':
                    # ** Criar um metodo para manipular os atributos do concorrente, nos seguintes pontos:
                    # ** Quando houver SELLER_SKU, substituir por SKU_REF_ECOMM: str.
                    # ** Quando houver GTIN, substituir por GTIN_SIAC: str.
                    # ** Quando houver MPN, substituir por NUM_FAB_SIAC: str.
                    # ** Quando houver OEM, substituir pela lista de OEM_SIAC: list[str].
                    # ** Quando houver PART_NUMBER, subistituir por NUM_FAB_SIAC: str
                    # ** Quando houver BRAND, subisituir por FANTASIA_MARCA_SIAC: str.
                    self.corpo_clonagem.update(
                        {key: self.lista_attributos(sku)}
                    )

    def post_descricao(self, item_id_novo: str, descricao: str) -> Response:
        with Client(base_url=self._base_url) as client:
            while True:
                if self.__num_tentativas < 16:
                    _res_descricao: Response = client.post(
                        url=f'/{item_id_novo}/description',
                        headers=self._headers,
                        data=dumps({'plain_text': descricao})
                    )
                    if _res_descricao.status_code in [429, 500]:
                        sleep(1)
                        self.__num_tentativas+=1
                        continue
                    self.__num_tentativas = 0
                    return _res_descricao

    def post_cadastro(self) -> dict:
        with Client() as client:
            while True:
                if self.__num_tentativas < 16:
                    try:
                        # _copia_corpo_clonagem: dict = self.corpo_clonagem
                        # _copia_corpo_clonagem.pop('description')
                        with open('teste.json', 'w', encoding='utf-8') as fp:
                            fp.write(dumps(self.corpo_clonagem, ensure_ascii=False))
                        rprint(self.corpo_clonagem)
                        _res_cadastro: Response = client.post(
                            url='https://api.mercadolibre.com/items',
                            headers=self._headers,
                            json=dumps(self.corpo_clonagem)
                        )
                        if _res_cadastro.status_code in [429, 500]:
                            self.__num_tentativas += 1
                            sleep(5)
                            continue
                        self.__num_tentativas = 0
                        return _res_cadastro.json()
                    except ReadError:
                        self.__num_tentativas += 1
                        sleep(30)
                        continue
                    except ReadTimeout:
                        self.__num_tentativas += 1
                        sleep(30)
                        continue
                return {'id': None}


    def main(self):
        self.__read_product_siac()
