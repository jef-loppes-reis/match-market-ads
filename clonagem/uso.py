from json import load, dumps
from os import path, mkdir

from aplicacao import Aplicacao
from clone import Clonador
from ecomm import MLInterface
from httpx import Response
from rich import print as rprint
from pandas import (
    read_excel,
    read_feather,
    read_csv,
    merge,
    isna,
    DataFrame
)
from tqdm import tqdm


def get_number_photo(lista_name_photo: str) -> list[str]:
    return [name.split('_')[-1:][0].replace('.jpg', '') for name in lista_name_photo]


ML_INTERFACE: MLInterface = MLInterface(1)
HEADERS: dict = ML_INTERFACE._headers()
CAMINHO_FOTOS: str = '../photo_validation/out_files_photos/%s'
NOME_LOJA: str = 'sampel'
PATH_LOJA: str = f'./out/{NOME_LOJA}'

if not path.exists(PATH_LOJA):
    mkdir(PATH_LOJA)

with open('./data/sale_terms.json', 'r', encoding='utf-8') as fp:
    sale_terms = load(fp)

df_photos: DataFrame = read_feather(
    f'../data/conferencia_fotos_{NOME_LOJA}.feather')
df_photos = df_photos.query('~mlb.isna()').reset_index(drop=True).copy()
df_photos['number_photo'] = get_number_photo(
    df_photos.path_file_photo.fillna('0'))

df_clona_genuino = read_excel(f'../data/{NOME_LOJA}.xlsx', dtype=str)
df_clona_genuino = df_clona_genuino.set_axis(
    df_clona_genuino.columns.str.lower(), axis=1).copy()
df_clona_genuino.loc[:,
                        'sku_certo'] = df_clona_genuino.sku_certo.str.strip()

df_clona_copy: DataFrame = merge(
    df_clona_genuino,
    df_photos,
    on='mlb',
    how='left'
)

df_clona_copy = df_clona_copy.query(
    'clona == "1" and (verifeid_photo and pegar_foto)'
).reset_index(drop=True).copy()

df_clona_copy = df_clona_copy.sort_values(
    ['mlb', 'number_photo']).reset_index(drop=True)

clonagem: Clonador = Clonador(
    headers=HEADERS,
    sales_terms=sale_terms
)

clonagem.read_product_siac()

aplic: Aplicacao = Aplicacao()
aplic.read_apl()
aplic.criar_ano_inicial_ano_final()

df_clonados: DataFrame = DataFrame(
    columns=[
        'item_id_genuino',
        'item_id',
        'corpo_json',
        'compatibilidades',
        'descricao',
    ]
)

try:
    df_ultima_clonagem: DataFrame = read_csv(f'./out/{NOME_LOJA}/df_clonados_{NOME_LOJA}.csv')
    df_clona_copy = df_clona_copy[~df_clona_copy['mlb'].isin(df_ultima_clonagem['item_id_genuino'])]
except FileNotFoundError:
    ...

for mlb in tqdm(df_clona_copy.mlb.unique(), desc='Anunciando...:', colour='blue'):
    row: DataFrame = df_clona_copy.query('mlb == @mlb').reset_index(drop=True).copy()
    sku_certo = row['sku_certo'].unique()[0]
    if isna(sku_certo):
        continue
    sku: str = '' if isna(sku_certo) else sku_certo
    codpro: str = clonagem.convert_sku_to_codpro(sku=sku)[0]

    try:
        quebra_sku: list[str] = row.loc[0, 'sku_certo'].split('_')[1:]
    except AttributeError:
        quebra_sku = []

    if len(quebra_sku) > 1:
        kit: tuple[bool, int] = (True, len(quebra_sku))
    else:
        kit: tuple[bool, int] = (False, 0)

    try:
        clonagem.read_df_siac_filter(
            codpro_produto=codpro.strip()
        )
        clonagem.df_siac_filter.loc[0]
    except KeyError:
        rprint(f'O MLB {mlb} selecionado, nao tem dados com esse codpro {codpro}, estou pulando ele.')
        continue

    mlb_info: tuple[Response, Response] = clonagem.mlb_infos(
        item_id_ml=row.loc[0, 'mlb']
    )

    clonagem.gerar_payload_cadastro(
        list_path_img=[
            CAMINHO_FOTOS % f'{NOME_LOJA}/{name_photo}' for name_photo in row['path_file_photo']],
        sku=sku
    )

    clonagem.corpo_clonagem.update({
        'description': aplic.criar_descricao(
            codpro=codpro,
            aplicacao_veiculos=aplic.get_aplicacao(
                original=clonagem.df_siac_filter.loc[0, 'num_orig']
            ),
            marca=clonagem.df_siac_filter.loc[0, 'marca'],
            multiplo_venda=clonagem.df_siac_filter.loc[0, 'embala'],
            kit=kit[0],
            num_fab=sku,
            oems=aplic.lista_originais(
                clonagem.df_siac_filter.loc[0, 'num_orig']
            ),
            titulo=clonagem.corpo_clonagem.get('title')
        )
    })

    # retorno_cadastro: dict = clonagem.cadastro()
    if clonagem.corpo_clonagem.get('pictures') is None:
        rprint(f'O MLB {mlb} selecionado nao tem foto aprovado, estou passando ele.')
        continue

    try:
        retorno_cadastro: dict = ML_INTERFACE.post_item(
            item=clonagem.corpo_clonagem
        )
    except Exception as e:
        rprint(e)
        rprint(clonagem.corpo_clonagem)
        continue

    compati: int = clonagem.compatibilidades(
        item_id_ml_clone=mlb,
        item_id_novo=retorno_cadastro.get('id'),
        view_compati=True
    )

    retorno_descricao: Response = clonagem.descricao(
        item_id_novo=retorno_cadastro.get('id'),
        descricao=clonagem.corpo_clonagem.get('description')
    )

    df_clonados.loc[len(df_clonados)] = {
        'item_id_genuino': mlb,
        'item_id': retorno_cadastro.get('id'),
        'corpo_json': dumps(retorno_cadastro, ensure_ascii=False),
        'compatibilidades': compati,
        'descricao': retorno_descricao.status_code
    }

    df_clonados.to_csv(f'./out/{NOME_LOJA}/df_clonados_{NOME_LOJA}.csv', index=False)
