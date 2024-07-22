from json import load

from ecomm import MLInterface
from httpx import Response
from pandas import read_excel, read_feather, merge, DataFrame
from rich import print as rprint
from clone import Clanador


def get_number_photo(lista_name_photo: str) -> list[str]:
    return [name.split('_')[-1:][0].replace('.jpg', '') for name in lista_name_photo]

if __name__ == '__main__':
    HEADERS: dict = MLInterface(1)._headers()
    CAMINHO_FOTOS: str = '../photo_validation/out_files_photos/%s'
    NOME_LOJA: str = 'sampel'

    with open('./data/sale_terms.json', 'r', encoding='utf-8') as fp:
        sale_terms = load(fp)

    df_photos: DataFrame = read_feather(
        '../data/conferencia_fotos_sampel.feather')
    df_photos = df_photos.query('~mlb.isna()').reset_index(drop=True).copy()
    df_photos['number_photo'] = get_number_photo(
        df_photos.path_file_photo.fillna('0'))

    df_clona_genuino = read_excel('../data/sampel.xlsx', dtype=str)
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

    clonagem: Clanador = Clanador(
        headers=HEADERS,
        sales_terms=sale_terms
    )

    clonagem.read_product_siac()

    for mlb in df_clona_copy.mlb.unique():
        row: DataFrame = df_clona_copy.query('mlb == @mlb')

        mlb_concorrente: str = row.loc[0, 'mlb']
        cod_ref_ecomm: str = row['sku_certo'].unique()[0]
        codpro: str = clonagem.convert_sku_to_codpro(sku=cod_ref_ecomm)[0]

        clonagem.df_siac_filter(
            codpro_produto=codpro
        )

        clonagem.mlb_infos(mlb_concorrente)

        clonagem.gerar_payload_cadastro(
            list_path_img=[
                CAMINHO_FOTOS % f'{NOME_LOJA}/{name_photo}' for name_photo in row['path_file_photo']],
            sku=cod_ref_ecomm
        )

        cadastro: Response = clonagem.cadastro()

        compati_clone: Response = clonagem.compatibilidades(
            item_id_ml_clone=mlb_concorrente,
            item_id_novo=cadastro.get('id'),
            view_compati=False
        )

        break

    rprint(corpo_cadastro)
