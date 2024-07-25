from json import load

from aplicacao import Aplicacao
from clone import Clanador
from ecomm import MLInterface
from httpx import Response
from rich import print as rprint
from pandas import read_excel, read_feather, merge, DataFrame
from tqdm import tqdm


def get_number_photo(lista_name_photo: str) -> list[str]:
    return [name.split('_')[-1:][0].replace('.jpg', '') for name in lista_name_photo]

if __name__ == '__main__':
    ML_INTERFACE: MLInterface = MLInterface(1)
    HEADERS: dict = ML_INTERFACE._headers()
    CAMINHO_FOTOS: str = '../photo_validation/out_files_photos/%s'
    NOME_LOJA: str = 'monroe'

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

    clonagem: Clanador = Clanador(
        headers=HEADERS,
        sales_terms=sale_terms
    )

    clonagem.read_product_siac()

    # aplic: Aplicacao = Aplicacao()
    # aplic.read_apl()
    # aplic.criar_ano_inicial_ano_final()
    # aplic.etl_tabela_aplic()

    for mlb in tqdm(df_clona_copy.mlb.unique(), desc='Anunciando...:', colour='blue'):
        row: DataFrame = df_clona_copy.query('mlb == @mlb').reset_index(drop=True).copy()
        sku: str = row['sku_certo'].unique()[0]
        codpro: str = clonagem.convert_sku_to_codpro(
            sku=sku
        )[0]
        # descricao: str = aplic.criar_descricao(
        #     titulo='',
        #     aplicacao_veiculos='',
        #     marca=row.loc[0, 'marca'],
        #     num_fab=row.loc[0, 'mpn'],
        #     oems=row.loc[0, 'oem'],
        #     multiplo_venda=1,
        #     codpro=codpro
        # )
        print(row)

        clonagem.df_siac_filter(
            codpro_produto=codpro
        )

        mlb_info: tuple[Response, Response] = clonagem.mlb_infos(
            item_id_ml=row.loc[0, 'mlb']
        )

        clonagem.gerar_payload_cadastro(
            list_path_img=[
                CAMINHO_FOTOS % f'monroe/{name_photo}' for name_photo in row['path_file_photo']],
            sku=sku
        )

        retorno_cadastro: str = ML_INTERFACE.post_item(clonagem.corpo_clonagem)

        clonagem.compatibilidades()

        rprint(clonagem.corpo_clonagem)
        rprint(retorno_cadastro)
        break
