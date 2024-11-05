from typing import Union
from json import load

from ecomm import MLInterface
from rich import print as rprint
from pandas import read_csv
from tqdm import tqdm

from clonagem.clone import Clonador
from clonagem.comp_headler import CompatibilidadeHandler



# with open('./clonagem/data/sale_terms.json', 'r', encoding='utf-8') as fp:
#     sale_terms: dict = load(fp)

ml: MLInterface = MLInterface(1)

# clonador: Clonador = Clonador(
#     headers=ml.headers(),
#     sales_terms=sale_terms
# )


df  = read_csv('./clonagem/out/takao/df_clonados_takao.csv', dtype=str)


comp: CompatibilidadeHandler = CompatibilidadeHandler(
    base_url='https://api.mercadolibre.com/items',
    headers=ml.headers()
)

for item_id_old, item_id_new in tqdm(df[['item_id_genuino', 'item_id']].values, total=len(df)):
    res: Union[int, dict] = comp.compatibilidades(
        item_id_ml_clone=item_id_old,
        item_id_novo=item_id_new
    )
    if res == 0:
        rprint('\nNao tem compatibilidades')
    else:
        rprint(f'\n{res}')


# for item_id_old, item_id_new in tqdm(df[['item_id_genuino', 'item_id']].values, total=len(df)):
#     res = clonador.compatibilidades(
#         item_id_ml_clone=item_id_old,
#         item_id_novo=item_id_new
#     )
#     if res == 0:
#         rprint('\nNao tem compatibilidades')
#     else:
#         break
