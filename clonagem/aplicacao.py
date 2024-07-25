from re import sub

from ecomm import Postgres
from pandas import DataFrame, isna
from numpy import nan
from tqdm import tqdm


class Aplicacao:

    _df_aplic: DataFrame = DataFrame()
    _df_aplic_orig: DataFrame = DataFrame(columns=['num_orig', 'aplic'])

    def __init__(self) -> None:
        pass

    def read_apl(self):
        with Postgres() as db:
            with open('./data/apl.sql', 'r', encoding='utf-8') as fp:
                self._df_aplic: DataFrame = db.query(fp.read())
                self._df_aplic['ano'] = None


    def criar_ano_inicial_ano_final(self):
        lista_temp = []
        for idx in tqdm(self._df_aplic.index):
            row = self._df_aplic.loc[idx]
            ano_i = row['ano_i']
            ano_f = row['ano_f']
            if not isna(ano_f) and isna(ano_i):
                lista_temp.append(f'até {ano_f}')
            elif not isna(ano_i) and isna(ano_f):
                lista_temp.append(f'de {ano_i} em diante')
            elif not isna(ano_i) and not isna(ano_f):
                lista_temp.append(f'{ano_i} até {ano_f}')
            elif isna(ano_i) and isna(ano_f):
                lista_temp.append('(Confira o ano de veículo no campo de perguntas.)')
            else:
                lista_temp.append(nan)
        self._df_aplic.loc[:, 'ano'] = lista_temp


    def etl_tabela_aplic(self):
        df_aplic_copy: DataFrame = self._df_aplic
        for cod_orig in tqdm(df_aplic_copy.num_orig.unique()):
            df_temp = df_aplic_copy.query('num_orig == @cod_orig').reset_index(drop=True).copy()
            str_aplic = '• '
            for idx in df_temp.index:
                row = df_temp.iloc[idx, 1:]
                for aplic in row:
                    if not isna(aplic):
                        str_aplic += f' {str(aplic)}'
                str_aplic += '\n• '
            self._df_aplic_orig.loc[len(self._df_aplic_orig)] = {'num_orig': cod_orig, 'aplic': str_aplic}
        # return self._df_aplic_orig


    def cria_lista_de_numeros_originais(self, oem: str) -> list:
        lista_temp: list[str] = []

        lista_oem = self._df_aplic_orig.query('nu_orig == @oem').num_orig.values
        str_temp = ''
        cont = 0
        for x in lista_oem:
            if cont > 0:
                str_temp += ', '
            str_temp += sub(r'\s','',x)
            cont += 1
        # list(set(str_temp.split(', ')))
        # lista_temp.append(str_temp)
        lista_temp.append(list(set(str_temp.split(', '))))
        return lista_temp


#     def criar_descricao(
#             self,
#             codpro: str
#         ) -> str:
#         _aplicacao_veiculos: str = 
#         veiculos = f'\nSERVE NOS SEGUINTES VEÍCULOS:\n(Em caso de dúvidas, perguntar no campo de perguntas.)\n\n{aplicacao_veiculos}\n\n' \
#             if not isna(aplicacao_veiculos) else '\n(Em caso de dúvidas, perguntar no campo de perguntas.)\n\n'
#         descricao = \
# f'''{titulo}
# {veiculos}
# CARACTERÍSTICAS DO PRODUTO:
# • Marca: {marca}
# • Código da peça: {num_fab}
# • Código de referência: {', '.join(oems.split(','))}

# CONTEÚDO DA EMBALAGEM:
# • {multiplo_venda} {'Unidade' if multiplo_venda == 1 else 'Unidades'}

# OBSERVAÇÃO:
# • A Responsabilidade de confirmar a aplicação no veículo é exclusivamente do proprietário e do mecânico, uma vez que não temos acesso pessoal e visual da peça instalada.
# • Nossos produtos tem garantia de fábrica de 3 meses. Não cobrimos má instalação ou mau uso do produto, recomendamos que a instalação seja feita por um profissional especializado.
# • As compras realizadas em nome de Pessoa Jurídica podem estar sujeitas à cobrança de ICMS e DIFAL, conforme Protocolo ICMS 41, de 4 de Abril de 2008. Caso você tenha dúvidas sobre o percentual a ser aplicado, consulte a Cláusula Segunda, §1° do referido Protocolo.
# • Para devolução por Defeito de Fabricação o produto deve acompanhar a Nota Fiscal de compra para sua identificação. Sem ela (Nota Fiscal), não é possível identificar o solicitante da Garantia
# • O produto deve ser devolvido nas mesmas condições em que foi enviado (na embalagem original, sem sinais de utilização para a perfeita condição de uso do próximo comprador).
# • Caixa (s) e Imagem (s) Ilustrativa (s), meramente comercial para efeito estético e informativo de marca e modelo do anúncio.

# NÚMERO INTERNO:
# • {codpro}'''
#         return descricao


if __name__ == '__main__':
    from rich import print as rprint
    apl = Aplicacao()
    apl.read_apl()
    apl.criar_ano_inicial_ano_final()
    apl.etl_tabela_aplic()
    rprint(apl._df_aplic)
    rprint(apl._df_aplic_orig)
    rprint(apl.cria_lista_de_numeros_originais('9295080059'))

