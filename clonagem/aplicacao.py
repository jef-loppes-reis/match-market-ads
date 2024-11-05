from pandas import DataFrame, isna
from numpy import nan
from tqdm import tqdm
from ecomm import Postgres


class Aplicacao:
    def __init__(self) -> None:
        self._df_aplic: DataFrame = DataFrame()
        self._df_aplic_orig: DataFrame = DataFrame(columns=['num_orig', 'aplic'])

    def read_apl(self) -> None:
        with Postgres() as db:
            with open('./data/apl.sql', 'r', encoding='utf-8') as fp:
                self._df_aplic = db.query(fp.read())
                self._df_aplic['ano'] = None
            self._df_aplic_orig: DataFrame = db.query('''
                SELECT original.nu_origina as num_orig,
                       original.codpro,
                       original.num_orig as original_ref
                FROM "D-1".original
            ''')

    def criar_ano_inicial_ano_final(self) -> None:
        def formatar_ano(row: DataFrame):
            ano_i, ano_f = row['ano_i'], row['ano_f']
            if not isna(ano_f) and isna(ano_i):
                return f'até {ano_f}'
            if not isna(ano_i) and isna(ano_f):
                return f'de {ano_i} em diante'
            if not isna(ano_i) and not isna(ano_f):
                return f'{ano_i} até {ano_f}'
            return '(Confira o ano de veículo no campo de perguntas.)'

        self._df_aplic['ano'] = self._df_aplic.apply(formatar_ano, axis=1)

    def get_aplicacao(self, original: str) -> str:
        df_temp: DataFrame = (self._df_aplic
                   .query(f'num_orig == "{original}"')
                   .drop(['ano_i', 'ano_f'], axis=1)
                   .reset_index(drop=True)
                )
        aplicacoes: list[str] = [
            ' '.join(filter(None, df_temp.iloc[idx, 1:].tolist())).strip()
            for idx in df_temp.index
        ]
        return '\n'.join(f'• {aplic}' for aplic in aplicacoes)

    def lista_originais(self, original: str) -> str:
        lista_oem: list[str] = (
            self._df_aplic_orig
            .query('num_orig == @original')['original_ref']
            .tolist())
        return ', '.join(lista_oem)

    def venda(self, kit: bool, multiplo: int) -> str:
        return '1 Kit' if kit else f'{multiplo} {"Unidade" if multiplo == 1 else "Unidades"}'

    def criar_descricao(
        self,
        codpro: str,
        aplicacao_veiculos: str,
        titulo: str,
        marca: str,
        num_fab: str,
        oems: str,
        multiplo_venda: int,
        kit: bool
    ) -> str:

        veiculos: str = (
            f'\nSERVE NOS SEGUINTES VEÍCULOS:\n(Em caso de dúvidas, perguntar no campo de perguntas.)\n\n{aplicacao_veiculos}\n\n'
            if not isna(aplicacao_veiculos) else '\n(Em caso de dúvidas, perguntar no campo de perguntas.)\n\n'
        )

        descricao: str = (
f'''{titulo}
{veiculos}
CARACTERÍSTICAS DO PRODUTO:
• Marca: {marca}
• Código da peça: {num_fab}
• Código de referência: {oems}

CONTEÚDO DA EMBALAGEM:
• {self.venda(kit, multiplo_venda)}

OBSERVAÇÃO:
• A Responsabilidade de confirmar a aplicação no veículo é exclusivamente do proprietário e do mecânico, uma vez que não temos acesso pessoal e visual da peça instalada.
• Nossos produtos têm garantia de fábrica de 3 meses. Não cobrimos má instalação ou mau uso do produto; recomendamos que a instalação seja feita por um profissional especializado.
• As compras realizadas em nome de Pessoa Jurídica podem estar sujeitas à cobrança de ICMS e DIFAL, conforme Protocolo ICMS 41, de 4 de Abril de 2008. Caso você tenha dúvidas sobre o percentual a ser aplicado, consulte a Cláusula Segunda, §1° do referido Protocolo.
• Para devolução por Defeito de Fabricação o produto deve acompanhar a Nota Fiscal de compra para sua identificação. Sem ela (Nota Fiscal), não é possível identificar o solicitante da Garantia.
• O produto deve ser devolvido nas mesmas condições em que foi enviado (na embalagem original, sem sinais de utilização para a perfeita condição de uso do próximo comprador).
• Caixa(s) e Imagem(s) Ilustrativa(s), meramente comercial para efeito estético e informativo de marca e modelo do anúncio.

NÚMERO INTERNO:
• {codpro}'''
        )

        return descricao


if __name__ == '__main__':
    from rich import print as rprint
    apl = Aplicacao()
    apl.read_apl()
    apl.criar_ano_inicial_ano_final()
    rprint(apl._df_aplic)
    rprint(apl._df_aplic_orig)
    rprint(apl.cria_lista_de_numeros_originais('ZM996'))

