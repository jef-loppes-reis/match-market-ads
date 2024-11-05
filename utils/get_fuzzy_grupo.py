from ecomm import Postgres
from pandas import DataFrame, isna
from rich import print as rprint


class GetFuzzyGrupos:

    tam_dataframe: int = 0
    df_marca_grupo: DataFrame = DataFrame()

    def __init__(self, marca) -> None:
        self._marca: str = marca

    def carrega_tabela(self) -> None:
        with open('./data/sql/grupo_por_marca.sql', 'r', encoding='utf-8') as fp:
            with Postgres() as db:
                self.df_marca_grupo: DataFrame = db.query(fp.read() % self._marca)
                self.tam_dataframe: int = len(self.df_marca_grupo)

    def manipulacao_dataframe(self) -> None:
        self.df_marca_grupo['grupo_subgrupo'] = None
        for idx in self.df_marca_grupo.index:
            row: DataFrame = self.df_marca_grupo.loc[idx].copy()
            if isna(row['subgrupo']):
                self.df_marca_grupo.loc[idx, 'grupo_subgrupo'] = row['grupo']
            else:
                self.df_marca_grupo.loc[idx, 'grupo_subgrupo'] = row['subgrupo']

    def gerar_link(self, base_url: str) -> str:
        linha: str = base_url
        return base_url.replace('')

    def main(self) -> None | DataFrame:
        self.carrega_tabela()
        self.manipulacao_dataframe()
        if self.tam_dataframe < 1:
            rprint('[red]Ops, nao existe grupos para essa marca de produto.[/red]')
            return None
        return self.df_marca_grupo


if __name__ == '__main__':
    # GRUPO: str = input('Digite a marca do produto referente ao SIAC.: ')
    MARCA: str = 'KYB'
    pegar_grupos: GetFuzzyGrupos = GetFuzzyGrupos(marca=MARCA)
    df_grupos: DataFrame = pegar_grupos.main()

