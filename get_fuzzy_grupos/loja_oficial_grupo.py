class LojaOficialGrupo:

    def gerar_link(self, grupo: str):
        base_url: str = f'https://lista.mercadolivre.com.br/{grupo}_Loja_bosch-autopecas_NoIndex_True#D[A:{grupo},on]'
        return base_url
