from requests import Client
from bs4 import BeautifulSoup
from tqdm import tqdm
from time import sleep
from random import randint

def pegar_link_anuncios(self) -> dict[str, list[str] | list[float]]:
    """Função para extrair links e informações de anúncios de uma lista de páginas."""
    
    lista_paginas = self.lista_link_paginas_seller() or ['Unica']
    rprint(f'\nQuantidade de páginas: {len(lista_paginas)}')

    # Inicializando listas para armazenar os dados
    lista_link_anuncios = []
    lista_tag_mais_vendido = []
    lista_tag_avaliacao = []
    lista_vendas = []
    lista_mlbs = []

    with Client() as _client:
        for num_page, link_pagina in enumerate(lista_paginas, start=1):
            # Determina a página a ser acessada
            driver_anuncios = (
                self.entrar_pagina(client=_client, url=link_pagina)
                if link_pagina != 'Unica'
                else self._site_loja_oficial
            )

            # Itera sobre anúncios na página atual
            for lista_anuncios in driver_anuncios.find_all(self._anuncios[0], self._anuncios[1]):
                for anuncio in tqdm(lista_anuncios, desc=f'Get infos anuncios page nº{num_page}: '):
                    # Extrai o link do anúncio
                    _link_anuncio = anuncio.find('a')['href']
                    lista_link_anuncios.append(_link_anuncio)

                    # Pausa randômica para evitar bloqueios
                    sleep(randint(0, 5))

                    # Extrai vendas e identificador do anúncio (MLB)
                    _vendas, _mlb = self.pegar_numero_vendas(client=_client, url_anuncio=_link_anuncio)
                    lista_vendas.append(_vendas)
                    lista_mlbs.append(_mlb)

                    # Extrai a tag "mais vendido" e a avaliação, se existirem
                    lista_tag_mais_vendido.append(self._extrair_texto_tag(anuncio, self._class_tag_mais_vendido))
                    lista_tag_avaliacao.append(float(self._extrair_texto_tag(anuncio, self._class_tag_avaliacao, default="0")))

    return {
        'lista_link_anuncios': lista_link_anuncios,
        'lista_tag_mais_vendido': lista_tag_mais_vendido,
        'lista_tag_avaliacao': lista_tag_avaliacao,
        'lista_vendas': lista_vendas,
        'lista_mlbs': lista_mlbs,
    }

def _extrair_texto_tag(self, anuncio: BeautifulSoup, tag_class: tuple[str, str], default: str = None) -> str | None:
    """Função auxiliar para extrair texto de uma tag especificada no anúncio."""
    tag = anuncio.find(tag_class[0], tag_class[1])
    return tag.text if tag is not None else default
