"""---"""
from re import findall
from time import sleep
from random import randint

from bs4 import BeautifulSoup
from httpx import Client, Response, ReadError, ReadTimeout
from rich import print as rprint
from tqdm import tqdm


class AnunciosLojaOficial:
    """_summary_
    """

    # Lista de todas as paginas de anuncios da loja oficial
    _link_num_pagina: tuple[str, dict[str, str]] = (
        'li',
        {'class': 'andes-pagination__button'}
    )
    _anuncios: tuple[str, dict[str, str]] = (
        'ol',
        {'class': 'ui-search-layout ui-search-layout--grid'}
    )
    _class_tag_mais_vendido: tuple[str, dict[str, str]] = (
        'div',
        {'class': 'ui-search-item__highlight-label__container'}
    )
    _class_tag_avaliacao: tuple[str, dict[str, str]] = (
        'span',
        {'class', 'ui-search-reviews__rating-number'}
    )

    def __init__(self, site_loja_oficial: BeautifulSoup) -> None:
        self._site_loja_oficial = site_loja_oficial


    def lista_link_paginas_seller(self) -> list[str]:
        """
        lista_link_paginas_seller _summary_

        Returns
        -------
        list[str]
            _description_
        """
        list_pages: list[str] = []
        paginas: list[str] = self._site_loja_oficial.find_all(
            self._link_num_pagina[0],
            self._link_num_pagina[1]
        )
        for x in paginas:
            if 'href' in x.find('a'):
                page = x.find('a')['href']
                if page is None or page != '':
                    list_pages.append(page)
        return list_pages

    def entrar_pagina(self, client: Client, url: str) -> BeautifulSoup:
        """
        entrar_pagina _summary_

        Parameters
        ----------
        client : Client
            _description_
        url : str
            _description_

        Returns
        -------
        BeautifulSoup
            _description_
        """
        tentativas: int = 0
        while True:
            if tentativas > 9:
                return
            try:
                _res: Response = client.get(url=url, timeout=None)
                if _res.status_code in [429, 500]:
                    sleep(randint(15, 30))
                    tentativas+=1
                    print('[429, 500] - Tentativas:', tentativas)
                    continue
                return BeautifulSoup(_res, 'html.parser')
            except (ConnectionError, ReadError):
                sleep(randint(15, 30))
                tentativas+=1
                print('[Erro de conexao] - Tentativas:', tentativas)
                continue
            except ReadTimeout:
                sleep(randint(15, 30))
                tentativas+=1
                print('[TimeOut] - Tentativas:', tentativas)
                continue

    def pegar_numero_vendas(self, client: Client, url_anuncio: str) -> tuple[str, str]:
        page_anuncio: BeautifulSoup = self.entrar_pagina(
            client,
            url_anuncio
        )
        try:
            vendas: str = page_anuncio.find(
                'span', {'class': 'ui-pdp-subtitle'}
            ).text
            mlb: str = page_anuncio.find(
                'span', {'class': 'ui-pdp-color--BLACK ui-pdp-family--SEMIBOLD'}
            ).text
            mlb: str = mlb.replace('#', '')
            mlb: str = f'MLB{mlb}'

            vendas_copy: list[str] = findall(
                pattern=r'\d+', string=vendas)
            if len(vendas_copy) > 0:
                return (vendas_copy[0], mlb)
            return ('0', mlb)
        except Exception as e:
            rprint(page_anuncio, e)
            return ('0', mlb)

    def _extrair_texto_tag(self,
                           anuncio: BeautifulSoup,
                           tag_class: tuple[str, str],
                           default: str = None) -> str | None:
        """Função auxiliar para extrair texto de uma tag especificada no anúncio."""
        tag = anuncio.find(tag_class[0], tag_class[1])
        return tag.text if tag is not None else default

    def pegar_link_anuncios(self) -> dict[str, list[str] | list[float]]:
        """
        Função para extrair links e informações de anúncios de uma lista de páginas.

        Returns
        -------
        dict[str, list[str] | list[float]]
            _description_
        """
        lista_paginas: list[str] = self.lista_link_paginas_seller() or ['Unica']
        rprint(f'\nQuantidade de paginas: {len(lista_paginas)}')

        # Inicializando listas para armazenar os dados
        lista_tag_mais_vendido: list[str | None] = []
        lista_tag_avaliacao: list[float] = []
        lista_link_anuncios: list = []
        lista_vendas: list[str] = []
        lista_mlbs: list[str] = []

        with Client() as _client:
            for num_page, link_pagina in enumerate(lista_paginas, start=1):
                # Determina a pagina a ser acessada.
                driver_anuncios: BeautifulSoup = (
                    self.entrar_pagina(client=_client, url=link_pagina)
                    if link_pagina != 'Unica'
                    else self._site_loja_oficial
                )

                # Itera sobre anúncios na página atual
                anuncio: BeautifulSoup = BeautifulSoup()
                for lista_anuncios in driver_anuncios.find_all(
                    self._anuncios[0], self._anuncios[1]
                ):
                    for anuncio in tqdm(
                        iterable=lista_anuncios,
                        desc=f'Get infos anuncios page nº{num_page}.: '
                    ):
                        # Extrai o link do anúncio
                        _link_anuncio: str = anuncio.find('a')['href']
                        lista_link_anuncios.append(_link_anuncio)

                        # Pausa randômica para evitar bloqueios
                        sleep(randint(0, 5))

                        # Extrai vendas e identificador do anúncio (MLB)
                        _vendas, _mlb = self.pegar_numero_vendas(
                            client=_client,
                            url_anuncio=_link_anuncio
                        )
                        lista_vendas.append(_vendas)
                        lista_mlbs.append(_mlb)

                        # Extrai a tag "mais vendido" e a avaliação, se existirem
                        lista_tag_mais_vendido.append(self._extrair_texto_tag(
                                anuncio,
                                self._class_tag_mais_vendido
                            ))
                        lista_tag_avaliacao.append(
                            self._extrair_texto_tag(
                                anuncio,
                                self._class_tag_avaliacao
                            ))
        return {
            'lista_link_anuncios': lista_link_anuncios,
            'lista_tag_mais_vendido': lista_tag_mais_vendido,
            'lista_tag_avaliacao': lista_tag_avaliacao,
            'lista_vendas': lista_vendas,
            'lista_mlbs': lista_mlbs,
        }


if __name__ == '__main__':
    from httpx import Client

    with Client() as client:
        driver_response = client.get(
            url='https://lista.mercadolivre.com.br/rele_Loja_bosch-autopecas_NoIndex_True#D[A:rele,on]',
            timeout=None
        )

    driver = BeautifulSoup(driver_response.content, 'html.parser')
    anuncios_loja_oficial = AnunciosLojaOficial(driver)
    anuncios_loja_oficial.lista_link_paginas_seller()
    # rprint(driver.find('ol', {'class': 'ui-search-layout ui-search-layout--grid'}))
    # rprint(anuncios_loja_oficial.pegar_link_anuncios().get('lista_tag_mais_vendido'))
    rprint(anuncios_loja_oficial.pegar_link_anuncios())
