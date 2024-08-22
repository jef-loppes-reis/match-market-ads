from re import findall
from time import sleep
from random import randint

from bs4 import BeautifulSoup
from httpx import Client, Response, ConnectError, ConnectTimeout
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
        list_pages: list[str] = []
        paginas: list[str] = self._site_loja_oficial.find_all(
            self._link_num_pagina[0],
            self._link_num_pagina[1]
        )
        for x in paginas:
            page = x.find('a')['href']
            if page is None or page != '':
                list_pages.append(page)
        return list_pages

    # def entrar_pagina(self, client: Client, url: str) -> BeautifulSoup:
    #     tentativas: int = 0
    #     while True:
    #         if tentativas > 9:
    #         _res: Response = client.get(url=url, timeout=None)
    #         if _res.status_code in [429, 500] and tentativas < 11:
    #             sleep(randint(15, 30))
    #             continue
    #         return BeautifulSoup(_res, 'html.parser')

    def entrar_pagina(self, client: Client, url: str) -> BeautifulSoup:
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
            except (ConnectionError, ConnectError):
                sleep(randint(15, 30))
                tentativas+=1
                print('[Erro de conexao] - Tentativas:', tentativas)
                continue
            except ConnectTimeout:
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


    def pegar_link_anuncios(self) -> dict[str, str | float]:
        # Lista de paginas.
        lista_paginas = self.lista_link_paginas_seller()
        rprint({'lista de paginas': lista_paginas})
        lista_tag_mais_vendido: list[str | None] = []
        lista_tag_avaliacao: list[float] = []
        # Entra em cada pagina, e pega o link dos anuncios.
        lista_link_anuncios: list = []
        lista_vendas: list[str] = []
        lista_mlbs: list[str] = []
        with Client() as _client:
            # Iteracao sobre a quantidade maxima de paginas = 10
            num_page: int = 1
            lista_paginas: list[str] = ['Unica'] if len(lista_paginas) == 0 else lista_paginas
            for link_pagina in lista_paginas:
                if link_pagina != 'Unica':
                    driver_anuncios: BeautifulSoup = self.entrar_pagina(
                        client=_client,
                        url=link_pagina
                    )
                else:
                    driver_anuncios: BeautifulSoup = self._site_loja_oficial

                for lista_anuncios in driver_anuncios.find_all(
                    self._anuncios[0],
                    self._anuncios[1]
                ):
                    for anuncio in tqdm(lista_anuncios, desc=f'Get infos anuncios page nº{num_page}: '):
                        _link_anuncio: str = anuncio.find('a')['href']
                        lista_link_anuncios.append(_link_anuncio)

                        sleep(randint(a=3, b=10))
                        _vendas_mlb: tuple[str, str] = self.pegar_numero_vendas(
                            client=_client,
                            url_anuncio=_link_anuncio
                        )
                        _vendas: str = _vendas_mlb[0]
                        _mlb: str = _vendas_mlb[1]

                        lista_vendas.append(_vendas)
                        lista_mlbs.append(_mlb)

                        _tag_mais_vendido: BeautifulSoup = anuncio.find(
                            self._class_tag_mais_vendido[0],
                            self._class_tag_mais_vendido[1]
                        )
                        _tag_mais_vendido_value: str | None = (
                            _tag_mais_vendido.text if not _tag_mais_vendido\
                                  is None else None
                            )
                        lista_tag_mais_vendido.append(_tag_mais_vendido_value)

                        _tag_nota_avaliacao: BeautifulSoup = anuncio.find(
                            self._class_tag_avaliacao[0],
                            self._class_tag_avaliacao[1]
                        )
                        _tag_nota_avaliacao_value: float = float(
                            _tag_nota_avaliacao.text
                            ) if not _tag_nota_avaliacao is None else 0.0
                        lista_tag_avaliacao.append(_tag_nota_avaliacao_value)
                    num_page+=1
                break
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
