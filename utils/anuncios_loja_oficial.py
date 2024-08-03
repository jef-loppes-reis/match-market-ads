from bs4 import BeautifulSoup
from httpx import Client, Response


class AnunciosLojaOficial:
    """_summary_
    """

    # Lista de todas as paginas de anuncios da loja oficial
    _link_num_pagina = ('li', {'class': 'andes-pagination__button'})
    _anuncios = ('ol', {'class': 'ui-search-layout ui-search-layout--grid'})

    def __init__(self, site_loja_oficial: BeautifulSoup) -> None:
        self._site_loja_oficial = site_loja_oficial


    def lista_link_paginas_seller(self) -> list[str]:
        list_pages: list = []
        for x in self._site_loja_oficial.find_all(
            self._link_num_pagina[0],
            self._link_num_pagina[1]
        ):
            try:
                page = x.find('a')['href']
                if page != '' or page is None:
                    list_pages.append(page)
            except TypeError:
                pass
        return list_pages

    def entrar_pagina(self, client: Client, url: str) -> BeautifulSoup:
        _res: Response = client.get(url=url, timeout=None)
        return BeautifulSoup(_res, 'html.parser')

    def pegar_link_anuncios(self):
        # Lista de paginas.
        lista_paginas = self.lista_link_paginas_seller()
        # Entra em cada pagina, e pega o link dos anuncios.
        lista_link_anuncios: list = []
        with Client() as _client:
            # Iteracao sobre a quantidade maxima de paginas = 10
            for link_pagina in lista_paginas:
                # print(link_pagina)
                driver_anuncios: BeautifulSoup = self.entrar_pagina(client=_client, url=link_pagina)
                for lista_anuncios in driver_anuncios.find_all(self._anuncios[0], self._anuncios[1]):
                    lista_link_anuncios.extend([anuncio.find('a')['href'] for anuncio in lista_anuncios])
        return lista_link_anuncios


if __name__ == '__main__':
    from httpx import Client
    from rich import print

    with Client() as client:
        driver_response = client.get(
            url='https://loja.mercadolivre.com.br/2m-plastic',
            timeout=None
        )

    driver = BeautifulSoup(driver_response.content, 'html.parser')
    anuncios_loja_oficial = AnunciosLojaOficial(driver)
    print(anuncios_loja_oficial.pegar_link_anuncios())
