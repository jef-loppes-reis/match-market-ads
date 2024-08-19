from bs4 import BeautifulSoup
from httpx import Client, Response
from rich import print as rprint


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
        for x in self._site_loja_oficial.find_all(
            self._link_num_pagina[0],
            self._link_num_pagina[1]
        ):
            page = x.find('a')['href']
            if page != '' or page is None:
                list_pages.append(page)
        return list_pages

    def entrar_pagina(self, client: Client, url: str) -> BeautifulSoup:
        _res: Response = client.get(url=url, timeout=None)
        return BeautifulSoup(_res, 'html.parser')

    def pegar_link_anuncios(self) -> dict[str, str | float]:
        # Lista de paginas.
        lista_paginas = self.lista_link_paginas_seller()
        rprint({'lista de paginas': lista_paginas})
        lista_tag_mais_vendido: list[str | None] = []
        lista_tag_avaliacao: list[float] = []
        # Entra em cada pagina, e pega o link dos anuncios.
        lista_link_anuncios: list = []
        with Client() as _client:
            # Iteracao sobre a quantidade maxima de paginas = 10
            for link_pagina in lista_paginas:
                driver_anuncios: BeautifulSoup = self.entrar_pagina(
                    client=_client,
                    url=link_pagina
                )
                for lista_anuncios in driver_anuncios.find_all(
                    self._anuncios[0],
                    self._anuncios[1]
                ):
                    for anuncio in lista_anuncios:
                        lista_link_anuncios.append(anuncio.find('a')['href'])

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
                # break
        return {
            'lista_link_anuncios': lista_link_anuncios,
            'lista_tag_mais_vendido': lista_tag_mais_vendido,
            'lista_tag_avaliacao': lista_tag_avaliacao
        }


if __name__ == '__main__':
    from httpx import Client

    with Client() as client:
        driver_response = client.get(
            url='https://lista.mercadolivre.com.br/vela_Loja_bosch-autopecas_NoIndex_True#deal_print_id=64326520-51a6-11ef-879f-a31cad41d250&c_id=special-normal&c_element_order=10&c_campaign=LABEL&c_uid=64326520-51a6-11ef-879f-a31cad41d250',
            timeout=None
        )

    driver = BeautifulSoup(driver_response.content, 'html.parser')
    anuncios_loja_oficial = AnunciosLojaOficial(driver)
    rprint(anuncios_loja_oficial.pegar_link_anuncios().get('lista_tag_mais_vendido'))
