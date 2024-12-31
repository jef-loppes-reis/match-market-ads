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
        paginas: list[BeautifulSoup] = self._site_loja_oficial.find_all(
            self._link_num_pagina[0],
            self._link_num_pagina[1]
        )
        for x in paginas:
            __href_page: str = x.find('a', class_='andes-pagination__link').get('href')
            if not __href_page is None:
                list_pages.append(__href_page)
        return list_pages

    def entrar_pagina(self, client: Client, url: str) -> BeautifulSoup:
        """
        Método para acessar uma página e retornar seu conteúdo como BeautifulSoup.

        Parameters
        ----------
        client : Client
            Cliente HTTP para realizar a requisição.
        url : str
            URL da página a ser acessada.

        Returns
        -------
        BeautifulSoup
            Conteúdo da página em formato BeautifulSoup.
        """
        for attempt in range(1, 11):  # Tentativas de 1 a 10
            try:
                _res: Response = client.get(url=url, timeout=10)  # Define um timeout para evitar bloqueios
                if _res.is_success:
                    return BeautifulSoup(_res.text, 'html.parser')  # Use .text para conteúdo HTML
                elif _res.is_server_error:
                    delay = randint(10, 15)
                    rprint('[purple]\n[Method - Entrar na página][/purple]')
                    rprint(f'[yellow]Aviso: Erro de servidor [{_res.status_code}]. Tentativa {attempt}/10, tentando novamente em {delay}s...[/yellow]')
                    sleep(delay)
                elif _res.is_client_error:
                    rprint('[purple]\n[Method - Entrar na página][/purple]')
                    rprint(f'[yellow]Erro cliente [{_res.status_code}].[/yellow]')
                    if _res.status_code == 429:  # Too Many Requests
                        delay = randint(3, 10)
                        rprint(f'[yellow]Aviso: Limite de requisições atingido. Tentativa {attempt}/10, tentando novamente em {delay}s...[/yellow]')
                        sleep(delay)
                    else:
                        rprint(f'[red]Erro cliente não recuperável: [{_res.status_code}][/red]')
                        rprint(f'[yellow]URL: {url}[/yellow]')
                        rprint(_res.json())
                        raise ValueError(f"Erro cliente não recuperável: {_res.status_code}")
            except ReadTimeout:
                delay = randint(15, 30)
                rprint('[purple]\n[Method - Entrar na página][/purple]')
                rprint(f'[yellow]Erro: Tempo limite de leitura. Tentativa {attempt}/10, tentando novamente em {delay}s...[/yellow]')
                sleep(delay)
            except Exception as e:
                delay = randint(15, 30)
                rprint('[purple]\n[Method - Entrar na página][/purple]')
                rprint(f'[yellow]Erro inesperado: {e}. Tentativa {attempt}/10, tentando novamente em {delay}s...[/yellow]')
                sleep(delay)
        rprint('[purple]\n[Method - Entrar na página][/purple]')
        rprint(f'[red]Erro: Número de tentativas excedido após 10 tentativas para URL: {url}.[/red]')
        return BeautifulSoup('', 'html.parser')  # Retorna um objeto BeautifulSoup vazio

    def pegar_numero_vendas(self, client: Client, url_anuncio: str) -> tuple[str, str]:
        page_anuncio: BeautifulSoup = self.entrar_pagina(
            client,
            url_anuncio
        )
        try:
            vendas: str = page_anuncio.find('span', {'class': 'ui-pdp-subtitle'}).text
            mlb: str = page_anuncio.find('span', {'class': 'ui-pdp-color--BLACK ui-pdp-family--SEMIBOLD'}).text
            mlb: str = mlb.replace('#', '')
            mlb: str = f'MLB{mlb}'

            vendas_copy: list[str] = findall(pattern=r'\d+', string=vendas)
            if len(vendas_copy) > 0:
                return (vendas_copy[0], mlb)
            vendas: str = '0'
            return (vendas, mlb)
        except AttributeError: # Caso nao exista a tag de venda.
            mlb: str = page_anuncio.find('span', {'class': 'ui-pdp-color--BLACK ui-pdp-family--SEMIBOLD'}).text
            mlb: str = mlb.replace('#', '')
            mlb: str = f'MLB{mlb}'
            vendas: str = '0'
            # rprint({
            #     'MLB': page_anuncio.find('span', {'class': 'ui-pdp-color--BLACK ui-pdp-family--SEMIBOLD'}),
            #     'vendas': page_anuncio.find('span', {'class': 'ui-pdp-subtitle'})
            # })
            # rprint(f'[purple]\n[Method - Pegar numero de vendas]: {e}[/purple]')
            return (vendas, mlb)

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
                        _link_anuncio: str = anuncio.find('a').get('href')
                        # rprint(f'[yellow]Link do anuncio: {_link_anuncio}[/yellow]')
                        lista_link_anuncios.append(_link_anuncio)

                        # Pausa randômica para evitar bloqueios
                        sleep(randint(2, 5))

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
