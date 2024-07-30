"""---"""
from bs4 import BeautifulSoup


class InfosLojaOficial:
    """_summary_
    """

    # Lista de todos os links das lojas oficiais da pagina,
    # literalmente uma grade de lojas oficiais.
    _GRID_SHOW_SELLERS_OFFICIALS = ('div', {'class': 'item-grid-show'})

    def __init__(self, driver_sellers: BeautifulSoup) -> None:
        self._driver_seller = driver_sellers

    def get_infos_seller(self) -> tuple[list[str]]:

        _name_seller_official_list: list = []
        _link_seller_official_list: list = []

        for _x in self._driver_seller.find_all(self._GRID_SHOW_SELLERS_OFFICIALS[0], self._GRID_SHOW_SELLERS_OFFICIALS[1]):
            for i in _x:
                _name_seller_official_list.append(
                    i.find('img', alt=True)['alt'])
                _link_seller_official_list.append(i['href'])

        return (
            _name_seller_official_list,
            _link_seller_official_list
        )


if __name__ == '__main__':
    from requests import get
    from rich import print

    driver_seller = BeautifulSoup(
        get(
            'https://www.mercadolivre.com.br/lojas-oficiais/show?category=MLB5672&name=undefined&source=PERSONALIZED',
            timeout=None
        ).content,
        'html.parser'
    )
    x = InfosLojaOficial(driver_seller)
    print(x.get_infos_seller())
