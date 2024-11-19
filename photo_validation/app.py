from os import path, listdir, system, makedirs
import tkinter as tk
from tkinter import messagebox

from PIL import Image, ImageTk, ImageFile
from pandas import DataFrame, Series, read_feather
from rich import print as rprint


class App:
    """_summary_
    """

    __df_default = DataFrame()

    # Configuracoes da janela
    __root = tk.Tk()

    # Criando um rótulo para a imagem
    __label_imagem = tk.Label(__root)
    __label_imagem.pack(pady=10)

    # Botões "Sim" e "Não"
    __frame_botoes = tk.Frame(__root)
    __frame_botoes.pack(pady=5)

    # Painal dos indices
    __index_label = tk.Label(__root, text="", font=('Arial', 12))
    __index_label.pack(side='bottom', anchor='se', padx=10, pady=10)

    __index_config = {
        'trava_do_indice': 0,
        'indice_atual': 0,
        'ultimo_indece': 0,
        'indice_maximo': 0,
    }

    __primeira_foto: bool = True
    __marca_dagua: bool = False
    __texto_img: bool = False

    def __init__(self, loja: str) -> None:
        self.__df_genuino: DataFrame = DataFrame()
        self.__df_copy: DataFrame = DataFrame()
        self.__loja: str = loja
        self.__path_files_photos: str = path.join(
            path.dirname(__file__), f'out_files_photos/{self.__loja}')
        self.__path_files_data: str = path.join(
            path.dirname(__file__), f'out/{self.__loja}'
        )

    def created_new_dataframe(self):
        """Metodo para criar a planilha principal da varificacao de fotos.
        """

        lista_fotos: list[str] = listdir(self.__path_files_photos)

        _df_lista_fotos: DataFrame = Series(
            lista_fotos).str.split('_', expand=True)
        _df_lista_fotos[4] = lista_fotos

        self.__df_default: DataFrame = DataFrame(columns=[
            'mlb',
            'verifeid_photo',
            'pegar_foto',
            'marca_dagua',
            'texto_na_foto',
        ])
        self.__df_default['mlb'] = _df_lista_fotos.iloc[:, 0]
        self.__df_default.loc[:, 'path_file_photo'] = lista_fotos
        self.__df_default.loc[:, 'verifeid_photo'] = False
        self.__df_default.loc[:, 'pegar_foto'] = False
        self.__df_default.loc[:, 'marca_dagua'] = False
        self.__df_default.loc[:, 'texto_na_foto'] = False

        self.__df_default.to_feather(f'./out/conferencia_fotos_{self.__loja}.feather')
        self.__df_default.to_csv(f'./out/conferencia_fotos_{self.__loja}.csv', index=False)

    def ler_dataframe(self):
        """Metodo para ler uma planilha de checagem, o primeiro passo do projeto "Verificacao de
        possiveis clonagens".
        """
        # self.__df_genuino = read_feather('./out/df_default.feather')
        try:
            self.__df_genuino = read_feather(
                f'./out/conferencia_fotos_{self.__loja}.feather')
        except FileNotFoundError:
            self.__menssagem_index_error(
                msg=f'./out/conferencia_fotos_{self.__loja}.feather\nChame o metodo "created_new_dataframe", ele vai criar uma planilha de conferencia.'
            )
            rprint('[bright_yellow]Ops, nao encontrei a planilha de conferencia.[/bright_yellow]')
            rprint('Chame o metodo "created_new_dataframe", ele vai criar uma planilha de conferencia.')
            raise
        self.__df_copy = self.__df_genuino.query('~verifeid_photo').copy()
        self.__mudar_indices(indices={
            'trava_do_indice': self.__df_copy.index.to_list()[0],
            'indice_atual': self.__df_copy.index.to_list()[0],
            'ultimo_indece': self.__df_copy.index.to_list()[0],
            'indice_maximo': self.__df_copy.index.to_list()[-1:][0]
        })

    @classmethod
    def __mudar_indices(self, indices: dict[str, int]):
        self.__index_config.update(indices)

    @classmethod
    def __alternar_indice(cls, avancar: False) -> None:
        """Metodo para alternar os indicies do DataFrame de fotos, avanca ou recua.

        Args:
            avancar (False): Parametro para avancar no DataFrame.
            retroceder (False): Paramentro para recuar no DataFrame.
        """
        if avancar:
            cls.__index_config.update(
                {
                    'indice_atual': cls.__index_config.get('indice_atual') + 1
                }
            )
            return
        cls.__index_config.update(
            {
                'indice_atual': cls.__index_config.get('indice_atual') - 1
            }
        )

    def __mostrar_imagem(self) -> ImageTk.PhotoImage:
        path_dir_img = self.__df_copy.loc[
            self.__index_config.get('indice_atual'), 'path_file_photo']

        _img: ImageFile = Image.open(
            f'./out_files_photos/{self.__loja}/{path_dir_img}')

        image_height: int = 500
        ratio: float = image_height / float(_img.height)
        image_width: int = int((float(_img.width) * float(ratio)))

        _img: Image = _img.resize((image_width, image_height))

        _img = ImageTk.PhotoImage(_img)

        self.__label_imagem.config(image=_img)
        self.__label_imagem.image = _img

    def __mostrar_indices(self):
        self.__index_label.config(
            text=f"Índice: {
                self.__index_config.get('indice_atual')
            } de {self.__index_config.get('indice_maximo')}")

    def __menssagem_index_error(self, msg: str):
        messagebox.showerror(
            title='Erro!',
            message=msg
        )

    def __save_data(self):
        self.__df_copy.to_csv(f'./out/conferencia_fotos_{self.__loja}.csv', index=False)
        self.__df_copy.to_feather(f'./out/conferencia_fotos_{self.__loja}.feather')

    def __janela(self,
                 proxima_foto: bool = False,
                 voltar: bool = False,
                 marca_dagua: bool = False,
                 texto_img: bool = False):
        """Metodo de interecao com a janela do Tkinter.

        Args:
            proxima_foto (bool): Resposta de um botao "Foto Correta", caso a foto estaja certa.
            botao_iniciar (bool, optional): Resposta de um botao "Iniciar".
            Dar alguns parametros para a funcao.
            . Defaults to False.
            voltar (bool, optional): Resposta de um botao "Voltar". Indica se o usuario deseja
            voltar a foto anterior. Defaults to False.
        """

        if marca_dagua:
            self.__mudar_status_marca_dagua(True)
        if texto_img:
            self.__mudar_status_texto_img(True)

        if proxima_foto:
            self.__df_copy.loc[self.__index_config.get(
                'indice_atual'), 'verifeid_photo'] = True

            self.__df_copy.loc[self.__index_config.get(
                'indice_atual'), 'pegar_foto'] = not any([self.__marca_dagua, self.__texto_img])

            self.__df_copy.loc[self.__index_config.get(
                'indice_atual'), 'marca_dagua'] = self.__marca_dagua

            self.__df_copy.loc[self.__index_config.get(
                'indice_atual'), 'texto_na_foto'] = self.__texto_img

            self.__save_data()
            self.__alternar_indice(avancar=True)
            self.__mostrar_imagem()
            self.__mostrar_indices()
            self.__mudar_status_marca_dagua(False)
            self.__mudar_status_texto_img(False)
            return

        if voltar:
            if self.__index_config.get('indice_atual') > 0:
                self.__alternar_indice(avancar=False)
                self.__mudar_status_marca_dagua(False)
                self.__mudar_status_texto_img(False)
                self.__df_copy.loc[self.__index_config.get('indice_atual'), 'verifeid_photo'] = False
                self.__df_copy.loc[self.__index_config.get('indice_atual'), 'pegar_foto'] = False
                self.__df_copy.loc[self.__index_config.get('indice_atual'), 'marca_dagua'] = self.__marca_dagua
                self.__df_copy.loc[self.__index_config.get('indice_atual'), 'texto_na_foto'] = self.__texto_img
                self.__save_data()
                self.__mostrar_imagem()
                self.__mostrar_indices()
                return
            self.__menssagem_index_error(msg='Nao possivel volta.')

    def __mudar_status_marca_dagua(self, value: bool):
        self.__marca_dagua = value

    def __mudar_status_texto_img(self, value: bool):
        self.__texto_img = value

    def main(self):
        """Metodo principal, executa a o objeto inteiro.
        """

        if not path.exists(self.__path_files_data):
            self.created_new_dataframe()

        rprint(self.__index_config)

        if self.__primeira_foto:
            self.__primeira_foto = False
            self.__mostrar_imagem()
            self.__mostrar_indices()

        botao_proxima_foto = tk.Button(
            self.__frame_botoes,
            text='Proxima Foto',
            # bg='green',
            command=lambda: self.__janela(
                proxima_foto=True,
                voltar=False
            )
        )
        botao_proxima_foto.pack(side='right', padx=5, pady=5)

        botao_foto_anterior = tk.Button(
            self.__frame_botoes,
            text='Anterior',
            command=lambda: self.__janela(
                proxima_foto=False,
                voltar=True
            )
        )
        botao_foto_anterior.pack(side='bottom', padx=5, pady=5)

        botao_foto_marca_dagua = tk.Button(
            self.__frame_botoes,
            text="Marca d'agua",
            command=lambda: self.__janela(
                proxima_foto=False,
                voltar=False,
                marca_dagua=True
            )
        )
        botao_foto_marca_dagua.pack(side='bottom', padx=5, pady=5)

        botao_foto_texto_img = tk.Button(
            self.__frame_botoes,
            text="Texto na imagem",
            command=lambda: self.__janela(
                proxima_foto=False,
                voltar=False,
                texto_img=True
            )
        )
        botao_foto_texto_img.pack(side='bottom', padx=5, pady=5)

        self.__root.mainloop()


if __name__ == '__main__':

    rprint('\nDigite o nome da loja: ')
    NOME_LOJA: str = input()
    while True:
        rprint(f'[bright_yellow]Nome da loja informada [bright_magenta]{NOME_LOJA.upper()}[/bright_magenta][/bright_yellow]')
        rprint('[bright_yellow]Esta comecando a conferencia agora? Ou deseja continuar de onde parou?[/bright_yellow]')
        rprint('1. Inicio.\n2. Continuar.')
        flag: int = int(input())
        match flag:
            case 1:
                INICIO: bool = True
            case 2:
                INICIO: bool = False
            case _:
                system('cls')
                rprint(f'[blue]{flag}[/blue] [red]e uma opcao nao valida![/red]')
                continue
        break

    app = App(loja=NOME_LOJA)
    if INICIO:
        app.created_new_dataframe()
    app.ler_dataframe()
    app.main()
