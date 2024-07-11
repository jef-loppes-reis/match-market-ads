from os import path, listdir
import tkinter as tk
from tkinter import messagebox

from PIL import Image, ImageTk, ImageFile
from pandas import DataFrame, Series, read_feather
from rich import print as pprint


class App:
    """_summary_
    """

    __path_files_photos = path.join(path.dirname(__file__), 'out_files_photos')

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

    __primeira_foto = True

    def __init__(self) -> None:
        self.__df_genuino: DataFrame = DataFrame()
        self.__df_copy: DataFrame = DataFrame()

    def created_new_dataframe(self):
        """Metodo para criar a planilha principal da varificacao de fotos.
        """

        lista_fotos: list[str] = listdir(self.__path_files_photos)

        _df_lista_fotos: DataFrame = Series(
            lista_fotos).str.split('_', expand=True)
        _df_lista_fotos[4] = lista_fotos

        self.__df_default: DataFrame = DataFrame(
            columns=['mlb', 'verifeid_photo', 'pegar_foto'])
        self.__df_default['mlb'] = _df_lista_fotos.iloc[:, 0]
        self.__df_default.loc[:, 'path_file_photo'] = lista_fotos
        self.__df_default.loc[:, 'verifeid_photo'] = False
        self.__df_default.loc[:, 'pegar_foto'] = False

        self.__df_default.to_feather('./temp/df_default.feather')
        self.__df_default.to_csv('./temp/df_default.csv', index=False)

    def ler_dataframe(self):
        """Metodo para ler uma planilha de checagem, o primeiro passo do projeto "Verificacao de
        possiveis clonagens".
        """
        self.__df_genuino = read_feather('df_default.feather')
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
            f'./out_files_photos/{path_dir_img}')

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

    def __menssagem_index_error(self):
        messagebox.showerror(
            title='Ops',
            message='Nao possivel volta.'
        )

    def __janela(self,
                        foto_correta: bool = False,
                        # botao_iniciar: bool = False,
                        voltar: bool = False
                        ) -> None:
        """Metodo de interecao com a janela do Tkinter.

        Args:
            foto_correta (bool): Resposta de um botao "Foto Correta", caso a foto estaja certa.
            botao_iniciar (bool, optional): Resposta de um botao "Iniciar".
            Dar alguns parametros para a funcao.
            . Defaults to False.
            voltar (bool, optional): Resposta de um botao "Voltar". Indica se o usuario deseja
            voltar a foto anterior. Defaults to False.
        """

        pprint(
            self.__df_copy
        )

        # Mostra o indices na janela, canto inferior direito.
        self.__mostrar_indices()

        # Exibir a imagem na janela
        self.__mostrar_imagem()

        if not voltar:
            self.__df_copy.loc[self.__index_config.get('indice_atual'), 'verifeid_photo'] = True
            self.__df_copy.loc[self.__index_config.get('indice_atual'), 'pegar_foto'] = foto_correta
            self.__alternar_indice(avancar=True)
            return

        if voltar:
            if self.__index_config.get('indice_atual') > 0:
                self.__alternar_indice(avancar=False)
                self.__df_copy.loc[self.__index_config.get(
                    'indice_atual'), 'verifeid_photo'] = False
                self.__df_copy.loc[self.__index_config.get(
                    'indice_atual'), 'pegar_foto'] = False
                return
            self.__menssagem_index_error()

    def main(self):
        """Metodo principal, executa a o objeto inteiro.
        """

        if self.__primeira_foto:
            self.__janela()
            self.__primeira_foto = False

        botao_foto_correta = tk.Button(
            self.__frame_botoes,
            text='Foto correta',
            command=lambda: self.__janela(foto_correta=True)
        )
        botao_foto_correta.pack(side='left', padx=5, pady=5)

        botao_foto_errada = tk.Button(
            self.__frame_botoes,
            text='Foto errada',
            command=lambda: self.__janela(foto_correta=False)
        )
        botao_foto_errada.pack(side='right', padx=5, pady=5)

        botao_foto_anterior = tk.Button(
            self.__frame_botoes,
            text='Anterior',
            command=lambda: self.__janela(
                foto_correta=False, voltar=True)
        )
        botao_foto_anterior.pack(side='left', padx=5, pady=5)

        self.__root.mainloop()


if __name__ == '__main__':
    app = App()
    # app.created_new_dataframe()
    app.ler_dataframe()
    app.main()
