from os import system

import customtkinter as ctk
from customtkinter import CTkButton, CTkImage
from CTkMessagebox import CTkMessagebox
from PIL import Image, ImageTk, ImageFile
from rich import print as rprint
from pandas import DataFrame, read_feather
from os import path, listdir
from re import sub


class ValidadorImagem:

    __columns_default: list[str] = [
        'nome_arquivo_foto',
        'codigo_referencia',
        'endereco',
        'extensao',
        'extensao_valida',
        'pegar_foto',
        'foto_verificada',
        'marca_dagua',
        'texto',
        'fundo_neutro',
        'logo',
        'carro'
    ]

    __extencoes: tuple[str] = (
        '.jpg',
        '.JPG',
        '.png',
        '.PNG',
        '.jpeg',
        '.JPEG'
    )

    __df_default: DataFrame = DataFrame(columns=__columns_default)
    __df_conferencia: DataFrame = DataFrame()
    __df_copy: DataFrame = DataFrame()

    # Configuracoes da janela
    __root: ctk.CTk = ctk.CTk()

    # Criando um rótulo para a imagem
    __label_imagem: ctk.CTkLabel = ctk.CTkLabel(__root)
    __label_imagem.pack(pady=10)

    # Criando um rotulo para texto
    __label_mensagem: ctk.CTkLabel = ctk.CTkLabel(
        __root,
        text='"Selecionar caso a foto tenha os seguintes problemas:"',
        font=('Arial', 16),
        anchor='se'
    )
    __label_mensagem.pack(padx=200, anchor='se')

    # Botões "Sim" e "Não"
    __frame_botoes = ctk.CTkFrame(__root)
    __frame_botoes.pack(pady=5)

    # Painal dos indices
    __index_label: ctk.CTkLabel = ctk.CTkLabel(
        __root,
        text="",
        font=('Arial', 12)
    )
    __index_label.pack(side='bottom', anchor='se', padx=10, pady=10)
    __index_config: dict[str, int] = {
        'trava_do_indice': 0,
        'indice_atual': 0,
        'ultimo_indece': 0,
        'indice_maximo': 0,
    }

    __primeira_foto: bool = True
    __marca_dagua: bool = False
    __texto_img: bool = False
    __fundo_neutro: bool = False
    __logo: bool = False
    __carro: bool = False

    def __init__(self, diretorio_fotos: str, fabricante: str) -> None:
        self._diretorio_fotos: str = diretorio_fotos
        self._fabricante: str = fabricante

    @classmethod
    def __mudar_indices(cls, indices: dict[str, int]):
        cls.__index_config.update(indices)

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

    def created_data_frame(self):
        _lista_fotos: list[str] = listdir(self._diretorio_fotos)
        self.__df_default['nome_arquivo_foto'] = _lista_fotos
        self.__df_default['endereco'] = _lista_fotos
        self.__df_default['foto_verificada'] = False
        self.__df_default['marca_dagua'] = False
        self.__df_default['texto'] = False
        self.__df_default['fundo_neutro'] = False
        self.__df_default['logo'] = False
        self.__df_default['carro'] = False

        self.__df_default.loc[:, 'extensao_valida'] = (
            self.__df_default['nome_arquivo_foto'].str.endswith(
                self.__extencoes)
        )
        self.__df_default.iloc[:,:].to_feather(f'./out/{self._fabricante}/conferencia_fotos_{self._fabricante}.feather')
        self.__df_default.iloc[:,:].to_csv(f'./out/{self._fabricante}/conferencia_fotos_{self._fabricante}.csv', index=False)
        rprint(self.__df_default)

    def ler_dataframe(self):
        """Metodo para ler uma planilha de checagem, o primeiro passo do projeto "Verificacao de
        possiveis clonagens".
        """
        # self.__df_conferencia = read_feather('./out/df_default.feather')
        try:
            self.__df_conferencia = read_feather(
                f'./out/{self._fabricante}/conferencia_fotos_{self._fabricante}.feather')
        except FileNotFoundError:
            __mensagem_erro: str = '\nChame o metodo "created_data_frame", ele vai criar uma planilha de conferencia.'
            self.__menssagem_index_error(
                msg=f'[./out/{self._fabricante}/conferencia_fotos_{self._fabricante}.feather] {__mensagem_erro}'
            )
            rprint('[bright_yellow]Ops, nao encontrei a planilha de conferencia.[/bright_yellow]')
            rprint('Chame o metodo "created_new_dataframe", ele vai criar uma planilha de conferencia.')
            raise
        self.__df_copy: DataFrame = self.__df_conferencia.copy()
        self.__mudar_indices(indices={
            'trava_do_indice': self.__df_copy.query('~foto_verificada').index.to_list()[0],
            'indice_atual': self.__df_copy.query('~foto_verificada').index.to_list()[0],
            'ultimo_indece': self.__df_copy.query('~foto_verificada').index.to_list()[0],
            'indice_maximo': self.__df_copy.query('~foto_verificada').index.to_list()[-1:][0]
        })

    def __mostrar_imagem(self) -> CTkImage:
        path_dir_img = self.__df_copy.loc[self.__index_config.get('indice_atual'), 'endereco']

        _img: ImageFile = Image.open(f'{self._diretorio_fotos}/{path_dir_img}')

        image_height: int = 500
        ratio: float = image_height / float(_img.height)
        image_width: int = int((float(_img.width) * float(ratio)))

        _img: Image = _img.resize((image_width, image_height))

        _ctk_img = CTkImage(light_image=_img, dark_image=_img, size=(_img.width, _img.height))

        self.__label_imagem.configure(image=_ctk_img)
        self.__label_imagem.image = _ctk_img

    def __mostrar_indices(self):
        self.__index_label.configure(
            text=f"Índice: {self.__index_config.get('indice_atual')} de {self.__index_config.get('indice_maximo')}"
        )

    def __menssagem_index_error(self, msg: str):
        CTkMessagebox(
            title='Erro!',
            message=msg
        )

    def __save_data(self):
        self.__df_copy.to_csv(f'./out/{self._fabricante}/conferencia_fotos_{self._fabricante}.csv', index=False)
        self.__df_copy.to_feather(f'./out/{self._fabricante}/conferencia_fotos_{self._fabricante}.feather')

    def __mudar_status_marca_dagua(self, value: bool):
        self.__marca_dagua = value

    def __mudar_status_texto_img(self, value: bool):
        self.__texto_img = value

    def __mudar_status_fundo_neutro(self, value: bool):
        self.__fundo_neutro = value

    def __mudar_status_logo(self, value: bool):
        self.__logo = value

    def __mudar_status_carro(self, value: bool):
        self.__carro = value

    def __janela(self,
                 proxima_foto: bool = False,
                 voltar: bool = False,
                 marca_dagua: bool = False,
                 texto_img: bool = False,
                 fundo_neutro: bool = False,
                 logo: bool = False,
                 carro: bool = False):
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
        if fundo_neutro:
            self.__mudar_status_fundo_neutro(True)
        if logo:
            self.__mudar_status_logo(True)
        if carro:
            self.__mudar_status_carro(True)

        if proxima_foto:
            rprint(self.__marca_dagua)
            self.__df_copy.loc[self.__index_config.get(
                'indice_atual'), 'foto_verificada'] = True

            self.__df_copy.loc[self.__index_config.get(
                'indice_atual'), 'pegar_foto'] = not any(
                    [self.__marca_dagua,
                     self.__texto_img,
                     self.__fundo_neutro,
                     self.__logo]
                )

            self.__df_copy.loc[self.__index_config.get(
                'indice_atual'), 'marca_dagua'] = self.__marca_dagua

            self.__df_copy.loc[self.__index_config.get(
                'indice_atual'), 'texto_na_foto'] = self.__texto_img

            self.__df_copy.loc[self.__index_config.get(
                'indice_atual'), 'fundo_neutro'] = self.__fundo_neutro

            self.__df_copy.loc[self.__index_config.get(
                'indice_atual'), 'logo'] = self.__logo

            self.__df_copy.loc[self.__index_config.get(
                'indice_atual'), 'carro'] = self.__carro

            self.__save_data()

            self.__alternar_indice(avancar=True)
            self.__mostrar_imagem()
            self.__mostrar_indices()
            # Muda os status de todas os atributos para FALSO.
            self.__mudar_status_marca_dagua(False)
            self.__mudar_status_texto_img(False)
            self.__mudar_status_fundo_neutro(False)
            self.__mudar_status_logo(False)
            self.__mudar_status_carro(False)
            # system('cls')
            rprint(self.__df_copy.query('foto_verificada').tail(3))
            return

        if voltar:
            if self.__index_config.get('indice_atual') > 0:
                self.__alternar_indice(avancar=False)
                # Muda os status de todas os atributos para FALSO.
                self.__mudar_status_marca_dagua(False)
                self.__mudar_status_texto_img(False)
                self.__mudar_status_fundo_neutro(False)
                self.__mudar_status_logo(False)
                self.__mudar_status_carro(False)
                # Fluxo de indice
                self.__df_copy.loc[self.__index_config.get('indice_atual'),
                                   'foto_verificada'] = False
                self.__df_copy.loc[self.__index_config.get('indice_atual'),
                                   'pegar_foto'] = False
                self.__df_copy.loc[self.__index_config.get('indice_atual'),
                                   'marca_dagua'] = self.__marca_dagua
                self.__df_copy.loc[self.__index_config.get('indice_atual'),
                                   'texto_na_foto'] = self.__texto_img
                self.__df_copy.loc[self.__index_config.get('indice_atual'),
                                   'fundo_neutro'] = self.__fundo_neutro
                self.__df_copy.loc[self.__index_config.get('indice_atual'),
                                   'logo'] = self.__logo
                self.__df_copy.loc[self.__index_config.get('indice_atual'),
                                   'carro'] = self.__carro
                self.__save_data()
                self.__mostrar_imagem()
                self.__mostrar_indices()
                # Muda os status de todas os atributos para FALSO.
                self.__mudar_status_marca_dagua(False)
                self.__mudar_status_texto_img(False)
                self.__mudar_status_fundo_neutro(False)
                self.__mudar_status_logo(False)
                self.__mudar_status_carro(False)
                system('cls')
                rprint(self.__df_copy.query('foto_verificada').tail(3))
                return
            self.__menssagem_index_error(msg='Nao possivel volta.')

    def main(self):
        """Metodo principal, executa a o objeto inteiro.
        """

        rprint(self.__index_config)

        if self.__primeira_foto:
            self.__primeira_foto = False
            self.__mostrar_imagem()
            self.__mostrar_indices()

        botao_proxima_foto = CTkButton(
            self.__frame_botoes,
            text='Proxima',
            fg_color='green',
            command=lambda: self.__janela(
                proxima_foto=True,
                voltar=False
            )
        )
        botao_proxima_foto.pack(side='right', padx=5, pady=5)

        botao_foto_anterior = CTkButton(
            self.__frame_botoes,
            text='Anterior',
            fg_color='yellow',
            text_color='black',
            command=lambda: self.__janela(
                proxima_foto=False,
                voltar=True
            )
        )
        botao_foto_anterior.pack(side='left', padx=5, pady=5)

        botao_foto_marca_dagua = CTkButton(
            self.__frame_botoes,
            text="Marca d'agua",
            command=lambda: self.__janela(
                proxima_foto=False,
                voltar=False,
                marca_dagua=True
            )
        )
        botao_foto_marca_dagua.pack(side='bottom', padx=5, pady=5)

        botao_foto_texto_img = CTkButton(
            self.__frame_botoes,
            text="Texto na imagem",
            command=lambda: self.__janela(
                proxima_foto=False,
                voltar=False,
                texto_img=True
            )
        )
        botao_foto_texto_img.pack(side='bottom', padx=5, pady=5)

        botao_foto_fundo_neutro = CTkButton(
            self.__frame_botoes,
            text="Fundo neutro",
            command=lambda: self.__janela(
                proxima_foto=False,
                voltar=False,
                fundo_neutro=True
            )
        )
        botao_foto_fundo_neutro.pack(side='bottom', padx=5, pady=5)

        botao_foto_logo = CTkButton(
            self.__frame_botoes,
            text="Logo",
            command=lambda: self.__janela(
                proxima_foto=False,
                voltar=False,
                logo=True
            )
        )
        botao_foto_logo.pack(side='bottom', padx=5, pady=5)

        botao_foto_logo = CTkButton(
            self.__frame_botoes,
            text="Carro",
            command=lambda: self.__janela(
                proxima_foto=False,
                voltar=False,
                carro=True
            )
        )
        botao_foto_logo.pack(side='bottom', padx=5, pady=5)

        self.__root.mainloop()

if __name__ == '__main__':
    # DIRETORIO_FOTOS: str = 'C:/Users/jeferson.lopes/ownCloud - Jeferson Lopes@cloud.pecista.com.br/takao'
    # DIRETORIO_FOTOS: str = 'C:/Users/jeferson.lopes/ownCloud - Jeferson Lopes@cloud.pecista.com.br/projeto_clonagem/takao'
    DIRETORIO_FOTOS: str = './out_files_photos/monroe'
    DIRETORIO_DATAFRAME: str = ''
    validador: ValidadorImagem = ValidadorImagem(DIRETORIO_FOTOS, 'monroe')
    # validador.created_data_frame()
    validador.ler_dataframe()
    validador.main()
