from os import path, mkdir, listdir
import tkinter as tk
from tkinter import messagebox

from PIL import Image, ImageTk, ImageFile
from pandas import DataFrame, Series, read_feather
from rich import print as pprint


class App:

    _path_files_photos = path.join(path.dirname(__file__), 'out_files_photos')

    _df_default = DataFrame()

    # Configuracoes da janela
    _root = tk.Tk()

    # Criando um rótulo para a imagem
    _label_imagem = tk.Label(_root)
    _label_imagem.pack(pady=10)

    # Botões "Sim" e "Não"
    _frame_botoes = tk.Frame(_root)
    _frame_botoes.pack(pady=5)

    # Painal dos indices
    _index_label = tk.Label(_root, text="")
    _index_label.pack(side='bottom', anchor='se', padx=10, pady=10)

    _index_config = {
        'trava_do_indice': 0,
        'indice_atual': 0,
        'ultimo_indece': 0,
        'indice_maximo': 0,
    }

    _bandeira_mudar_status_foto = False

    def __init__(self) -> None:
        self._df_genuino: DataFrame = DataFrame()
        self._df_copy: DataFrame = DataFrame()

    def created_new_dataframe(self):
        lista_fotos: list[str] = listdir(self._path_files_photos)

        _df_lista_fotos: DataFrame = Series(lista_fotos).str.split('_', expand=True)
        _df_lista_fotos[4] = lista_fotos

        self._df_default: DataFrame = DataFrame(columns=['mlb', 'verifeid_photo'])
        self._df_default['mlb'] = _df_lista_fotos.iloc[:, 0]
        self._df_default.loc[:, 'path_file_photo'] = lista_fotos
        self._df_default.loc[:, 'verifeid_photo'] = False
        # for mlb in _df_lista_fotos.iloc[:, 0].unique():
        #     _df.loc[len(_df)] = {
        #         'mlb': mlb,
        #         'data_mlb': [x for x in _df_lista_fotos[_df_lista_fotos.iloc[:, 0] == mlb].iloc[:, 4]],
        #         'verifeid_photo_list': False
        #     }
        self._df_default.to_feather('df_default.feather')
        self._df_default.to_csv('df_default.csv', index=False)

    def _mudar_indices(self, **kargs):
        self._index_config.update(kargs)

    def ler_dataframe(self):
        self._df_genuino = read_feather('df_default.feather')
        self._df_copy = self._df_genuino.query('~verifeid_photo').copy()
        self._mudar_indices(kargs={
            'trava_do_indice': self._df_copy.index.to_list()[0],
            'indice_atual': self._df_copy.index.to_list()[0],
            'ultimo_indece': self._df_copy.index.to_list()[0],
            'indice_maximo': self._df_copy.index.to_list()[-1:][0]
        })

    def menssagem_index_error(self):
        messagebox.showerror('Ops!', 'Nao e possivel voltar !')

    def imagem_janela(self, foto_correta: bool, botao_iniciar: bool = False, voltar: bool = False):
        try:
            _df_row = self._df_copy.loc[self._index_config.get('indice_atual')].copy()
        except KeyError:
            # ! Nao esta funcionando, verificar.
            self.menssagem_index_error()
            self._index_config.update({'indice_atual': 0})
            return None

        _img: ImageFile = Image.open(f'./out_files_photos/{_df_row['path_file_photo']}')

        image_height: int = 500
        ratio: float = image_height / float(_img.height)
        image_width: int = int((float(_img.width) * float(ratio)))

        _img: Image = _img.resize((image_width, image_height))

        _img = ImageTk.PhotoImage(_img)

        # Exibir a imagem na janela
        self._label_imagem.config(image=_img)
        self._label_imagem.image = _img

        if not botao_iniciar:
            self._df_copy.loc[self._index_config.get(
                'indice_atual'), 'verifeid_photo'] = foto_correta
            if voltar and (self._index_config.get('indice_atual') > 0):
                self._index_config.update({
                    'indice_atual': self._index_config.get('indice_atual') - 1
                })
            else :
                self._index_config.update({
                    'indice_atual': self._index_config.get('indice_atual') + 1
                })

        pprint(self._df_copy.loc[self._index_config.get('indice_atual')-1])
        pprint(self._index_config)


    def main(self):

        botao_comecar = tk.Button(
            self._frame_botoes,
            text='Iniciar',
            command=lambda: self.imagem_janela(foto_correta=False, botao_iniciar=True)
        )
        botao_comecar.pack(side='left', padx=5, pady=5)

        botao_foto_correta = tk.Button(
            self._frame_botoes,
            text='Foto correta',
            command=lambda: self.imagem_janela(foto_correta=True)
        )
        botao_foto_correta.pack(side='left', padx=5, pady=5)

        botao_foto_errada = tk.Button(
            self._frame_botoes,
            text='Foto errada',
            command=lambda: self.imagem_janela(foto_correta=False)
        )
        botao_foto_errada.pack(side='left', padx=5, pady=5)

        botao_foto_anterior = tk.Button(
            self._frame_botoes,
            text='Anterior',
            command=lambda: self.imagem_janela(foto_correta=False, voltar=True)
        )
        botao_foto_anterior.pack(side='left', padx=5, pady=5)

        self._root.mainloop()

if __name__ == '__main__':
    app = App()
    app.ler_dataframe()
    app.main()
