"""----"""
import unicodedata
import re


class ReplaceCaract:

    _texto_sem_acento = None
    _texto_limpo = None

    def __init__(self, texto: str) -> None:
        self._texto: str = texto

    def remover_acentos(self) -> str:
        # Normaliza o texto para decompor caracteres acentuados
        _nfkd = unicodedata.normalize('NFKD', self._texto)
        # Remove caracteres acentuados
        self._texto_sem_acento = ''.join(
            [c for c in _nfkd if not unicodedata.combining(c)])

    def remover_caracteres_especiais(self) -> str:
        # Regex para manter apenas letras e números
        self._texto_limpo = re.sub(r'[^a-zA-Z0-9\s]', '', self._texto)

    def get_texto_limpo(self):
        return self._texto_limpo


if __name__ == "__main__":
    texto_replace = ReplaceCaract("Olá, como você está? Ça va bien!")
    texto_replace.remover_acentos()
    texto_replace.remover_caracteres_especiais()
    print(texto_replace.get_texto_limpo())
