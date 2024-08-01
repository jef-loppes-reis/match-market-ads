# Uso

1. Baixe o repositorio do projeto
```bash
git clone https://github.com/jef-loppes-reis/match-market-ads
```
2. Depedencias
```bash
py -m venv .venv;
.venv/Scripts/activate;
pip install -r ./requirements.txt
```

## Processos

-> Pegar informações de uma determinada loja oficial. Passar por uma conferência de **[CORRESPONDÊNCIAS]**, onde o usuário vai apontar se deve ou não clonar tal produto.

```bash
py ./main.py
```

-> Com a lista de correspondências pronta, agora é passar por uma validação de fotos. O programa baixa as fotos de todos os anúncios apontados para clonagem, depois abrirá um **[APLICATIVO]** apontando tal foto e o usuário decidirá qual foto deve ir ou não para o anúncio.

```bash
py ./photo_validation/get_photo_url.py # Baixar fotos
py ./photo_validation/app.py           # Conferir fotos
```

-> Com a lista de correspondências e suas respectivas fotos selecionadas, agora o programa deve **[CLONAR]** os produtos, alterando seus atributos conforme os atributos cadastrados no sistema "SIAC".

```bash
py ./clonagem/uso.py  # Modulo principal para clonagem
```
