@echo off
call .\.venv\Scripts\activate
:: Executando o arquivo fuzzy_siac_ml.py
py fuzzy_siac_ml.py
:: Verifica se a ativação foi bem-sucedida
if errorlevel 1 (
    echo Falha ao ativar o ambiente virtual.
    pause
    exit /b
)
:: Finaliza com sucesso
echo Script executado com sucesso.
:: Verifica se o fuzzy_siac_ml.py encontrou algum erro
if errorlevel 1 (
    echo Ocorreu um erro ao executar o script Python.
    echo Saída de erro: %errorlevel%
    pause
    exit /b
)
:: Finaliza com sucesso
echo Script executado com sucesso.
pause
