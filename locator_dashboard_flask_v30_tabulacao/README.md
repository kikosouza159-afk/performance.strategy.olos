# Dashboard Flask V11

Projeto com duas telas:
- `/` Dashboard principal do fluxo Localizador IA → Humano
- `/comparativo` Dashboard comparativo entre campanhas usando a mesma base

## Como usar
1. Coloque a planilha em `data/base.xlsx` ou defina a variável `EXCEL_PATH`
2. Instale as dependências:
   ```bash
   pip install -r requirements.txt
   ```
3. Rode a aplicação:
   ```bash
   python app.py
   ```
4. Abra no navegador:
   ```
   http://127.0.0.1:5000
   ```

## Estrutura esperada da base
Colunas principais:
- Dt
- Hour
- NomeCampanha
- AD
- ATH
- Mailing
- Discado
- Atendidas
- Transferencia
- Recebidas
- Cpc
- Acordo
- Spin
- Custo

Campos ausentes em uma campanha são tratados como zero.

## Aba Tabulação - V22

A rota `/tabulacao` lê a segunda planilha do mesmo arquivo `data/base.xlsx` por padrão.

Estrutura esperada da segunda aba:
- data
- NomeCampanha
- Tabulacao
- Class_Loc
- Class_Rec
- Quantidade
- TMA_LOC
- TMA_REC
- colunas por hora, como 8horas, 9horas, 10horas...

Caso sua aba tenha outro nome, defina a variável de ambiente:

```powershell
$env:TABULACAO_SHEET="Planilha2"
python app.py
```


## V24 - Aba Tabulação
A aba Tabulação agora suporta o layout: data, Hora, NomeCampanha, Origem_Tabulação, Tabulacao, Classificacao, Quantidade, Tempo_Total_Tabulação e TMA. Use Origem_Tabulação com valores Locator/Receptivo para separar os gráficos.


## V28
Correção dos filtros laterais da aba Tabulação: removidos campos ocultos duplicados que impediam o select de aplicar o valor escolhido.


## V29
Na aba Tabulação, os cards acima dos gráficos de Locator/Receptivo exibem somente TMA, removendo os cards quantitativos de volume.


## V30
Cards principais de TMA na aba Tabulação agora consideram somente classificações Contato e CPC com TMA maior que zero, evitando distorção por Discado com 00:00:00.
