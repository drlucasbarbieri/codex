# OFI + VPIN no WIN via ProfitDLL (modo virtual)

Pesquisa e paper trading. **Nenhum arquivo daqui envia ordem real**: o feed faz login
só de market data (`DLLInitializeMarketLogin`), e toda execução é simulada pelo
`PaperBroker`.

| Arquivo | Função |
|---|---|
| `ofi_vpin_strategy.py` | Estratégia, PaperBroker e replay/backtest de arquivos JSONL |
| `profitdll_feed.py` | Conecta na ProfitDLL, grava o pregão em JSONL e roda a estratégia ao vivo em modo virtual |
| `optimize.py` | Grade de parâmetros + walk-forward (resultado fora da amostra) |
| `synthetic.py` | Mercado sintético e DLL simulada, para testar o pipeline sem a Nelogica |

## Passo a passo no Windows (PowerShell)

1. **Python 64-bit** (3.11+). Confira: `py -c "import struct; print(struct.calcsize('P')*8)"` → precisa dar `64`.
2. Copie a `ProfitDLL64.dll` **e os arquivos que vêm junto com ela** para esta pasta
   (ou passe `--dll C:\caminho\ProfitDLL64.dll`).
3. Teste o pipeline sem conectar (usa DLL simulada):
   ```powershell
   py profitdll_feed.py --simulate --ticker WINZ26 --record-dir dados_teste
   ```
4. Credenciais (só nesta janela do PowerShell; se não definir, o script pergunta com a digitação oculta):
   ```powershell
   $env:PROFITDLL_KEY = 'sua-chave-de-ativacao'
   $env:PROFITDLL_USER = 'seu-usuario'
   $env:PROFITDLL_PASSWORD = 'sua-senha'
   ```
5. Pregão ao vivo em modo virtual (grava tudo em `dados\`):
   ```powershell
   py profitdll_feed.py --ticker WINZ26 --config exemplos\config_percentil.json
   ```
   Só gravar, sem estratégia: acrescente `--record-only`. Encerra sozinho às 17:05 (`--until`) ou com Ctrl+C.
6. Backtest dos pregões gravados:
   ```powershell
   py ofi_vpin_strategy.py --replay dados --config exemplos\config_percentil.json --output backtest.json
   ```
7. Otimização (com **pelo menos 15–20 pregões** gravados):
   ```powershell
   py optimize.py --replay dados --grid exemplos\grade.json --train-sessions 10
   ```

O WINZ26 vence em dezembro/2026; depois troque para `WING27`.

### Conferência no primeiro dia gravado

O mapeamento de agressor usa `nTradeType` 2 = compra, 3 = venda (manual da ProfitDLL).
Confira que a maioria dos negócios veio com 2 ou 3:

```powershell
py -c "import json,collections,glob; print(collections.Counter(json.loads(l).get('trade_type') for f in glob.glob('dados/*.jsonl') for l in open(f) if 'trade_type' in l))"
```

Se aparecerem outros códigos dominando, a regra de cotação (`infer_aggressor`) classifica os
negócios, mas avise para ajustar `TRADE_TYPE_AGGRESSOR` em `profitdll_feed.py`.

## Como ler os resultados

- `filter_funnel` (no backtest): quantas janelas de decisão cada filtro barrou. Mostra qual
  parâmetro está travando a estratégia.
- `optimize.py`: o número que vale é o **walk-forward fora da amostra**. O ranking in-sample
  sempre parece bom; se o fora da amostra for negativo, não há vantagem e mais ajuste só piora.

## Correções feitas na estratégia original

- Limite de perda diária era acumulado de todos os dias; agora é por pregão.
- Posição podia atravessar a noite se os dados acabassem antes das 17:00; agora zera na troca de pregão.
- `finish()` podia abrir operação nova no fim do replay; agora só encerra a posição aberta.
- Janela de decisão podia misturar o fim de um pregão com o começo do seguinte; agora reinicia.
- Stop saía no preço do stop mesmo com gap; agora sai no preço negociado (pior dos dois).
- Alvo pagava slippage de ordem a mercado; agora é limitada e exige o preço passar 1 tick além (fila).
- `max_feed_staleness_ms` existia mas não era usado; agora lacuna no feed descarta a janela.
- Volatilidade do stop era medida por evento (sempre ~1 tick, o multiplicador nunca agia); agora é por janela.
- Negócios sem agressor eram descartados do VPIN/delta; agora são classificados pela regra de cotação.
- Novo: `--config` para trocar parâmetros sem editar código, métricas (PF, drawdown, expectativa) e funil de filtros.
