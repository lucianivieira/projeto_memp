# 📊 Panorama de Participação do MEI nas Compras Públicas Federais (PNCP)

Este projeto constrói um **panorama quantitativo da participação de Microempreendedores Individuais (MEI)** nas compras públicas federais brasileiras, utilizando dados abertos da Receita Federal (CNPJ/Simples) e da API oficial do PNCP (Portal Nacional de Contratações Públicas).

---

## 🎯 Objetivo

Medir:

* Quantos contratos federais foram firmados com MEIs
* Qual a participação percentual dos MEIs:

  * por **quantidade de contratos**
  * por **valor financeiro**
* Perfil geográfico (UF)
* Perfil econômico (CNAE)
* Evolução temporal (série diária)
* Principais órgãos compradores

---

## 🗂 Fontes de Dados

### 1) Receita Federal – Cadastro CNPJ (dados abertos)

Fonte:
[https://dadosabertos.rfb.gov.br/CNPJ/](https://dadosabertos.rfb.gov.br/CNPJ/)

Usamos os arquivos mensais mais recentes contendo:

* **EMPRESAS** → razão social
* **ESTABELECIMENTOS** → UF, município, situação, CNAE
* **SIMPLES** → opção pelo MEI

Esses arquivos vêm **sem cabeçalho** e são massivos (dezenas de milhões de linhas).

---

### 2) PNCP – Portal Nacional de Contratações Públicas

API oficial (Swagger):
[https://pncp.gov.br/api/consulta/swagger-ui/index.html#/](https://pncp.gov.br/api/consulta/swagger-ui/index.html#/)

Endpoint utilizado:

```
GET /v1/contratos
```

Campos-chave:

* `dataPublicacaoPncp`
* `niFornecedor`
* `tipoPessoa` (PJ / PF)
* `valorGlobal`
* `orgaoEntidade`
* `unidadeOrgao`

---

## 🧠 Lógica Geral do Pipeline

O pipeline foi dividido em **etapas independentes**, para evitar reprocessamentos desnecessários.

---

## Etapa 1 — Ingestão da base CNPJ (RFB)

Arquivos:

* `import_empresas.py`
* `import_estabelecimentos.py`
* `import_simples.py`

Problemas resolvidos:

* Codificação inconsistente (`utf-8`, `latin-1`)
* Linhas gigantes
* Falhas de inferência de tipo
* Arquivos sem header

Solução:

* Importação com `header=false`
* Mapeamento por índice
* Uso de `DuckDB` para suportar volume massivo

Resultado:

Tabelas no DuckDB:

* `empresas`
* `estabelecimentos`
* `simples`

---

## Etapa 2 — Construção da base de MEI ativo

Script:

* `create_mei_ativo.py`

Lógica:

```
MEI ativo =
  empresas
  ⨝ estabelecimentos
  ⨝ simples
  onde:
    - opção MEI = S
    - situação cadastral = ativa
```

Resultado:

Tabela:

```
mei_ativo
```

Com colunas:

* CNPJ (14 dígitos)
* Razão social
* UF
* Município
* CNAE principal
* Situação
* Flag MEI

---

## Etapa 3 — Extração dos contratos federais do PNCP

Script:

* `pncp_mei_federal_pipeline.py`

Lógica:

* Consulta paginada da API `/v1/contratos`
* Janela: últimos 6 meses
* Tamanho da página: 500 (máximo permitido)
* Salvamento incremental em JSONL

Resultado:

Arquivo:

```
pncp_contratos_6m.jsonl
```

---

## Etapa 4 — Importação do JSONL para DuckDB

Script:

* `pncp_join_mei_federal_from_jsonl.py`

Lógica:

1. Carrega JSONL no DuckDB
2. Filtra contratos federais
3. Normaliza CNPJ (`niFornecedor`)
4. Cruza com `mei_ativo`
5. Gera KPIs

Tabelas criadas:

* `pncp_contratos_raw`
* `pncp_contratos_federal_6m`
* `pncp_mei_federal_6m`

---

## Etapa 5 — Geração de KPIs

Tabelas finais:

* `kpi_mei_participacao_federal_6m`
* `kpi_mei_top_uf_federal_6m`
* `kpi_mei_top_cnae_federal_6m`
* `kpi_mei_top_orgaos_federal_6m`
* `kpi_mei_serie_diaria_federal_6m`

---

## Etapa 6 — Visualização

Script:

* `plot_kpis_mei_pncp.py`

Gera:

* Gráficos PNG
* Relatório HTML

Ajustes de usabilidade:

* Gráfico horizontal para rótulos longos
* Texto explícito: **"em milhões"**
* Layout para público leigo
* Sem notação científica ambígua

---

## 📁 Estrutura do Projeto

```
Projeto MEMP/
│
├── rf_cnpj_csv/
│
├── cnpj_2026_01.duckdb
│
├── import_empresas.py
├── import_estabelecimentos.py
├── import_simples.py
├── create_mei_ativo.py
│
├── pncp_contratos_6m.jsonl
├── pncp_mei_federal_pipeline.py
├── pncp_join_mei_federal_from_jsonl.py
│
├── plot_kpis_mei_pncp.py
│
└── out_charts/
    ├── *.png
    └── relatorio_mei_pncp.html
```

---

## 📈 KPIs Calculados

| KPI                         | Descrição                               |
| --------------------------- | --------------------------------------- |
| Participação por quantidade | % de contratos com MEI                  |
| Participação por valor      | % do valor total contratado com MEI     |
| Top UF                      | Estados com maior volume MEI            |
| Top CNAE                    | Atividades mais contratadas             |
| Top órgãos                  | Órgãos federais que mais compram de MEI |
| Série temporal              | Evolução diária                         |

---

## ⚠️ Limitações

* Contratos com fornecedor PF não entram (MEI é PJ)
* Contratos sem CNPJ válido são descartados
* Algumas compras pequenas não passam pelo PNCP
* Dados dependem da qualidade do preenchimento dos órgãos

---

## 🚀 Reprodutibilidade

Ordem de execução:

```bash
python import_empresas.py
python import_estabelecimentos.py
python import_simples.py
python create_mei_ativo.py

python pncp_mei_federal_pipeline.py
python pncp_join_mei_federal_from_jsonl.py

python plot_kpis_mei_pncp.py
```

---

## 📌 Observação Final

Este projeto foi desenhado para **escala nacional**, com centenas de milhões de registros, priorizando:

* Robustez
* Reprodutibilidade
* Transparência metodológica
* Visualização compreensível para público não técnico

---
