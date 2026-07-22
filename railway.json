# Prospecta FIM — Backend API

Backend do sistema de gestão do Prospecta FIM.  
Núcleo de Finanças Insper.

## O que faz

- Busca preços automaticamente todo dia às 18h (Yahoo Finance + Tesouro Direto + BCB)
- Calcula a cota diária do fundo
- Salva tudo num banco PostgreSQL
- Expõe uma API REST que o Hub HTML consome em tempo real

## Endpoints principais

| Endpoint | O que retorna |
|---|---|
| `GET /api/metricas` | Todas as métricas: Sharpe, drawdown, alpha, VaR, retornos mensais |
| `GET /api/cotas` | Série histórica de cotas |
| `GET /api/cota/hoje` | Cota e PL do dia mais recente |
| `GET /api/carteira` | Carteira vigente com pesos |
| `GET /api/status` | Status do sistema |
| `POST /api/batimento` | Dispara batimento manual |
| `PUT /api/preco-manual` | Atualiza preço manual (Swedish Bond, Siemens Bond) |
| `PUT /api/cdi` | Atualiza CDI de um mês |

## Deploy no Railway

1. Cria conta em railway.app (via GitHub)
2. New Project → Deploy from GitHub repo
3. Seleciona este repositório
4. Adiciona plugin PostgreSQL
5. A variável DATABASE_URL é preenchida automaticamente

## Variáveis de ambiente

Apenas uma necessária (preenchida automaticamente pelo Railway):
- `DATABASE_URL` — string de conexão PostgreSQL
