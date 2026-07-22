"""
PROSPECTA FIM — Backend API
Núcleo de Finanças Insper

Deploy: Railway
Stack: FastAPI + PostgreSQL + APScheduler
Roda o batimento de cotas todo dia às 18h (horário de Brasília)
e expõe uma API REST que o Hub HTML consome.
"""

import os
import json
import logging
from datetime import date, datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd
import psycopg2
import requests
import yfinance as yf
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from psycopg2.extras import RealDictCursor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
DATABASE_URL = os.environ.get("DATABASE_URL", "")
PL_INICIAL   = 10_000_000.0
DATA_T0      = date(2025, 4, 30)
COTA_T0      = 1.0

# Tickers Yahoo Finance → nome interno
YAHOO_TICKERS = {
    "IVV":    "IVV",
    "IAU":    "IAU",
    "STIP":   "STIP",
    "URNM":   "URNM",
    "REMX":   "REMX",
    "CPER":   "CPER",
    "CORN":   "CORN",
    "CANE":   "CANE",
    "BTC-USD":"Bitcoin",
    "BRL=X":  "USDBRL",
    "EURUSD=X":"EURUSD",
    "BOVA11.SA":"BOVA11",
    "UTLL11.SA":"UTLL11",
    "RAIL3.SA": "RAIL3",
    "SMAL11.SA":"SMAL11",
}

# Carteiras históricas: (data, {ativo: peso})
CARTEIRAS = [
    (date(2025, 4, 30), {
        "LFT 2031":0.14,"NTN-B 2029":0.27,"IAU":0.12,"IVV":0.08
    }),
    (date(2025, 8, 29), {
        "LTN 2032":0.46,"IVV":0.304,"IAU":0.109
    }),
    (date(2025, 9, 30), {
        "LTN 2032":0.45,"BOVA11":0.135,"IAU":0.103,"IVV":0.06
    }),
    (date(2025, 10, 31), {
        "LTN 2032":0.47,"BOVA11":0.115,"IAU":0.075,"IVV":0.06,
        "URNM":0.06,"REMX":0.05,"Bitcoin":0.03,"CPER":0.02
    }),
    (date(2026, 1, 30), {
        "LTN 2032":0.50,"BOVA11":0.115,"IAU":0.075,"IVV":0.06,
        "URNM":0.06,"REMX":0.05,"Bitcoin":0.03,"CPER":0.02
    }),
    (date(2026, 3, 20), {
        "LTN 2032":0.25,"NTN-B 2040":0.13,"LFT 2031":0.15,
        "BOVA11":0.08,"IAU":0.08,"URNM":0.07,"IVV":0.06,
        "REMX":0.05,"STIP":0.03,"Bitcoin":0.03,"RAIL3":0.02,"CPER":0.02
    }),
    (date(2026, 4, 24), {
        "NTN-B 2040":0.185,"LFT 2031":0.15,"LTN 2032":0.13,
        "IVV":0.10,"STIP":0.07,"Swedish Gov Bond":0.05,"IAU":0.05,
        "Siemens Bond":0.04,"BOVA11":0.03,"UTLL11":0.03,
        "CPER":0.025,"URNM":0.02,"REMX":0.02,"RAIL3":0.02,
        "CORN":0.015,"CANE":0.01,"Bitcoin":0.01,
    }),
    (date(2026, 5, 29), {
        "LFT 2031":0.24,"NTN-B 2029":0.195,"NTN-B 2035":0.145,
        "STIP":0.07,"IAU":0.05,"Swedish Gov Bond":0.05,
        "Siemens Bond":0.04,"IVV":0.03,"BOVA11":0.03,"UTLL11":0.03,
        "CPER":0.025,"URNM":0.02,"REMX":0.02,"RAIL3":0.02,
        "CORN":0.015,"CANE":0.01,"Bitcoin":0.01,
    }),
]

# Futuros/derivativos
FUTUROS = {
    "EUR/BRL":  {"entrada": date(2026, 4, 24), "long": False, "notional": 0.08,  "preco_ref": "EURUSD"},
    "MXN/CAD":  {"entrada": date(2026, 4, 27), "long": True,  "notional": 0.03,  "preco_ref": "MXNCAD"},
    "SMAL11":   {"entrada": date(2026, 5, 29), "long": False, "notional": 0.025, "preco_ref": "SMAL11"},
}

# ─────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

def init_db():
    """Cria as tabelas se não existirem."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS cotas_diarias (
            id          SERIAL PRIMARY KEY,
            data        DATE UNIQUE NOT NULL,
            cota        NUMERIC(12,6) NOT NULL,
            pl          NUMERIC(18,2),
            retorno_dia NUMERIC(12,8),
            created_at  TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS precos_ativos (
            id         SERIAL PRIMARY KEY,
            data       DATE NOT NULL,
            ativo      VARCHAR(64) NOT NULL,
            preco      NUMERIC(18,6) NOT NULL,
            fonte      VARCHAR(32),
            created_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(data, ativo)
        );

        CREATE TABLE IF NOT EXISTS cdi_mensal (
            mes        VARCHAR(7) PRIMARY KEY,  -- formato YYYY-MM
            taxa       NUMERIC(8,6) NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS precos_manuais (
            ativo      VARCHAR(64) PRIMARY KEY,
            preco      NUMERIC(18,6) NOT NULL,
            data_ref   DATE NOT NULL,
            updated_at TIMESTAMP DEFAULT NOW()
        );
    """)
    conn.commit()
    cur.close()
    conn.close()
    logger.info("Database initialized")

    # Inserir CDI histórico se tabela estiver vazia
    _seed_cdi()

def _seed_cdi():
    """Insere o CDI histórico conhecido."""
    cdi_historico = {
        "2025-05": 0.01140, "2025-06": 0.01100, "2025-07": 0.01280,
        "2025-08": 0.01160, "2025-09": 0.01220, "2025-10": 0.01280,
        "2025-11": 0.01050, "2025-12": 0.01220, "2026-01": 0.01160,
        "2026-02": 0.01000, "2026-03": 0.01210, "2026-04": 0.01090,
        "2026-05": 0.01070,
    }
    conn = get_conn()
    cur = conn.cursor()
    for mes, taxa in cdi_historico.items():
        cur.execute("""
            INSERT INTO cdi_mensal (mes, taxa)
            VALUES (%s, %s)
            ON CONFLICT (mes) DO NOTHING
        """, (mes, taxa))
    conn.commit()
    cur.close()
    conn.close()

# ─────────────────────────────────────────────
# BUSCA DE PREÇOS
# ─────────────────────────────────────────────
def fetch_yahoo(tickers: list, data: date) -> dict:
    """Busca preços de fechamento do Yahoo Finance."""
    precos = {}
    try:
        start = data - timedelta(days=5)
        end   = data + timedelta(days=1)
        raw = yf.download(
            tickers, start=start.isoformat(), end=end.isoformat(),
            auto_adjust=True, progress=False, threads=True
        )
        if raw.empty:
            return precos

        close = raw["Close"] if len(tickers) > 1 else raw[["Close"]]
        close.columns = tickers if len(tickers) > 1 else tickers

        for t in tickers:
            if t in close.columns:
                series = close[t].dropna()
                if not series.empty:
                    precos[t] = float(series.iloc[-1])
    except Exception as e:
        logger.error(f"Yahoo error: {e}")
    return precos

def fetch_tesouro(data: date) -> dict:
    """Busca PU dos títulos do Tesouro Direto via API pública."""
    precos = {}
    try:
        url = "https://www.tesourodireto.com.br/json/br/com/b3/tesourodireto/model/dto/TesouroDiretoDto.json"
        r = requests.get(url, timeout=10)
        if r.status_code != 200:
            return precos
        dados = r.json()
        titulos = dados.get("response", {}).get("TrsrBdTradgList", [])
        mapa = {
            "Tesouro Selic 2031":   "LFT 2031",
            "Tesouro IPCA+ 2029":   "NTN-B 2029",
            "Tesouro IPCA+ 2035":   "NTN-B 2035",
            "Tesouro IPCA+ 2040":   "NTN-B 2040",
            "Tesouro Prefixado 2032":"LTN 2032",
        }
        for t in titulos:
            nome = t.get("TrsrBd", {}).get("nm", "")
            pu   = t.get("TrsrBd", {}).get("untrRedVal", None)
            for nome_td, nome_interno in mapa.items():
                if nome_td in nome and pu:
                    precos[nome_interno] = float(pu)
    except Exception as e:
        logger.error(f"Tesouro error: {e}")
    return precos

def fetch_bcb_cdi(mes: str) -> Optional[float]:
    """Busca CDI mensal do Banco Central. mes = 'YYYY-MM'"""
    try:
        ano, m = mes.split("-")
        url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.4391/dados?formato=json&dataInicial=01/{m}/{ano}&dataFinal=31/{m}/{ano}"
        r = requests.get(url, timeout=10)
        if r.status_code != 200:
            return None
        dados = r.json()
        if not dados:
            return None
        # CDI diário acumulado no mês
        taxa_acum = 1.0
        for d in dados:
            taxa_acum *= (1 + float(d["valor"]) / 100)
        return round(taxa_acum - 1, 8)
    except Exception as e:
        logger.error(f"BCB error: {e}")
        return None

def get_precos_manuais() -> dict:
    """Retorna últimos preços inseridos manualmente (Swedish Bond, Siemens Bond)."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT ativo, preco FROM precos_manuais")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {r["ativo"]: float(r["preco"]) for r in rows}

def get_ultimo_preco(ativo: str, antes_de: date) -> Optional[float]:
    """Busca o último preço registrado de um ativo antes de uma data."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT preco FROM precos_ativos
        WHERE ativo = %s AND data < %s
        ORDER BY data DESC LIMIT 1
    """, (ativo, antes_de))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return float(row["preco"]) if row else None

# ─────────────────────────────────────────────
# CARTEIRA
# ─────────────────────────────────────────────
def get_carteira_vigente(data: date) -> dict:
    carteira = {}
    for rebalance_date, pesos in CARTEIRAS:
        if rebalance_date <= data:
            carteira = pesos
    return carteira

def get_cota_anterior(data: date) -> tuple:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT data, cota, pl FROM cotas_diarias
        WHERE data < %s ORDER BY data DESC LIMIT 1
    """, (data,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    if row:
        return row["data"], float(row["cota"]), float(row["pl"] or PL_INICIAL)
    return DATA_T0, COTA_T0, PL_INICIAL

# ─────────────────────────────────────────────
# BATIMENTO PRINCIPAL
# ─────────────────────────────────────────────
def run_batimento(data: date = None):
    """
    Executa o batimento de cotas para uma data.
    Busca preços, calcula retorno ponderado, salva nova cota.
    """
    if data is None:
        data = date.today()

    logger.info(f"=== Batimento: {data} ===")

    # 1. Buscar preços automáticos
    yahoo_precos_raw = fetch_yahoo(list(YAHOO_TICKERS.keys()), data)
    yahoo_precos = {YAHOO_TICKERS[k]: v for k, v in yahoo_precos_raw.items() if k in YAHOO_TICKERS}

    tesouro_precos = fetch_tesouro(data)
    manuais = get_precos_manuais()

    precos_hoje = {**yahoo_precos, **tesouro_precos, **manuais}

    # 2. Salvar preços no banco
    conn = get_conn()
    cur = conn.cursor()
    for ativo, preco in precos_hoje.items():
        fonte = "yahoo" if ativo in yahoo_precos else ("tesouro" if ativo in tesouro_precos else "manual")
        cur.execute("""
            INSERT INTO precos_ativos (data, ativo, preco, fonte)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (data, ativo) DO UPDATE SET preco = EXCLUDED.preco
        """, (data, ativo, preco, fonte))
    conn.commit()

    # 3. Calcular retorno do dia
    carteira = get_carteira_vigente(data)
    data_ant, cota_ant, pl_ant = get_cota_anterior(data)

    # USD/BRL hoje e anterior (para converter ativos em USD)
    usdbrl_hoje = precos_hoje.get("USDBRL", 1.0)
    usdbrl_ant  = get_ultimo_preco("USDBRL", data) or usdbrl_hoje

    ativos_usd = {
        "IVV","IAU","STIP","URNM","REMX","CPER","CORN","CANE",
        "Bitcoin","Swedish Gov Bond","Siemens Bond"
    }

    retorno_dia = 0.0
    detalhes = []

    for ativo, peso in carteira.items():
        preco_hoje_a = precos_hoje.get(ativo)
        preco_ant_a  = get_ultimo_preco(ativo, data)

        if preco_hoje_a is None or preco_ant_a is None:
            logger.warning(f"  Sem preço para {ativo} — ignorado")
            continue

        if ativo in ativos_usd:
            # Converte variação para BRL
            val_hoje = preco_hoje_a * usdbrl_hoje
            val_ant  = preco_ant_a  * usdbrl_ant
            var = val_hoje / val_ant - 1 if val_ant > 0 else 0
        else:
            var = preco_hoje_a / preco_ant_a - 1 if preco_ant_a > 0 else 0

        contrib = peso * var
        retorno_dia += contrib
        detalhes.append({"ativo": ativo, "peso": peso, "var": var, "contrib": contrib})
        logger.info(f"  {ativo}: {var:.4%} × {peso:.1%} = {contrib:.4%}")

    # Derivativos
    for nome, cfg in FUTUROS.items():
        if cfg["entrada"] > data:
            continue
        ref = cfg["preco_ref"]
        ph = precos_hoje.get(ref)
        pa = get_ultimo_preco(ref, data)
        if ph and pa:
            var_fut = ph / pa - 1 if pa > 0 else 0
            if not cfg["long"]:
                var_fut = -var_fut
            contrib = cfg["notional"] * var_fut
            retorno_dia += contrib
            logger.info(f"  {nome}: {var_fut:.4%} × {cfg['notional']:.1%} = {contrib:.4%}")

    # 4. Nova cota e PL
    nova_cota = cota_ant * (1 + retorno_dia)
    novo_pl   = pl_ant   * (1 + retorno_dia)

    # 5. Salvar cota
    cur.execute("""
        INSERT INTO cotas_diarias (data, cota, pl, retorno_dia)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (data) DO UPDATE
        SET cota = EXCLUDED.cota, pl = EXCLUDED.pl, retorno_dia = EXCLUDED.retorno_dia
    """, (data, round(nova_cota, 6), round(novo_pl, 2), round(retorno_dia, 8)))
    conn.commit()
    cur.close()
    conn.close()

    logger.info(f"  Retorno: {retorno_dia:.4%} | Cota: {nova_cota:.6f} | PL: R$ {novo_pl:,.2f}")
    return {"data": str(data), "cota": nova_cota, "retorno": retorno_dia, "pl": novo_pl}

# ─────────────────────────────────────────────
# MÉTRICAS
# ─────────────────────────────────────────────
def calcular_metricas() -> dict:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT data, cota, retorno_dia, pl FROM cotas_diarias ORDER BY data")
    rows = cur.fetchall()

    cur.execute("SELECT mes, taxa FROM cdi_mensal ORDER BY mes")
    cdi_rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        return {}

    cotas   = [float(r["cota"]) for r in rows]
    rets    = [float(r["retorno_dia"]) for r in rows if r["retorno_dia"] is not None]
    datas   = [r["data"] for r in rows]
    pl_atual = float(rows[-1]["pl"] or PL_INICIAL)

    # CDI acumulado
    cdi_acum = 1.0
    for r in cdi_rows:
        cdi_acum *= (1 + float(r["taxa"]))
    cdi_acum -= 1

    ret_total = cotas[-1] / cotas[0] - 1
    n_anos    = len(cotas) / 252
    ret_anual = (1 + ret_total) ** (1 / max(n_anos, 0.01)) - 1
    cdi_anual = (1 + cdi_acum) ** (1 / max(n_anos, 0.01)) - 1
    vol_d     = float(np.std(rets)) if rets else 0
    vol_a     = vol_d * (252 ** 0.5)
    sharpe    = (ret_anual - cdi_anual) / vol_a if vol_a > 0 else 0

    downside  = float(np.std([r for r in rets if r < 0])) * (252 ** 0.5) if rets else 0
    sortino   = (ret_anual - cdi_anual) / downside if downside > 0 else 0

    # Drawdown
    running_max = cotas[0]
    max_dd = 0.0
    peak_date = datas[0]
    valley_date = datas[0]
    for i, c in enumerate(cotas):
        if c > running_max:
            running_max = c
            peak_date = datas[i]
        dd = (c - running_max) / running_max
        if dd < max_dd:
            max_dd = dd
            valley_date = datas[i]

    # VaR
    var95 = float(np.percentile(rets, 5)) if rets else 0
    var99 = float(np.percentile(rets, 1)) if rets else 0

    # Retornos mensais
    df = pd.DataFrame({"data": datas, "cota": cotas})
    df["data"] = pd.to_datetime(df["data"])
    df["mes"]  = df["data"].dt.to_period("M").astype(str)
    monthly = {}
    prev = df["cota"].iloc[0]
    for mes, g in df.groupby("mes"):
        g = g.sort_values("data")
        r = g["cota"].iloc[-1] / prev - 1
        monthly[mes] = round(r, 6)
        prev = g["cota"].iloc[-1]

    meses_pos = sum(1 for v in monthly.values() if v > 0)
    meses_neg = sum(1 for v in monthly.values() if v < 0)
    cdi_dict  = {r["mes"]: float(r["taxa"]) for r in cdi_rows}
    meses_acima_cdi = sum(
        1 for mes, v in monthly.items()
        if v > cdi_dict.get(mes, 0)
    )

    return {
        "ret_total":      round(ret_total, 6),
        "ret_anual":      round(ret_anual, 6),
        "cdi_acum":       round(cdi_acum, 6),
        "cdi_anual":      round(cdi_anual, 6),
        "alpha_total":    round(ret_total - cdi_acum, 6),
        "alpha_anual":    round(ret_anual - cdi_anual, 6),
        "vol_anual":      round(vol_a, 6),
        "sharpe":         round(sharpe, 4),
        "sortino":        round(sortino, 4),
        "max_dd":         round(max_dd, 6),
        "peak_date":      str(peak_date),
        "valley_date":    str(valley_date),
        "var95_d":        round(var95, 6),
        "var99_d":        round(var99, 6),
        "pl_atual":       round(pl_atual, 2),
        "cota_atual":     round(cotas[-1], 6),
        "cota_inicial":   round(cotas[0], 6),
        "data_inicio":    str(datas[0]),
        "data_atual":     str(datas[-1]),
        "meses_pos":      meses_pos,
        "meses_neg":      meses_neg,
        "meses_acima_cdi":meses_acima_cdi,
        "monthly":        monthly,
        "cdi_monthly":    cdi_dict,
    }

# ─────────────────────────────────────────────
# FASTAPI APP
# ─────────────────────────────────────────────
app = FastAPI(title="Prospecta FIM API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "PUT"],
    allow_headers=["*"],
)

@app.on_event("startup")
def startup():
    if DATABASE_URL:
        init_db()
        # Scheduler: roda batimento todo dia às 18h Brasília (UTC-3 = 21h UTC)
        scheduler = BackgroundScheduler(timezone="America/Sao_Paulo")
        scheduler.add_job(run_batimento, "cron", hour=18, minute=0)
        scheduler.start()
        logger.info("Scheduler started — batimento às 18h todo dia útil")
    else:
        logger.warning("DATABASE_URL não definida — rodando sem banco")

@app.get("/")
def root():
    return {"status": "ok", "fundo": "Prospecta FIM", "version": "1.0.0"}

@app.get("/api/metricas")
def get_metricas():
    """Retorna todas as métricas calculadas do fundo."""
    try:
        return calcular_metricas()
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/api/cotas")
def get_cotas(limit: int = 500):
    """Retorna a série histórica de cotas."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT data, cota, pl, retorno_dia
        FROM cotas_diarias
        ORDER BY data DESC LIMIT %s
    """, (limit,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [dict(r) for r in rows]

@app.get("/api/cota/hoje")
def get_cota_hoje():
    """Retorna a cota mais recente."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT data, cota, pl, retorno_dia
        FROM cotas_diarias ORDER BY data DESC LIMIT 1
    """)
    row = cur.fetchone()
    cur.close()
    conn.close()
    if not row:
        raise HTTPException(404, "Nenhuma cota encontrada")
    return dict(row)

@app.get("/api/precos/{ativo}")
def get_precos_ativo(ativo: str, limit: int = 252):
    """Retorna histórico de preços de um ativo."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT data, preco, fonte FROM precos_ativos
        WHERE ativo = %s ORDER BY data DESC LIMIT %s
    """, (ativo, limit))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [dict(r) for r in rows]

@app.post("/api/batimento")
def trigger_batimento(data_str: Optional[str] = None):
    """Dispara o batimento manualmente. Opcional: data no formato YYYY-MM-DD."""
    try:
        data = date.fromisoformat(data_str) if data_str else date.today()
        resultado = run_batimento(data)
        return resultado
    except Exception as e:
        raise HTTPException(500, str(e))

@app.put("/api/preco-manual")
def upsert_preco_manual(ativo: str, preco: float, data_ref: str):
    """
    Atualiza preço manual de um ativo (Swedish Bond, Siemens Bond, etc).
    Chamado pelo Hub quando você insere um preço manualmente.
    """
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO precos_manuais (ativo, preco, data_ref)
        VALUES (%s, %s, %s)
        ON CONFLICT (ativo) DO UPDATE
        SET preco = EXCLUDED.preco, data_ref = EXCLUDED.data_ref, updated_at = NOW()
    """, (ativo, preco, date.fromisoformat(data_ref)))
    conn.commit()
    cur.close()
    conn.close()
    return {"ok": True, "ativo": ativo, "preco": preco}

@app.put("/api/cdi")
def upsert_cdi(mes: str, taxa: float):
    """
    Atualiza o CDI de um mês. mes = 'YYYY-MM', taxa = decimal (ex: 0.0107)
    Chamado automaticamente pelo scheduler ou manualmente pelo Hub.
    """
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO cdi_mensal (mes, taxa)
        VALUES (%s, %s)
        ON CONFLICT (mes) DO UPDATE SET taxa = EXCLUDED.taxa
    """, (mes, taxa))
    conn.commit()
    cur.close()
    conn.close()

    # Tenta buscar do BCB automaticamente
    taxa_bcb = fetch_bcb_cdi(mes)
    if taxa_bcb:
        cur2 = conn.cursor() if not conn.closed else get_conn().cursor()
        conn2 = get_conn()
        cur2 = conn2.cursor()
        cur2.execute("""
            INSERT INTO cdi_mensal (mes, taxa)
            VALUES (%s, %s)
            ON CONFLICT (mes) DO UPDATE SET taxa = EXCLUDED.taxa
        """, (mes, taxa_bcb))
        conn2.commit()
        cur2.close()
        conn2.close()
        return {"ok": True, "mes": mes, "taxa": taxa_bcb, "fonte": "BCB"}

    return {"ok": True, "mes": mes, "taxa": taxa, "fonte": "manual"}

@app.get("/api/cdi")
def get_cdi():
    """Retorna todos os CDIs registrados."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT mes, taxa FROM cdi_mensal ORDER BY mes")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [dict(r) for r in rows]

@app.get("/api/carteira")
def get_carteira_atual():
    """Retorna a carteira vigente com pesos."""
    hoje = date.today()
    carteira = get_carteira_vigente(hoje)
    return {
        "data": str(hoje),
        "posicoes": [{"ativo": k, "peso": v} for k, v in carteira.items()],
        "derivativos": [
            {
                "nome": k,
                "long": v["long"],
                "notional": v["notional"],
                "entrada": str(v["entrada"])
            }
            for k, v in FUTUROS.items()
            if v["entrada"] <= hoje
        ]
    }

@app.get("/api/status")
def get_status():
    """Retorna status do sistema e última atualização."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT data, cota FROM cotas_diarias ORDER BY data DESC LIMIT 1")
    ultima = cur.fetchone()
    cur.execute("SELECT COUNT(*) as total FROM cotas_diarias")
    total = cur.fetchone()
    cur.close()
    conn.close()
    return {
        "status": "ok",
        "ultima_cota": dict(ultima) if ultima else None,
        "total_dias": int(total["total"]) if total else 0,
        "timestamp": datetime.now().isoformat(),
    }
