# utils/transforms.py
#
# Funções utilitárias para transformar os dados de proposições da Câmara
# em DataFrames prontos para análise no app LegiTrack BR.

from __future__ import annotations
from typing import Any, Dict, List, Optional
from dataclasses import dataclass
from datetime import datetime, date

import pandas as pd
from dateutil import parser as dateparser


# ---------------------------------------------------------
# Conversão de datas
# ---------------------------------------------------------
def parse_date(value: Any) -> Optional[pd.Timestamp]:
    """Converte strings da API em Timestamp do pandas."""
    if value in (None, "", pd.NaT):
        return None
    try:
        dt = dateparser.parse(str(value))
        return pd.to_datetime(dt)
    except Exception:
        return None


def dias_desde(dt: Any) -> Optional[int]:
    """Calcula dias desde dt até hoje."""
    if dt in (None, "", pd.NaT):
        return None
    if isinstance(dt, pd.Timestamp):
        d = dt.date()
    elif isinstance(dt, datetime):
        d = dt.date()
    elif isinstance(dt, date):
        d = d
    else:
        parsed = parse_date(dt)
        if parsed is None:
            return None
        d = parsed.date()

    hoje = date.today()
    return (hoje - d).days


# ---------------------------------------------------------
# Auxiliar seguro para buscar campos aninhados
# ---------------------------------------------------------
def _safe_get(d: Dict[str, Any], *keys, default=None) -> Any:
    """Busca d[key1][key2]... com fallback."""
    cur = d
    try:
        for k in keys:
            if cur is None:
                return default
            cur = cur.get(k)
        return cur if cur is not None else default
    except Exception:
        return default


# ---------------------------------------------------------
# DataFrame principal de proposições
# ---------------------------------------------------------
def df_proposicoes(registros: List[Dict[str, Any]]) -> pd.DataFrame:
    """Transforma lista de proposições em DataFrame padronizado."""
    linhas: List[Dict[str, Any]] = []

    for p in registros:
        id_prop = p.get("id") or p.get("idProposicao")
        sigla_tipo = p.get("siglaTipo") or p.get("sigla_tipo")
        numero = p.get("numero") or p.get("numProposicao") or p.get("num")
        ano = p.get("ano") or p.get("anoProposicao")

        ementa = p.get("ementa") or ""
        ementa_det = p.get("ementaDetalhada") or ""

        # status pode vir em diversos formatos
        status = (
            p.get("statusProposicao")
            or p.get("ultimoStatus")
            or p.get("status_proposicao")
            or {}
        )

        situacao = (
            _safe_get(status, "descricaoSituacao")
            or _safe_get(status, "situacao")
            or _safe_get(status, "descricaoTramitacao")
        )

        tramitacao_atual = (
            _safe_get(status, "descricaoTramitacao")
            or _safe_get(status, "apreciacao")
        )

        data_status_raw = (
            _safe_get(status, "dataHora")
            or _safe_get(status, "dataUltimoDespacho")
            or _safe_get(status, "data")
        )

        data_status = parse_date(data_status_raw)

        # link oficial
        if id_prop:
            link = f"https://www.camara.leg.br/proposicoesWeb/fichadetramitacao?idProposicao={id_prop}"
        else:
            link = ""

        rotulo = None
        if sigla_tipo and numero and ano:
            rotulo = f"{sigla_tipo} {numero}/{ano}"

        linhas.append(
            {
                "id": id_prop,
                "siglaTipo": sigla_tipo,
                "numero": numero,
                "ano": ano,
                "rotulo": rotulo,
                "ementa": ementa or ementa_det,
                "situacao": situacao,
                "tramitacao_atual": tramitacao_atual,
                "data_status": data_status,
                "link": link,
            }
        )

    if not linhas:
        return pd.DataFrame(
            columns=[
                "id",
                "siglaTipo",
                "numero",
                "ano",
                "rotulo",
                "ementa",
                "situacao",
                "tramitacao_atual",
                "data_status",
                "link",
            ]
        )

    df = pd.DataFrame(linhas)
    return df


# ---------------------------------------------------------
# Extrai o nome do autor principal
# ---------------------------------------------------------
def extrair_autor_principal(autores_payload: List[Dict[str, Any]]) -> Optional[str]:
    """
    Extrai o NOME do autor principal a partir de /proposicoes/{id}/autores.

    - Prioriza autores do tipo 'Deputado'.
    - Se não houver, pega o primeiro da lista.
    - Não tenta extrair partido/UF porque a API é extremamente inconsistente.
    """
    if not autores_payload:
        return None

    deputado = None
    for a in autores_payload:
        tipo = (a.get("tipo") or "").lower()
        if "deputado" in tipo:
            deputado = a
            break

    autor = deputado or autores_payload[0]

    nome = (
        autor.get("nome")
        or autor.get("nomeAutor")
        or autor.get("nomeAutorPrimeiroSignatario")
    )

    return nome
