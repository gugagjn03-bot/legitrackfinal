# utils/transforms.py
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone
from dateutil import parser as dtparser
import pandas as pd

def parse_date(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        # aceita ISO, com/sem Z
        return dtparser.isoparse(s).astimezone(timezone.utc)
    except Exception:
        try:
            return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except Exception:
            return None

def dias_desde(d: Optional[datetime]) -> Optional[int]:
    if not d:
        return None
    return int((datetime.now(timezone.utc) - d).days)

def df_proposicoes(dados: List[Dict[str, Any]]) -> pd.DataFrame:
    if not dados:
        return pd.DataFrame()
    df = pd.json_normalize(dados)
    keep = [
        "id", "idTipo", "siglaTipo", "numero", "ano",
        "ementa", "ementaDetalhada",
        "statusProposicao.descricaoTramitacao",
        "statusProposicao.dataHora",
        "statusProposicao.siglaOrgao",
        "statusProposicao.descricaoSituacao",
        "statusProposicao.despacho",
        "uriAutores", "uri"
    ]
    for k in keep:
        if k not in df.columns:
            df[k] = None
    df = df[keep].copy()
    df.rename(columns={
        "statusProposicao.descricaoTramitacao": "tramitacao_atual",
        "statusProposicao.dataHora": "data_status",
        "statusProposicao.siglaOrgao": "orgao_atual",
        "statusProposicao.descricaoSituacao": "situacao",
        "statusProposicao.despacho": "despacho"
    }, inplace=True)

    df["data_status"] = df["data_status"].apply(parse_date)
    # link humano: troca 'api.' e remove '/v2'
    df["link"] = df["uri"].astype(str).str.replace("api.", "", regex=False).str.replace("/v2", "", regex=False)
    df["rotulo"] = df["siglaTipo"].fillna("") + " " + df["numero"].astype(str) + "/" + df["ano"].astype(str)
    return df

def extrair_autor_principal(autores_payload: List[Dict[str, Any]]) -> Dict[str, Optional[str]]:
    if not autores_payload:
        return {"nome": None, "partido": None, "uf": None, "tipoAutor": None}
    a = autores_payload[0].get("autor", {})
    return {
        "nome": a.get("nome"),
        "partido": a.get("siglaPartido"),
        "uf": a.get("siglaUf"),
        "tipoAutor": autores_payload[0].get("tipoAutor")
    }

