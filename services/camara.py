# services/camara.py
# Camada de acesso à API de Dados Abertos da Câmara dos Deputados
# Docs: https://dadosabertos.camara.leg.br/swagger/api.html

from typing import Optional, List, Dict, Any
import requests

BASE = "https://dadosabertos.camara.leg.br/api/v2"

class CamaraAPIError(RuntimeError):
    pass

def _get(url: str, params: Optional[dict] = None, timeout: int = 25) -> dict:
    try:
        r = requests.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        raise CamaraAPIError(f"Erro ao consultar {url}: {e}") from e

def buscar_proposicoes(termo: str,
                       ano: Optional[int] = None,
                       tipo: str = "PL",
                       itens: int = 100,
                       ordenar_por: str = "ano",
                       ordem: str = "DESC") -> List[Dict[str, Any]]:
    """
    Busca proposições pela ementa (palavra-chave), filtrando por tipo e ano.
    Params:
      - termo: palavra-chave na ementa
      - ano: (opcional) ano da proposição
      - tipo: sigla do tipo (PL, PLP, PEC, etc.)
      - itens: quantidade de itens por página (máx ~100)
    """
    params = {
        "ementa": termo,
        "siglaTipo": tipo,
        "itens": itens,
        "ordem": ordem,
        "ordenarPor": ordenar_por,
    }
    if ano:
        params["ano"] = ano
    data = _get(f"{BASE}/proposicoes", params=params)
    return data.get("dados", [])

def detalhes_proposicao(id_prop: int) -> Dict[str, Any]:
    data = _get(f"{BASE}/proposicoes/{id_prop}")
    return data.get("dados", {})

def tramitacoes(id_prop: int) -> List[Dict[str, Any]]:
    data = _get(f"{BASE}/proposicoes/{id_prop}/tramitacoes")
    return data.get("dados", [])

def autores_por_uri(uri_autores: str) -> List[Dict[str, Any]]:
    if not uri_autores:
        return []
    data = _get(uri_autores)
    return data.get("dados", [])

