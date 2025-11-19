# services/camara.py
#
# Camada de acesso aos Dados Abertos da Câmara dos Deputados
# para o app LegiTrack BR.

from typing import Optional, List, Dict, Any
import requests

BASE_API = "https://dadosabertos.camara.leg.br/api/v2"
BASE_ARQUIVOS = "https://dadosabertos.camara.leg.br/arquivos/proposicoes/json"


class CamaraAPIError(RuntimeError):
    pass


def _get_api(path: str, params: Optional[dict] = None, timeout: int = 25) -> dict:
    """
    Chamada genérica para a API REST (/api/v2/...).
    """
    url = f"{BASE_API}{path}"
    try:
        r = requests.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        raise CamaraAPIError(f"Erro ao consultar a API da Câmara ({url}): {e}") from e


def _get_arquivo_proposicoes_ano(ano: int, timeout: int = 40) -> List[Dict[str, Any]]:
    """
    Baixa o arquivo JSON de proposições de um determinado ano:
    https://dadosabertos.camara.leg.br/arquivos/proposicoes/json/proposicoes-{ano}.json
    """
    url = f"{BASE_ARQUIVOS}/proposicoes-{ano}.json"
    try:
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        data = r.json()

        if isinstance(data, dict):
            if "dados" in data and isinstance(data["dados"], list):
                return data["dados"]
            for key in ("proposicoes", "lista", "itens"):
                if key in data and isinstance(data[key], list):
                    return data[key]
            return [data]
        elif isinstance(data, list):
            return data
        else:
            return []
    except requests.RequestException as e:
        raise CamaraAPIError(
            f"Erro ao baixar arquivo de proposições de {ano} ({url}): {e}"
        ) from e


def buscar_proposicoes_por_tema(
    termo: str,
    ano: int,
    tipos: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """
    Busca proposições de um ano específico, filtrando por:
      - tipos (PL, PEC, PLP, MPV, PDC etc.)
      - presença do termo na ementa, keywords ou ementaDetalhada.
    """
    if not termo:
        raise ValueError("O termo de busca não pode ser vazio.")

    registros = _get_arquivo_proposicoes_ano(ano)

    termo_lower = termo.lower().strip()
    tipos = [t.upper() for t in (tipos or [])]

    filtradas: List[Dict[str, Any]] = []
    for prop in registros:
        sigla_tipo = str(prop.get("siglaTipo", "")).upper()

        if tipos and sigla_tipo not in tipos:
            continue

        ementa = str(prop.get("ementa", "") or "")
        keywords = str(prop.get("keywords", "") or "")
        resumo = str(prop.get("ementaDetalhada", "") or "")

        texto_busca = " ".join([ementa, keywords, resumo]).lower()

        if termo_lower in texto_busca:
            filtradas.append(prop)

    try:
        filtradas.sort(
            key=lambda p: (int(p.get("ano", 0)), int(p.get("numero", 0))),
            reverse=True,
        )
    except Exception:
        pass

    return filtradas


def detalhes_proposicao(id_prop: int) -> Dict[str, Any]:
    data = _get_api(f"/proposicoes/{id_prop}")
    return data.get("dados", {})


def tramitacoes(id_prop: int) -> List[Dict[str, Any]]:
    data = _get_api(f"/proposicoes/{id_prop}/tramitacoes")
    return data.get("dados", [])


def autores_por_proposicao(id_prop: int) -> List[Dict[str, Any]]:
    """
    Busca autores em /proposicoes/{id}/autores.
    """
    data = _get_api(f"/proposicoes/{id_prop}/autores")
    if isinstance(data, dict):
        return data.get("dados", [])
    elif isinstance(data, list):
        return data
    return []


def autores_por_uri(uri_autores: str) -> List[Dict[str, Any]]:
    """
    Mantido como fallback, caso algum registro traga diretamente a URI de autores.
    """
    if not uri_autores:
        return []
    try:
        r = requests.get(uri_autores, timeout=25)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, dict):
            return data.get("dados", [])
        elif isinstance(data, list):
            return data
        else:
            return []
    except requests.RequestException:
        return []
