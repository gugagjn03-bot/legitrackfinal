# services/camara.py
# Camada de acesso aos Dados Abertos da Câmara dos Deputados
#
# Aqui usamos DOIS tipos de fonte:
# 1) Arquivo anual de proposições em JSON (para busca por tema/ementa/keywords)
# 2) Endpoints /proposicoes/{id} e /proposicoes/{id}/tramitacoes para detalhes e tramitação.

from typing import Optional, List, Dict, Any
import requests

# Base da API REST
BASE_API = "https://dadosabertos.camara.leg.br/api/v2"
# Base dos arquivos anuais (proposicoes-{ano}.json)
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

    Esse arquivo contém:
      - siglaTipo, numero, ano, ementa, keywords, temas
      - statusProposicao, uri, etc.
    """
    url = f"{BASE_ARQUIVOS}/proposicoes-{ano}.json"
    try:
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        data = r.json()

        # Alguns arquivos vêm como {"dados": [...]} ; outros podem vir diretamente como lista
        if isinstance(data, dict):
            if "dados" in data and isinstance(data["dados"], list):
                return data["dados"]
            # fallback: se não tiver "dados", mas tiver "proposicoes" ou algo assim
            for key in ("proposicoes", "lista", "itens"):
                if key in data and isinstance(data[key], list):
                    return data[key]
            # se nada disso, talvez o próprio dict seja um único registro
            return [data]
        elif isinstance(data, list):
            return data
        else:
            return []
    except requests.RequestException as e:
        raise CamaraAPIError(f"Erro ao baixar arquivo de proposições de {ano} ({url}): {e}") from e


def buscar_proposicoes_por_tema(
    termo: str,
    ano: int,
    tipos: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """
    Busca proposições de um ano específico, filtrando por:
      - tipos (PL, PEC, PLP, MPV, PDC etc.)
      - e presença do termo na ementa OU em keywords.

    Isso garante maior especificidade temática.
    """
    if not termo:
        raise ValueError("O termo de busca não pode ser vazio.")

    # 1) Baixa todas as proposições do ano
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

    # Ordena por ano + número (só pra ficar bonitinho)
    try:
        filtradas.sort(key=lambda p: (int(p.get("ano", 0)), int(p.get("numero", 0))), reverse=True)
    except Exception:
        pass

    return filtradas


def detalhes_proposicao(id_prop: int) -> Dict[str, Any]:
    data = _get_api(f"/proposicoes/{id_prop}")
    return data.get("dados", {})


def tramitacoes(id_prop: int) -> List[Dict[str, Any]]:
    data = _get_api(f"/proposicoes/{id_prop}/tramitacoes")
    return data.get("dados", [])


def autores_por_uri(uri_autores: str) -> List[Dict[str, Any]]:
    """
    uri_autores já vem completa (https://dadosabertos...), então aqui fazemos
    um GET direto sem BASE_API.
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
