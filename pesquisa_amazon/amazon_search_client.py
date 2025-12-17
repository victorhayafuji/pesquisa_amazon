"""Cliente SearchAPI para Amazon Search.

Documentação: https://www.searchapi.io/docs/amazon-search
Endpoint: GET /api/v1/search?engine=amazon_search
Parâmetros principais:
- q (obrigatório)
- amazon_domain (opcional)
- language (opcional)
- rh (opcional, filtros)

Nota de robustez:
- O schema do JSON pode variar; por isso o parsing é feito no processador com fallback.
"""

from __future__ import annotations

import os
import requests
from dotenv import load_dotenv

from config_amazon import SEARCHAPI_BASE_URL, ENGINE_AMAZON_SEARCH, AMAZON_DOMAIN_DEFAULT, LANGUAGE_DEFAULT


class AmazonSearchError(Exception):
    pass


def buscar_amazon_search(
    q: str,
    page: int | None = None,
    amazon_domain: str | None = None,
    language: str | None = None,
    rh: str | None = None,
    timeout: int = 30,
) -> dict:
    """Consulta a SearchAPI (engine amazon_search) e retorna o JSON bruto."""
    load_dotenv()
    api_key = os.getenv("SEARCHAPI_API_KEY")
    if not api_key:
        raise AmazonSearchError("SEARCHAPI_API_KEY não encontrada. Configure no .env (ou variável de ambiente).")

    params = {
        "engine": ENGINE_AMAZON_SEARCH,
        "q": q,
        "api_key": api_key,
    }

    # Localização/idioma (conforme doc)
    params["amazon_domain"] = amazon_domain or AMAZON_DOMAIN_DEFAULT
    params["language"] = language or LANGUAGE_DEFAULT

    # Paginação (mantida como parâmetro do projeto; se o provedor não suportar, ele ignora ou retorna erro)
    if page is not None:
        params["page"] = int(page)

    # Filtro de resultados (conforme doc)
    if rh:
        params["rh"] = rh

    try:
        resp = requests.get(SEARCHAPI_BASE_URL, params=params, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except requests.HTTPError as e:
        raise AmazonSearchError(f"Erro HTTP ao consultar SearchAPI (amazon_search): {e} | URL: {getattr(resp, 'url', '')}")
    except requests.RequestException as e:
        raise AmazonSearchError(f"Erro de rede ao consultar SearchAPI (amazon_search): {e}")
    except ValueError as e:
        raise AmazonSearchError(f"Falha ao decodificar JSON da SearchAPI (amazon_search): {e}")
