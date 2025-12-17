"""Processador de resultados do Amazon Search (SearchAPI).

Estratégia:
- Tentar chaves comuns de lista de resultados (sem assumir um schema único).
- Fallback: procurar a primeira lista de dicionários que contenha campos típicos (title/name e price).
- Extrair campos padronizados para CSV (padrão do projeto).

Importante:
- Não mistura 'seller' com 'marca'. Aqui, marca não é inferida.
"""

from __future__ import annotations

import re
from typing import Any


_PRICE_RE = re.compile(r"(\d+[\d\.,]*)")


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip()
    m = _PRICE_RE.search(s)
    if not m:
        return None
    num = m.group(1)
    # normaliza: '1.234,56' -> '1234.56' ; '1,234.56' -> '1234.56'
    if "," in num and "." in num:
        # se o último separador for vírgula, assume formato pt-BR
        if num.rfind(",") > num.rfind("."):
            num = num.replace(".", "").replace(",", ".")
        else:
            num = num.replace(",", "")
    else:
        num = num.replace(".", "").replace(",", ".") if num.count(",") == 1 and num.count(".") >= 1 else num.replace(",", ".")
    try:
        return float(num)
    except ValueError:
        return None


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    s = str(value)
    digits = re.sub(r"\D", "", s)
    if not digits:
        return None
    try:
        return int(digits)
    except ValueError:
        return None


def _pick(d: dict, keys: list[str]) -> Any:
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return None


def _extract_price_fields(item: dict) -> tuple[float | None, str | None]:
    """Extrai preço (float) e uma string de preço (raw) com fallback."""
    price_obj = _pick(item, ["price", "price_total", "price_value", "current_price", "final_price"])
    if isinstance(price_obj, dict):
        raw = _pick(price_obj, ["raw", "text", "value", "display", "symbol"])
        val = _to_float(_pick(price_obj, ["value", "raw", "text", "amount"]))
        return val, str(raw) if raw is not None else None
    return _to_float(price_obj), str(price_obj) if price_obj is not None else None


def _find_results_list(resposta_json: dict) -> list[dict]:
    candidates = [
        "search_results",
        "organic_results",
        "results",
        "products",
        "product_results",
        "amazon_results",
    ]
    for k in candidates:
        v = resposta_json.get(k)
        if isinstance(v, list) and v and all(isinstance(x, dict) for x in v):
            return v

    # fallback: primeira lista de dicts com cara de resultado
    for v in resposta_json.values():
        if isinstance(v, list) and v and all(isinstance(x, dict) for x in v):
            # heurística: precisa ter title/name em pelo menos 30% e algum campo de preço em pelo menos 10%
            titles = sum(1 for x in v if _pick(x, ["title", "name", "product_title"]))
            prices = sum(1 for x in v if _pick(x, ["price", "current_price", "final_price"]))
            if titles / max(len(v), 1) >= 0.3 and prices / max(len(v), 1) >= 0.1:
                return v

    return []


def extrair_resultados_amazon(
    resposta_json: dict,
    palavra_chave: str,
    fonte: str = "amazon_search",
) -> list[dict]:
    """Extrai resultados em formato tabular para CSV."""
    resultados = []
    itens = _find_results_list(resposta_json)

    for idx, item in enumerate(itens, start=1):
        titulo = _pick(item, ["title", "name", "product_title", "product"])
        preco, preco_raw = _extract_price_fields(item)

        # links e ids
        link = _pick(item, ["link", "url", "product_link", "product_url"])
        asin = _pick(item, ["asin", "product_id", "id"])

        # seller/merchant (quando disponível)
        seller = _pick(item, ["seller", "merchant", "store", "seller_name", "sold_by"])

        # reputação
        rating = _to_float(_pick(item, ["rating", "stars", "avg_rating"]))
        reviews = _to_int(_pick(item, ["reviews", "review_count", "ratings_total", "total_reviews"]))

        patrocinado = _pick(item, ["sponsored", "is_sponsored", "ad", "ads"])
        if isinstance(patrocinado, str):
            patrocinado = patrocinado.lower() in ("true", "yes", "1", "sim", "sponsored", "patrocinado")
        elif patrocinado is None:
            patrocinado = False

        resultados.append(
            {
                "fonte": fonte,
                "palavra_chave": palavra_chave,
                "produto": titulo,
                "preco": preco,
                "preco_raw": preco_raw,
                "seller": seller,
                "asin": asin,
                "rating": rating,
                "reviews": reviews,
                "patrocinado": bool(patrocinado),
                "posicao": idx,
                "link": link,
            }
        )

    return resultados


def resumo_schema(resposta_json: dict) -> dict:
    """Resumo rápido para auditoria/debug (sem imprimir JSON inteiro)."""
    def _len_if_list(x):
        return len(x) if isinstance(x, list) else None

    return {
        "keys": list(resposta_json.keys())[:50],
        "len_search_results": _len_if_list(resposta_json.get("search_results")),
        "len_organic_results": _len_if_list(resposta_json.get("organic_results")),
        "len_results": _len_if_list(resposta_json.get("results")),
    }
