"""Main — Amazon Search (SearchAPI).

Mantém o modelo do Google Shopping:
- input palavra-chave
- consulta SearchAPI em páginas
- extrai campos para tabela
- deduplica
- exporta CSV em outputs/

Nota:
- Como o schema pode variar, o processador usa fallback e pode retornar lista vazia.
  Se isso ocorrer, confira o JSON de resposta manualmente e ajuste o mapeamento de campos.
"""

from __future__ import annotations

import re
from datetime import datetime

import pandas as pd

from config_amazon import OUTPUT_DIR, N_PAGINAS_DEFAULT, AMAZON_DOMAIN_DEFAULT, LANGUAGE_DEFAULT
from amazon_search_client import buscar_amazon_search, AmazonSearchError
from processador_resultados_amazon import extrair_resultados_amazon, resumo_schema
from fuzzy_matching import carregar_marcas_conhecidas, preparar_mapa_marcas, detectar_marca_no_texto, registrar_titulo_sem_marca


# Auditoria (RAW JSON) — DESLIGADO POR PADRÃO
# Motivo: você pediu para remover a solicitação interativa.
# Se no futuro quiser salvar o JSON bruto, mude para True.
SALVAR_JSON_BRUTO = False


def _slugify(texto: str) -> str:
    texto = texto.strip().lower()
    texto = re.sub(r"\s+", "_", texto)
    texto = re.sub(r"[^a-z0-9_\-]+", "", texto)
    return texto[:80] or "busca"


def _deduplicar(df: pd.DataFrame) -> pd.DataFrame:
    if "asin" in df.columns and df["asin"].notna().any():
        return df.drop_duplicates(subset=["asin"], keep="first")
    cols = [c for c in ["produto", "seller", "preco"] if c in df.columns]
    return df.drop_duplicates(subset=cols, keep="first") if cols else df


def _aplicar_fuzzy_marcas(df: pd.DataFrame, threshold: float = 88.0) -> pd.DataFrame:
    """Identifica e normaliza marcas via fuzzy matching (padrão v1.1 do Google Search),
    usando `marcas_conhecidas.py` (lista MARCAS_KNOWN) e o *título do anúncio*.

    Prioridade de identificação:
    1) `marca_raw` (quando vier do schema da API)
    2) `produto` (título do anúncio)

    Saídas adicionadas ao DataFrame:
    - marca_canonica: marca identificada (ou None)
    - marca_score: score (100 em exact; score do RapidFuzz em fuzzy)
    - marca_metodo: raw_exact | raw_fuzzy | titulo_exact | titulo_fuzzy | sem_match | rapidfuzz_off | sem_base
    - Observação: quando não houver match, registramos o título em referenciais/titulos_sem_marca.csv
      para curadoria (adicionar marcas novas em marcas_conhecidas.py).
    """
    if ("produto" not in df.columns) and ("marca_raw" not in df.columns):
        return df

    marcas = carregar_marcas_conhecidas()
    if not marcas:
        df["marca_canonica"] = None
        df["marca_score"] = None
        df["marca_metodo"] = "sem_base"
        print("Aviso: marcas_conhecidas.py não encontrado ou lista vazia. Colunas de marca ficarão vazias.")
        return df

    mapa_norm, escolhas = preparar_mapa_marcas(marcas)

    def _resolver(row: pd.Series):
        raw = row.get("marca_raw", None)
        titulo = row.get("produto", None)

        # 1) tenta pela marca retornada pela API
        marca_raw, score_raw, metodo_raw, best_raw, best_score_raw = detectar_marca_no_texto(
            str(raw) if raw is not None else None,
            mapa_norm_para_canon=mapa_norm,
            escolhas_canonicas=escolhas,
            threshold=threshold,
        )
        if marca_raw:
            return marca_raw, score_raw, f"raw_{metodo_raw}"

        # 2) tenta pelo título do anúncio
        marca_tit, score_tit, metodo_tit, best_tit, best_score_tit = detectar_marca_no_texto(
            str(titulo) if titulo is not None else None,
            mapa_norm_para_canon=mapa_norm,
            escolhas_canonicas=escolhas,
            threshold=threshold,
        )
        if marca_tit:
            return marca_tit, score_tit, f"titulo_{metodo_tit}"

        # 3) sem match: registra para revisão
        if titulo:
            registrar_titulo_sem_marca(str(titulo), melhor_candidato=best_tit, melhor_score=best_score_tit)

        return None, None, metodo_tit

    df[["marca_canonica", "marca_score", "marca_metodo"]] = df.apply(_resolver, axis=1, result_type="expand")
    return df

def main() -> None:
    print("=== Pesquisa de Mercado — Amazon (SearchAPI) ===")
    palavra = input("Digite o produto a ser pesquisado (ex.: 'mop spray', 'jogo de panelas'): ").strip()
    if not palavra:
        print("Palavra-chave vazia. Encerrando.")
        return

    n_paginas_in = input(f"Número de páginas (Enter para padrão {N_PAGINAS_DEFAULT}): ").strip()
    n_paginas = int(n_paginas_in) if n_paginas_in.isdigit() else N_PAGINAS_DEFAULT

    amazon_domain = input(f"amazon_domain (Enter para padrão {AMAZON_DOMAIN_DEFAULT}): ").strip() or AMAZON_DOMAIN_DEFAULT
    language = input(f"language (Enter para padrão {LANGUAGE_DEFAULT}): ").strip() or LANGUAGE_DEFAULT

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    slug = _slugify(palavra)

    # Diretório opcional de JSON bruto (somente se SALVAR_JSON_BRUTO=True)
    run_dir = OUTPUT_DIR / "raw_json_amazon" / slug / timestamp
    if SALVAR_JSON_BRUTO:
        import json
        run_dir.mkdir(parents=True, exist_ok=True)

    todos: list[dict] = []

    for page in range(1, n_paginas + 1):
        try:
            resposta = buscar_amazon_search(
                q=palavra,
                page=page,
                amazon_domain=amazon_domain,
                language=language,
            )
        except AmazonSearchError as e:
            print(f"Erro na página {page}: {e}")
            break

        if SALVAR_JSON_BRUTO:
            import json
            (run_dir / f"page_{page:02d}.json").write_text(
                json.dumps(resposta, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

        resultados_pagina = extrair_resultados_amazon(resposta_json=resposta, palavra_chave=palavra)

        if not resultados_pagina:
            print(f"Página {page}: nenhum item extraído. Schema: {resumo_schema(resposta)}")
            break

        # Ajusta posição global
        base = len(todos)
        for i, r in enumerate(resultados_pagina, start=1):
            r["posicao"] = base + i
        todos.extend(resultados_pagina)

        print(f"Página {page}: {len(resultados_pagina)} itens extraídos")

    if not todos:
        print("Nenhum resultado extraído. Verifique a resposta JSON e o mapeamento no processador.")
        return

    df = pd.DataFrame(todos)
    total_raw = len(df)
    df = _deduplicar(df)
    total_unicos = len(df)

    df = _aplicar_fuzzy_marcas(df)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_csv = OUTPUT_DIR / f"resultado_amazon_{slug}_{timestamp}.csv"
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")

    print(f"Total de itens (raw): {total_raw}")
    print(f"Total de itens únicos: {total_unicos}")
    print(f"Arquivo gerado: {out_csv}")


if __name__ == "__main__":
    main()
