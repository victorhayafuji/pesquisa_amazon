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
# Fuzzy Matching de marcas (base: marcas_conhecidas.py)
# OBS: importado dentro de _aplicar_match_marcas para reduzir acoplamento.


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


def _aplicar_match_marcas(df: pd.DataFrame, threshold: float = 88.0) -> pd.DataFrame:
    """Identifica e normaliza marcas via base `marcas_conhecidas.py` (padrão v1.1).

    Regras:
    - Tenta identificar marca a partir do TÍTULO (coluna `produto`).
    - Se não encontrar, tenta a partir de `marca_raw` (quando existir no schema).
    - Gera:
      * marca_canonica
      * marca_score
      * marca_metodo
    - Registra títulos sem marca em referenciais/titulos_sem_marca.csv.

    Regra especial:
    - A marca "Ou" só é identificada se o título contiver "Ou" ou "OU" com o "O" maiúsculo.
      "ou" minúsculo é separador e não deve virar marca.
    """
    from fuzzy_matching import (
        carregar_marcas_conhecidas,
        preparar_mapa_marcas,
        detectar_marca_no_texto,
        registrar_titulo_sem_marca,
    )

    if df.empty:
        return df

    # garante colunas de saída
    for col in ("marca_canonica", "marca_score", "marca_metodo"):
        if col not in df.columns:
            df[col] = None

    marcas = carregar_marcas_conhecidas()
    if not marcas:
        df["marca_metodo"] = "sem_base"
        return df

    mapa_norm, escolhas = preparar_mapa_marcas(marcas)
    if not mapa_norm and not escolhas:
        df["marca_metodo"] = "sem_base"
        return df

    def _detectar_linha(row: pd.Series) -> Tuple[Optional[str], Optional[float], str]:
        titulo = row.get("produto", None)
        marca_raw = row.get("marca_raw", None)

        m1, s1, met1, cand1, sc1 = detectar_marca_no_texto(titulo, mapa_norm, escolhas, threshold=threshold)
        if m1:
            return m1, s1, f"titulo_{met1}"

        m2, s2, met2, cand2, sc2 = detectar_marca_no_texto(marca_raw, mapa_norm, escolhas, threshold=threshold)
        if m2:
            return m2, s2, f"raw_{met2}"

        # curadoria: registra o título (usa melhor candidato disponível)
        melhor_cand = cand1 or cand2
        melhor_score = sc1 if sc1 is not None else sc2
        registrar_titulo_sem_marca(str(titulo or "").strip(), melhor_cand, melhor_score)
        return None, None, "sem_match"

    res = df.apply(_detectar_linha, axis=1, result_type="expand")
    res.columns = ["marca_canonica", "marca_score", "marca_metodo"]
    df[["marca_canonica", "marca_score", "marca_metodo"]] = res
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

    df = _aplicar_match_marcas(df)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_csv = OUTPUT_DIR / f"resultado_amazon_{slug}_{timestamp}.csv"
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")

    print(f"Total de itens (raw): {total_raw}")
    print(f"Total de itens únicos: {total_unicos}")
    print(f"Arquivo gerado: {out_csv}")


if __name__ == "__main__":
    main()
