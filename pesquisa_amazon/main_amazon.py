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
import math

import pandas as pd

# Matching de marca (marcas_conhecidas.py) — determinístico + fuzzy
try:
    from fuzzy_matching import aplicar_matching_em_df  # type: ignore
except Exception:  # pragma: no cover
    aplicar_matching_em_df = None  # type: ignore

from config_amazon import OUTPUT_DIR, N_PAGINAS_DEFAULT, AMAZON_DOMAIN_DEFAULT, LANGUAGE_DEFAULT
from amazon_search_client import buscar_amazon_search, AmazonSearchError
from processador_resultados_amazon import extrair_resultados_amazon, resumo_schema


# Auditoria (RAW JSON) — DESLIGADO POR PADRÃO
# Motivo: você pediu para remover a solicitação interativa.
# Se no futuro quiser salvar o JSON bruto, mude para True.
SALVAR_JSON_BRUTO = False



# Saída padronizada para análise (padrão Brasil)
ANALISE_DIR = OUTPUT_DIR.parent / "outputs_analise"
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


# =========================
# Saída padronizada (Diretoria / Análises)
# =========================

def _adicionar_metricas_relevancia(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adiciona colunas de análise e score de relevância ao DataFrame:

    - preco_outlier (bool)
    - indice_preco
    - desconto_percentual
    - score_preco
    - score_promo
    - score_atratividade
    - score_visibilidade
    - score_qualidade
    - score_relevancia
    """

    df = df.copy()

    # --- 1. Garantir que preço e preço_original são numéricos ---
    df["preco"] = pd.to_numeric(df.get("preco"), errors="coerce")
    df["preco_original"] = pd.to_numeric(df.get("preco_original"), errors="coerce")

    # --- 2. Detecção de outliers de preço (IQR) ---
    valid_prices = df["preco"].dropna()
    df["preco_outlier"] = False

    if len(valid_prices) >= 4:
        q1 = valid_prices.quantile(0.25)
        q3 = valid_prices.quantile(0.75)
        iqr = q3 - q1
        if iqr > 0:
            limite_inferior = q1 - 1.5 * iqr
            limite_superior = q3 + 1.5 * iqr
            mask_outlier = (df["preco"] < limite_inferior) | (df["preco"] > limite_superior)
            df.loc[mask_outlier, "preco_outlier"] = True

    # --- 3. Índice de preço vs mediana ---
    base_mediana = df.loc[~df["preco_outlier"] & df["preco"].notna(), "preco"]
    if base_mediana.empty:
        base_mediana = valid_prices

    preco_mediana = base_mediana.median() if not base_mediana.empty else None

    if preco_mediana and preco_mediana > 0:
        df["indice_preco"] = df["preco"] / preco_mediana
    else:
        df["indice_preco"] = pd.NA

    # --- 4. Desconto percentual ---
    cond_desconto_valido = (
        df["preco_original"].notna()
        & df["preco_original"].gt(0)
        & df["preco"].notna()
        & df["preco_original"].gt(df["preco"])
    )

    df["desconto_percentual"] = 0.0
    df.loc[cond_desconto_valido, "desconto_percentual"] = (
        (df.loc[cond_desconto_valido, "preco_original"] - df.loc[cond_desconto_valido, "preco"])
        / df.loc[cond_desconto_valido, "preco_original"]
    )

    # --- 5. Score de preço ---
    TOLERANCIA_PRECO = 0.5

    df["score_preco"] = pd.NA

    cond_idx_valid = df["indice_preco"].notna()
    cond_idx_le_1 = cond_idx_valid & (df["indice_preco"] <= 1)
    cond_idx_gt_1 = cond_idx_valid & (df["indice_preco"] > 1)

    df.loc[cond_idx_le_1, "score_preco"] = 1.0

    df.loc[cond_idx_gt_1, "score_preco"] = (
        1.0 - (df.loc[cond_idx_gt_1, "indice_preco"] - 1.0) / TOLERANCIA_PRECO
    ).clip(lower=0.0)

    # --- 6. Score de promoção ---
    DESCONTO_REF = 0.30

    df["score_promo"] = 0.0
    cond_desc_valid = df["desconto_percentual"].notna() & (df["desconto_percentual"] > 0)

    df.loc[cond_desc_valid, "score_promo"] = (
        df.loc[cond_desc_valid, "desconto_percentual"] / DESCONTO_REF
    ).clip(upper=1.0)

    # --- 7. Score de atratividade comercial ---
    df["score_atratividade"] = pd.NA

    cond_atrativ_valid = df["score_preco"].notna()
    df.loc[cond_atrativ_valid, "score_atratividade"] = (
        0.7 * df.loc[cond_atrativ_valid, "score_preco"].astype(float)
        + 0.3 * df.loc[cond_atrativ_valid, "score_promo"].astype(float)
    )

    # --- 8. Score de visibilidade ---
    df["posicao"] = pd.to_numeric(df.get("posicao"), errors="coerce")
    df["score_visibilidade"] = pd.NA

    posicoes_validas = df["posicao"].dropna()
    if not posicoes_validas.empty:
        p_max = posicoes_validas.max()
        if p_max > 0:
            df.loc[df["posicao"].notna(), "score_visibilidade"] = (
                (p_max - df.loc[df["posicao"].notna(), "posicao"] + 1.0) / p_max
            )

    # --- 9. Score de qualidade ---
    df["rating"] = pd.to_numeric(df.get("rating"), errors="coerce")
    df["reviews"] = pd.to_numeric(df.get("reviews"), errors="coerce")

    df["score_qualidade"] = pd.NA

    R_REF = 100.0

    cond_qual_valid = df["rating"].notna() & df["reviews"].notna() & (df["rating"] > 0) & (df["reviews"] >= 0)

    if cond_qual_valid.any():
        q_rating = df.loc[cond_qual_valid, "rating"] / 5.0

        def _calc_f_reviews(rev: float) -> float:
            try:
                return min(1.0, math.log(1.0 + rev) / math.log(1.0 + R_REF))
            except (ValueError, ZeroDivisionError):
                return 0.0

        f_reviews = df.loc[cond_qual_valid, "reviews"].apply(_calc_f_reviews)

        df.loc[cond_qual_valid, "score_qualidade"] = (q_rating * f_reviews).clip(upper=1.0)

    # --- 10. Score final de relevância ---
    df["score_relevancia"] = pd.NA

    cond_final = (
        df["score_visibilidade"].notna()
        | df["score_qualidade"].notna()
        | df["score_atratividade"].notna()
    )

    if cond_final.any():
        vis = pd.to_numeric(df.loc[cond_final, "score_visibilidade"], errors="coerce").fillna(0.0)
        qual = pd.to_numeric(df.loc[cond_final, "score_qualidade"], errors="coerce").fillna(0.0)
        atr = pd.to_numeric(df.loc[cond_final, "score_atratividade"], errors="coerce").fillna(0.0)

        df.loc[cond_final, "score_relevancia"] = 0.4 * vis + 0.3 * qual + 0.3 * atr

    return df


EXEC_COLS_ANALISE = [
    "origem",
    "capturado_em",
    "palavra_chave",
    "pagina",
    "posicao",
    "produto",
    "marca",
    "seller",
    "preco",
    "preco_original",
    "desconto_percentual",
    "rating",
    "reviews",
    "patrocinado",
    "id_item",
    "link",
    "indice_preco",
    "score_relevancia",
    "score_visibilidade",
    "score_qualidade",
    "score_atratividade",
]


def _montar_df_analise(df: pd.DataFrame, *, origem: str, palavra_chave: str) -> pd.DataFrame:
    """Cria um DataFrame padronizado (mesmo layout para Amazon e Google Shopping).

    Regra conservadora:
    - Preenche com NA quando o campo não existir na fonte.
    - Mantém valores numéricos como numéricos (para Excel/Power BI lerem corretamente).
    """
    if df.empty:
        return df.copy()

    base = df.copy()

    # Captura (string)
    if "capturado_em" not in base.columns:
        base["capturado_em"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Marca (prioridade: canonizada -> marca_raw -> vazio)
    marca = None
    for c in ("marca_canonica", "marca", "marca_raw"):
        if c in base.columns:
            marca = base[c]
            break
    if marca is None:
        marca = pd.NA

    # Padroniza capitalização da marca para leitura executiva
    def _fmt_marca(x):
        if pd.isna(x):
            return pd.NA
        s = str(x).strip()
        if not s:
            return pd.NA
        # Mantém tokens com dígitos (ex.: 3M) como estão
        def _fmt_piece(piece: str) -> str:
            if not piece:
                return piece
            if any(ch.isdigit() for ch in piece):
                return piece
            if len(piece) == 1:
                return piece.upper()
            return piece[0].upper() + piece[1:].lower()

        partes = re.split(r"(\s+)", s)
        out = []
        for tok in partes:
            if not tok:
                continue
            if tok.isspace():
                out.append(tok)
                continue
            sub = re.split(r"([\-\/&\+])", tok)
            sub_out = []
            for p in sub:
                if p in "-/&+":
                    sub_out.append(p)
                else:
                    if "'" in p:
                        sub_out.append("'".join(_fmt_piece(t) for t in p.split("'")))
                    else:
                        sub_out.append(_fmt_piece(p))
            out.append("".join(sub_out))
        return "".join(out)

    try:
        marca = marca.map(_fmt_marca)  # type: ignore[attr-defined]
    except Exception:
        # Se marca não for Series por algum motivo, trata como escalar
        marca = _fmt_marca(marca)

    # ID do item (Amazon = ASIN)
    id_item = base["asin"] if "asin" in base.columns else pd.NA

    # Campos opcionais de Google (mantemos para padronizar, ficam vazios aqui)
    def _col_or_na(col: str):
        return base[col] if col in base.columns else pd.NA

    out = pd.DataFrame({
        "origem": origem,
        "capturado_em": base["capturado_em"],
        "palavra_chave": base["palavra_chave"] if "palavra_chave" in base.columns else palavra_chave,
        "pagina": _col_or_na("pagina"),
        "posicao": _col_or_na("posicao"),
        "produto": _col_or_na("produto"),
        "marca": marca,
        "seller": _col_or_na("seller"),
        "preco": _col_or_na("preco"),
        "preco_original": _col_or_na("preco_original"),
        "desconto_percentual": _col_or_na("desconto_percentual"),
        "rating": _col_or_na("rating"),
        "reviews": _col_or_na("reviews"),
        "patrocinado": _col_or_na("patrocinado"),
        "id_item": id_item,
        "link": _col_or_na("link"),
        "indice_preco": _col_or_na("indice_preco"),
        "score_relevancia": _col_or_na("score_relevancia"),
        "score_visibilidade": _col_or_na("score_visibilidade"),
        "score_qualidade": _col_or_na("score_qualidade"),
        "score_atratividade": _col_or_na("score_atratividade"),
    })

    # desconto em % (para leitura executiva). Se vier como fração 0-1, converte para 0-100.
    if "desconto_percentual" in out.columns:
        try:
            s = pd.to_numeric(out["desconto_percentual"], errors="coerce")
            if s.notna().any() and (s.dropna().max() <= 1.0):
                out["desconto_percentual"] = (s * 100.0)
        except Exception:
            pass

    # Garante ordem e presença das colunas
    for c in EXEC_COLS_ANALISE:
        if c not in out.columns:
            out[c] = pd.NA
    out = out[EXEC_COLS_ANALISE]

    # Tipagem numérica (para exportar bem no CSV)
    num_cols = [
        "pagina", "posicao", "preco", "preco_original", "desconto_percentual",
        "rating", "reviews", "indice_preco",
        "score_relevancia", "score_visibilidade", "score_qualidade", "score_atratividade",
    ]
    for c in num_cols:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")

    return out


def _exportar_csv_analise(df_analise: pd.DataFrame, *, origem: str, slug: str, timestamp: str) -> Path:
    """Exporta CSV PT-BR: separador ';' e decimal ',' (bom para Excel no Brasil)."""
    data_exec = datetime.now().strftime("%Y-%m-%d")
    out_dir = ANALISE_DIR / "csv" / data_exec
    out_dir.mkdir(parents=True, exist_ok=True)

    caminho = out_dir / f"Analise_{origem}_{slug}_{timestamp}.csv"
    df_analise.to_csv(caminho, index=False, sep=";", decimal=",", encoding="utf-8-sig")
    return caminho


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
            r["pagina"] = page
            r["posicao"] = base + i
        todos.extend(resultados_pagina)

        print(f"Página {page}: {len(resultados_pagina)} itens extraídos")

    if not todos:
        print("Nenhum resultado extraído. Verifique a resposta JSON e o mapeamento no processador.")
        return

    df = pd.DataFrame(todos)

    df = _adicionar_metricas_relevancia(df)

    # Coluna de timestamp da captura (padrão compartilhado com Google Shopping)
    df["capturado_em"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    total_raw = len(df)
    df = _deduplicar(df)
    total_unicos = len(df)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_csv = OUTPUT_DIR / f"resultado_amazon_{slug}_{timestamp}.csv"
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")

    # --- Identificação de marca (título do anúncio) ---
    # Mantemos o CSV original acima. A marca é aplicada em uma cópia para a saída executiva.
    df_exec = df.copy()
    if aplicar_matching_em_df is not None:
        try:
            df_exec = aplicar_matching_em_df(df_exec, col_titulo='produto', col_marca_raw='marca_raw', threshold=88.0)
        except Exception as e:
            print(f"Aviso: matching de marca desativado por erro: {e}")
    else:
        print("Aviso: fuzzy_matching.py não disponível; saída executiva ficará sem marca identificada.")

    # --- Saída executiva padronizada (para análises / diretoria) ---
    # Mantém o CSV original acima e cria um segundo CSV com layout idêntico ao do Google Shopping.
    # Garante métricas na base executiva (se já existirem, a função apenas sobrescreve as colunas).
    df_metric = _adicionar_metricas_relevancia(df_exec)
    df_analise = _montar_df_analise(df_metric, origem="Amazon", palavra_chave=palavra)
    out_csv_analise = _exportar_csv_analise(df_analise, origem="Amazon", slug=slug, timestamp=timestamp)

    print(f"Arquivo executivo gerado (PT-BR, ; e vírgula): {out_csv_analise}")

    print(f"Total de itens (raw): {total_raw}")
    print(f"Total de itens únicos: {total_unicos}")
    print(f"Arquivo gerado: {out_csv}")


if __name__ == "__main__":
    main()