# fuzzy_matching_marcas_conhecidas.py
"""Fuzzy Matching de marcas baseado em referenciais/marcas_conhecidas.py.

Regra do projeto (padrão v1.1):
- A base oficial de marcas é a lista MARCAS_KNOWN no arquivo marcas_conhecidas.py.
- A identificação é feita principalmente pelo TÍTULO do anúncio (campo `produto`).
- Primeiro tenta "exact" (determinístico) e só depois fuzzy (RapidFuzz), para evitar falso positivo.

Regra especial (obrigatória):
- A marca "Ou" só deve ser identificada quando houver "Ou" ou "OU" no título com a letra "O" MAIÚSCULA.
  Se aparecer "ou" minúsculo, é separador e NÃO deve ser considerado marca.
"""

from __future__ import annotations

from typing import Iterable, List, Optional, Tuple, Dict
from pathlib import Path
import ast
import csv
import os
import re
import unicodedata

try:
    from rapidfuzz import process, fuzz, utils as fuzz_utils  # type: ignore[import]
except Exception:
    process = None  # type: ignore[assignment]
    fuzz = None  # type: ignore[assignment]
    fuzz_utils = None  # type: ignore[assignment]


def _base_dir() -> Path:
    """Base do projeto (prioriza config_amazon.PROJETO_DIR; fallback para pasta do arquivo)."""
    try:
        from config_amazon import PROJETO_DIR  # type: ignore
        return Path(PROJETO_DIR)
    except Exception:
        return Path(__file__).resolve().parent


BASE_DIR = _base_dir()
REFERENCIAIS_DIR = BASE_DIR / "referenciais"
MARCAS_CONHECIDAS_PY = "marcas_conhecidas.py"
TITULOS_SEM_MARCA_CSV = REFERENCIAIS_DIR / "titulos_sem_marca.csv"

# cache simples para não escrever o mesmo título várias vezes na mesma execução
_TITULOS_SEM_MARCA_SESSAO: set[str] = set()

# --- Normalização básica (conservadora) ---

def _remover_acentos(texto: str) -> str:
    texto_nfkd = unicodedata.normalize("NFKD", texto)
    return "".join(ch for ch in texto_nfkd if not unicodedata.combining(ch))


def normalizar_texto(texto: str) -> str:
    """Normaliza para matching:
    - remove acentos
    - deixa em MAIÚSCULO
    - troca qualquer coisa que não seja A-Z/0-9 por espaço
    - colapsa espaços
    """
    t = _remover_acentos(texto or "")
    t = t.upper()
    t = re.sub(r"[^A-Z0-9]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


# --- Carregamento de marcas conhecidas ---

def _extrair_marcas_via_ast(py_text: str) -> Optional[List[str]]:
    """Tenta extrair MARCAS_KNOWN via AST (mais seguro quando o arquivo é Python válido)."""
    try:
        tree = ast.parse(py_text)
    except Exception:
        return None

    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "MARCAS_KNOWN":
                    try:
                        marcas = ast.literal_eval(node.value)
                        if isinstance(marcas, list):
                            out = []
                            for x in marcas:
                                if x is None:
                                    continue
                                s = str(x).strip()
                                if s:
                                    out.append(s)
                            return out
                    except Exception:
                        return None
    return None


def _extrair_strings_por_regex(py_text: str) -> List[str]:
    """Fallback: extrai todas as strings literais do arquivo (caso o AST falhe)."""
    strings: List[str] = []
    strings += re.findall(r'"([^"\\]*(?:\\.[^"\\]*)*)"', py_text)
    strings += re.findall(r"'([^'\\]*(?:\\.[^'\\]*)*)'", py_text)
    # limpa escapes simples
    out = []
    for s in strings:
        try:
            out.append(bytes(s, "utf-8").decode("unicode_escape").strip())
        except Exception:
            out.append(s.strip())
    return [s for s in out if s]


def carregar_marcas_conhecidas() -> List[str]:
    """Carrega marcas conhecidas a partir de marcas_conhecidas.py.

    Procura em:
    - referenciais/marcas_conhecidas.py
    - ./marcas_conhecidas.py (mesma pasta do projeto)
    - mesma pasta deste arquivo
    """
    candidatos = [
        (REFERENCIAIS_DIR / MARCAS_CONHECIDAS_PY),
        (BASE_DIR / MARCAS_CONHECIDAS_PY),
        (Path(__file__).resolve().parent / MARCAS_CONHECIDAS_PY),
    ]

    for p in candidatos:
        if not p.exists():
            continue

        try:
            py_text = p.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            py_text = p.read_text(encoding="latin-1", errors="ignore")

        marcas = _extrair_marcas_via_ast(py_text)
        if marcas is None:
            # fallback por regex (ainda filtra bastante)
            marcas = _extrair_strings_por_regex(py_text)

        # normaliza a lista final: remove vazios e duplica exata preservando ordem
        visto = set()
        out: List[str] = []
        for m in marcas:
            mm = (m or "").strip()
            if not mm:
                continue
            if mm in visto:
                continue
            visto.add(mm)
            out.append(mm)
        return out

    return []


def preparar_mapa_marcas(marcas: Iterable[str]) -> Tuple[Dict[str, str], List[str]]:
    """Cria mapa normalizado -> marca canônica, e lista de escolhas canônicas.

    Critério de canônica quando há duplicatas:
    - prefere a que contém espaço (ex.: 'EURO HOME' em vez de 'EUROHOME')
    - depois, prefere a mais longa

    Regra especial:
    - Remove "Ou"/"OU" do mapa e das escolhas, porque essa marca NÃO pode ser case-insensitive.
      Ela é tratada por regra case-sensitive em detectar_marca_no_texto().
    """
    mapa: Dict[str, str] = {}

    for m in marcas:
        orig = (m or "").strip()
        if not orig:
            continue
        norm = normalizar_texto(orig)
        if not norm:
            continue

        # "OU" / "Ou" fica fora do matching case-insensitive
        if norm == "OU":
            continue

        if norm not in mapa:
            mapa[norm] = orig
        else:
            atual = mapa[norm]

            def _score(s: str) -> Tuple[int, int]:
                return (1 if " " in s else 0, len(s))

            if _score(orig) > _score(atual):
                mapa[norm] = orig

    canonicas = [mapa[n] for n in mapa.keys()]
    return mapa, canonicas


# --- Matching ---

_OU_REGEX = re.compile(r"(?<![A-Za-z0-9])(?:Ou|OU)(?![A-Za-z0-9])")


def melhor_match_fuzzy(texto: str, choices: List[str]) -> Tuple[Optional[str], Optional[float]]:
    """Retorna (melhor_choice, score) usando RapidFuzz."""
    if process is None or fuzz is None or fuzz_utils is None:
        return None, None

    if not texto or not choices:
        return None, None

    resultado = process.extractOne(
        texto,
        choices,
        scorer=fuzz.WRatio,
        processor=fuzz_utils.default_process,
    )
    if not resultado:
        return None, None
    melhor, score, _ = resultado
    return melhor, float(score)


def detectar_marca_no_texto(
    texto: Optional[str],
    mapa_norm_para_canon: Dict[str, str],
    escolhas_canonicas: List[str],
    threshold: float = 88.0,
) -> Tuple[Optional[str], Optional[float], Optional[str], Optional[str], Optional[float]]:
    """Detecta marca conhecida dentro de um texto (título ou marca_raw).

    Retorna:
    - marca_detectada (ou None)
    - score (100 em exact; score do RapidFuzz em fuzzy)
    - metodo ('exact' | 'fuzzy' | 'sem_match' | 'rapidfuzz_off' | 'ou_case')
    - melhor_candidato (mesmo abaixo de threshold; útil para auditoria)
    - melhor_score
    """
    t = (texto or "").strip()
    if not t:
        return None, None, "sem_match", None, None

    # Regra especial: "Ou" só casa se tiver O maiúsculo no texto original.
    # Isso evita confundir com "ou" (conjunção/separador).
    if _OU_REGEX.search(t):
        return "Ou", 100.0, "ou_case", "Ou", 100.0

    # 1) exact contain (determinístico): casa por palavra/frase inteira no texto normalizado
    t_norm = normalizar_texto(t)
    if not t_norm:
        return None, None, "sem_match", None, None


    # Guarda conservadora: textos muito curtos geram falso positivo em fuzzy.
    if len(t_norm) < 4:
        return None, None, "sem_match", None, None

    alvo = f" {t_norm} "
    melhor_norm = None
    for marca_norm in mapa_norm_para_canon.keys():
        if not marca_norm:
            continue
        if f" {marca_norm} " in alvo:
            if melhor_norm is None or len(marca_norm) > len(melhor_norm):
                melhor_norm = marca_norm

    if melhor_norm is not None:
        canon = mapa_norm_para_canon[melhor_norm]
        return canon, 100.0, "exact", canon, 100.0

    # 2) fuzzy (quando disponível)
    if process is None or fuzz is None or fuzz_utils is None:
        return None, None, "rapidfuzz_off", None, None

    melhor, score = melhor_match_fuzzy(t, escolhas_canonicas)
    if melhor is None or score is None:
        return None, None, "sem_match", None, None

    if score >= threshold:
        return melhor, score, "fuzzy", melhor, score

    return None, None, "sem_match", melhor, score


def registrar_titulo_sem_marca(titulo: str, melhor_candidato: Optional[str], melhor_score: Optional[float]) -> None:
    """Registra títulos que ficaram sem marca para curadoria."""
    t = (titulo or "").strip()
    if not t:
        return

    key = t.lower()
    if key in _TITULOS_SEM_MARCA_SESSAO:
        return
    _TITULOS_SEM_MARCA_SESSAO.add(key)

    REFERENCIAIS_DIR.mkdir(parents=True, exist_ok=True)
    escrever_header = not TITULOS_SEM_MARCA_CSV.exists()

    with TITULOS_SEM_MARCA_CSV.open("a", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f, delimiter=";")
        if escrever_header:
            writer.writerow(["titulo", "melhor_candidato", "melhor_score"])
        writer.writerow([t, melhor_candidato or "", f"{melhor_score:.1f}" if melhor_score is not None else ""])
