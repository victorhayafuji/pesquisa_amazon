# fuzzy_matching.py
"""Utilitários de Fuzzy Matching para normalizar e identificar marcas.

Neste projeto, a fonte oficial de marcas conhecidas é:
- referenciais/marcas_conhecidas.py  (lista MARCAS_KNOWN)

A identificação de marca é feita principalmente pelo *título do anúncio* (campo `produto`).
"""

from __future__ import annotations

from typing import Iterable, List, Optional, Tuple, Dict
from pathlib import Path
import csv
import os
import re
import unicodedata

try:
    # RapidFuzz: fuzzy matching moderno e performático
    # pip install rapidfuzz
    from rapidfuzz import process, fuzz  # type: ignore[import]
except ImportError:
    process = None  # type: ignore[assignment]
    fuzz = None  # type: ignore[assignment]


# Base do projeto (tenta config padrão; fallback para config do Amazon)
try:
    from config import BASE_DIR  # type: ignore
except Exception:  # pragma: no cover
    from config_amazon import PROJETO_DIR as BASE_DIR


def _dir_referenciais() -> Path:
    """Retorna um diretório de referenciais gravável.

    Em alguns ambientes (como sandbox), a pasta `referenciais/` pode não estar gravável.
    Neste caso, usamos `referenciais_local/` como fallback.
    """
    preferido = BASE_DIR / "referenciais"
    if preferido.exists() and os.access(preferido, os.W_OK):
        return preferido

    # tenta criar e usar o preferido
    try:
        preferido.mkdir(parents=True, exist_ok=True)
        if os.access(preferido, os.W_OK):
            return preferido
    except Exception:
        pass

    fallback = BASE_DIR / "referenciais_local"
    fallback.mkdir(parents=True, exist_ok=True)
    return fallback


REFERENCIAIS_DIR = _dir_referenciais()

MARCAS_CONHECIDAS_PY = "marcas_conhecidas.py"
TITULOS_SEM_MARCA_CSV = REFERENCIAIS_DIR / "titulos_sem_marca.csv"

# Deduplicação em memória (por execução)
_TITULOS_SEM_MARCA_SESSAO: set[str] = set()


def _remover_acentos(texto: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", texto) if not unicodedata.combining(c))


def normalizar_texto(texto: str) -> str:
    """Normaliza texto para matching:
    - remove acentos
    - upper
    - remove pontuação (vira espaço)
    - colapsa espaços
    """
    t = _remover_acentos(texto or "").upper()
    t = re.sub(r"[^A-Z0-9]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _extrair_strings_de_python(py_text: str) -> List[str]:
    """Extrai *todas* as strings literais do arquivo .py.

    Isto é propositalmente mais robusto do que `import marcas_conhecidas`,
    porque evita problemas de sintaxe/concatenação de strings por falta de vírgula.
    """
    # captura "..." e '...'
    strings = []
    strings += re.findall(r'"([^"\\]*(?:\\.[^"\\]*)*)"', py_text)
    strings += re.findall(r"'([^'\\]*(?:\\.[^'\\]*)*)'", py_text)
    # desfaz escapes mais comuns
    def _unescape(s: str) -> str:
        try:
            return bytes(s, "utf-8").decode("unicode_escape")
        except Exception:
            return s
    return [_unescape(s) for s in strings]


def carregar_marcas_conhecidas() -> List[str]:
    """Carrega lista de marcas conhecidas a partir de `marcas_conhecidas.py`.

    Procura em:
    - BASE_DIR/referenciais/marcas_conhecidas.py
    - BASE_DIR/referenciais_local/marcas_conhecidas.py
    - BASE_DIR/marcas_conhecidas.py
    - mesmo diretório deste arquivo
    """
    candidatos = [
        (BASE_DIR / "referenciais" / MARCAS_CONHECIDAS_PY),
        (BASE_DIR / "referenciais_local" / MARCAS_CONHECIDAS_PY),
        (BASE_DIR / MARCAS_CONHECIDAS_PY),
        (Path(__file__).resolve().parent / MARCAS_CONHECIDAS_PY),
    ]

    for p in candidatos:
        if p.exists():
            try:
                conteudo = p.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                conteudo = p.read_text(encoding="latin-1", errors="ignore")

            # pega strings e filtra o mínimo: remove coisas vazias e nomes de variáveis
            brutas = [s.strip() for s in _extrair_strings_de_python(conteudo)]
            # tenta focar no que parece marca: remove strings muito pequenas e genéricas
            marcas = [s for s in brutas if s and len(s) >= 2]
            # dedupe preservando ordem
            visto = set()
            out = []
            for m in marcas:
                key = m.strip()
                if key not in visto:
                    visto.add(key)
                    out.append(key)
            return out

    return []


def preparar_mapa_marcas(marcas: Iterable[str]) -> Tuple[Dict[str, str], List[str]]:
    """Cria um mapa normalizado -> marca canônica.

    Critério de escolha da forma canônica quando há duplicatas:
    - prefere a que contém espaço (ex.: 'EURO HOME' em vez de 'EUROHOME')
    - depois, prefere a mais longa
    """
    mapa: Dict[str, str] = {}
    for m in marcas:
        orig = (m or "").strip()
        if not orig:
            continue
        norm = normalizar_texto(orig)
        if not norm:
            continue
        if norm not in mapa:
            mapa[norm] = orig
        else:
            atual = mapa[norm]
            # preferir com espaços, e depois mais longo
            def _score(s: str) -> Tuple[int, int]:
                return (1 if " " in s else 0, len(s))
            if _score(orig) > _score(atual):
                mapa[norm] = orig

    # lista canônica (valores) deduplicada preservando ordem dos norms
    canonicas = [mapa[n] for n in mapa.keys()]
    return mapa, canonicas


def melhor_match_fuzzy(texto: str, escolhas: List[str]) -> Tuple[Optional[str], Optional[float]]:
    """Retorna o melhor match fuzzy (mesmo que abaixo de threshold).

    Se RapidFuzz não estiver instalado, retorna (None, None).
    """
    if not texto or not escolhas or process is None or fuzz is None:
        return None, None

    # parcial costuma funcionar melhor para "marca dentro de título grande"
    resultado = process.extractOne(
        query=texto,
        choices=escolhas,
        scorer=fuzz.partial_ratio,
        processor=normalizar_texto,
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
    - metodo ('exact' | 'fuzzy' | 'sem_match' | 'rapidfuzz_off')
    - melhor_candidato (mesmo abaixo de threshold; útil para auditoria)
    - melhor_score
    """
    t = (texto or "").strip()
    if not t:
        return None, None, "sem_match", None, None

    t_norm = normalizar_texto(t)
    if not t_norm:
        return None, None, "sem_match", None, None

    # 1) exact contain (mais conservador e determinístico)
    # escolhe o match mais "específico" (norm mais longo)
    alvo = f" {t_norm} "
    melhor_norm = None
    for marca_norm in mapa_norm_para_canon.keys():
        if not marca_norm:
            continue
        if f" {marca_norm} " in alvo:
            if melhor_norm is None or len(marca_norm) > len(melhor_norm):
                melhor_norm = marca_norm

    if melhor_norm is not None:
        return mapa_norm_para_canon[melhor_norm], 100.0, "exact", mapa_norm_para_canon[melhor_norm], 100.0

    # 2) fuzzy (quando disponível)
    if process is None or fuzz is None:
        return None, None, "rapidfuzz_off", None, None

    melhor, score = melhor_match_fuzzy(t, escolhas_canonicas)
    if melhor is None or score is None:
        return None, None, "sem_match", None, None

    if score >= threshold:
        return melhor, score, "fuzzy", melhor, score

    return None, None, "sem_match", melhor, score


def registrar_titulo_sem_marca(
    titulo: str,
    melhor_candidato: Optional[str] = None,
    melhor_score: Optional[float] = None,
) -> None:
    """Registra títulos sem marca identificada para revisão/curadoria."""
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
