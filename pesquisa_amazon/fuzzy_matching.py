# fuzzy_matching.py
"""
Identificação de marca (determinístico + fuzzy) usando a lista oficial em marcas_conhecidas.py.

Regras do projeto (tradicionais / conservadoras):
- Primeiro tenta achar a marca por "contém" no título (match exato de palavras), pois é auditável.
- Só usa fuzzy matching como fallback.
- Marca "Ou": só é considerada marca se estiver com 'O' maiúsculo no título ("Ou" ou "OU").
  Se aparecer como "ou" minúsculo, é separador e NÃO deve virar marca.

Dependências:
- Fuzzy é opcional. Se RapidFuzz não estiver instalado, roda apenas o match determinístico.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Tuple, List, Dict
import re
import unicodedata

try:
    from rapidfuzz import process, fuzz  # type: ignore
except Exception:  # pragma: no cover
    process = None  # type: ignore
    fuzz = None  # type: ignore


def _remover_acentos(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))


def _normalizar_texto(s: str) -> str:
    """Normaliza texto para matching (sem acento, maiúsculo, pontuação vira espaço)."""
    s = (s or "").strip()
    if not s:
        return ""
    s = _remover_acentos(s).upper()
    s = re.sub(r"[^A-Z0-9]+", " ", s)  # mantém letras e números
    s = re.sub(r"\s+", " ", s).strip()
    return s



def formatar_marca_titlecase(marca: Optional[str]) -> Optional[str]:
    """Formata marca no padrão: Primeira letra maiúscula e o restante minúsculo em cada palavra.

    Observação:
    - Mantém tokens com dígitos (ex.: '3M') como estão, para não perder significado.
    - Preserva separadores comuns ('-', '/', '&', '+') dentro do token.
    """
    if marca is None:
        return None
    s = str(marca).strip()
    if not s:
        return None

    def _fmt_piece(piece: str) -> str:
        if not piece:
            return piece
        if any(ch.isdigit() for ch in piece):
            return piece
        if len(piece) == 1:
            return piece.upper()
        return piece[0].upper() + piece[1:].lower()

    # Mantém espaços, formata cada "token" entre espaços
    partes = re.split(r"(\s+)", s)
    out: list[str] = []
    for tok in partes:
        if not tok:
            continue
        if tok.isspace():
            out.append(tok)
            continue

        # Separa por delimitadores internos, preservando-os
        sub = re.split(r"([\-\/&\+])", tok)
        sub_out: list[str] = []
        for p in sub:
            if p in "-/&+":
                sub_out.append(p)
            else:
                # Trata apóstrofo (O'Neill)
                if "'" in p:
                    sub_out.append("'".join(_fmt_piece(x) for x in p.split("'")))
                else:
                    sub_out.append(_fmt_piece(p))
        out.append("".join(sub_out))

    return "".join(out)
def _extrair_strings_python(texto_py: str) -> List[str]:
    """Extrai strings de um arquivo .py mesmo se houver erros de sintaxe (ex.: vírgulas faltando)."""
    # captura "..." e '...'
    itens = re.findall(r'(?:"([^"]+)"|\'([^\']+)\')', texto_py)
    out: List[str] = []
    for a, b in itens:
        v = a or b
        v = (v or "").strip()
        if v:
            out.append(v)
    return out


def carregar_marcas_conhecidas(caminho_marcas_py: Path) -> List[str]:
    """Carrega a lista de marcas do arquivo marcas_conhecidas.py (robusto a pequenos erros)."""
    texto = caminho_marcas_py.read_text(encoding="utf-8", errors="replace")
    marcas = _extrair_strings_python(texto)

    # remove vazios e duplica, preservando a primeira ocorrência
    seen = set()
    marcas_unicas: List[str] = []
    for m in marcas:
        key = _normalizar_texto(m)
        if not key:
            continue
        if key not in seen:
            seen.add(key)
            marcas_unicas.append(m.strip())

    return marcas_unicas


def localizar_marcas_conhecidas() -> Optional[Path]:
    """
    Procura marcas_conhecidas.py:
    - na pasta atual
    - em ./referenciais/
    - na pasta do arquivo caller (onde o script principal está)
    """
    candidatos = [
        Path.cwd() / "marcas_conhecidas.py",
        Path.cwd() / "referenciais" / "marcas_conhecidas.py",
        Path(__file__).resolve().parent / "marcas_conhecidas.py",
        Path(__file__).resolve().parent / "referenciais" / "marcas_conhecidas.py",
    ]
    for p in candidatos:
        if p.exists():
            return p
    return None


@dataclass
class MatchMarca:
    marca: Optional[str]
    score: Optional[float]
    metodo: str


class MarcaMatcher:
    def __init__(self, marcas: Iterable[str], threshold: float = 88.0):
        self.threshold = float(threshold)

        # separa "OU" para tratamento especial
        self._tem_ou = any(_normalizar_texto(m) == "OU" for m in marcas)

        # prepara lista (exceto OU) para match
        prepped: List[Tuple[str, str]] = []
        for m in marcas:
            norm = _normalizar_texto(m)
            if not norm:
                continue
            if norm == "OU":
                continue
            prepped.append((m, norm))

        # ordena por tamanho desc para evitar "marca curta" ganhar de "marca longa"
        prepped.sort(key=lambda t: len(t[1]), reverse=True)
        self._prepped = prepped

        # choices para fuzzy (evita marcas curtíssimas sem dígitos)
        self._choices_fuzzy: List[Tuple[str, str]] = []
        for orig, norm in self._prepped:
            if len(norm) >= 4 or any(ch.isdigit() for ch in norm):
                self._choices_fuzzy.append((orig, norm))

        self._norm_to_orig: Dict[str, str] = {norm: orig for orig, norm in self._choices_fuzzy}

    def _match_ou_especial(self, titulo_original: str) -> bool:
        """
        Marca 'Ou' só é válida se o título tiver 'Ou' ou 'OU' com O maiúsculo.
        Não pode bater em 'ou' minúsculo.
        """
        if not self._tem_ou:
            return False
        if not titulo_original:
            return False
        # palavra isolada, O precisa ser maiúsculo e U pode ser maiúsculo ou minúsculo
        return bool(re.search(r"(?<![A-Za-zÀ-ÖØ-öø-ÿ0-9])O[uU](?![A-Za-zÀ-ÖØ-öø-ÿ0-9])", titulo_original))

    def _match_exato(self, texto: str) -> Optional[str]:
        """Match exato (contém) em texto normalizado."""
        t_norm = _normalizar_texto(texto)
        if not t_norm:
            return None

        padded = f" {t_norm} "
        for orig, m_norm in self._prepped:
            if f" {m_norm} " in padded:
                return orig
        return None

    def _match_fuzzy(self, texto: str) -> Optional[Tuple[str, float]]:
        """Fuzzy matching: retorna (marca, score) ou None."""
        if process is None or fuzz is None:
            return None
        t_norm = _normalizar_texto(texto)
        if not t_norm:
            return None
        if not self._choices_fuzzy:
            return None

        choices_norm = [n for _, n in self._choices_fuzzy]
        res = process.extractOne(t_norm, choices_norm, scorer=fuzz.WRatio)
        if not res:
            return None
        best_norm, score, _ = res
        if score >= self.threshold:
            return (self._norm_to_orig.get(best_norm, best_norm), float(score))
        return None

    def match(self, titulo: str, marca_raw: Optional[str] = None) -> MatchMarca:
        """
        Ordem:
        1) OU especial no título (case-sensitive)
        2) Exato no título
        3) Exato no marca_raw
        4) Fuzzy no marca_raw
        5) Fuzzy no título
        """
        titulo = titulo or ""
        if self._match_ou_especial(titulo):
            return MatchMarca("Ou", 100.0, "titulo_exact_ou")

        m = self._match_exato(titulo)
        if m:
            return MatchMarca(m, 100.0, "titulo_exact")

        if marca_raw:
            if self._match_ou_especial(marca_raw):
                return MatchMarca("Ou", 100.0, "raw_exact_ou")

            m2 = self._match_exato(marca_raw)
            if m2:
                return MatchMarca(m2, 100.0, "raw_exact")

            mf = self._match_fuzzy(marca_raw)
            if mf:
                return MatchMarca(mf[0], mf[1], "raw_fuzzy")

        mf2 = self._match_fuzzy(titulo)
        if mf2:
            return MatchMarca(mf2[0], mf2[1], "titulo_fuzzy")

        return MatchMarca(None, None, "sem_match")


def aplicar_matching_em_df(df, col_titulo: str = "produto", col_marca_raw: str = "marca_raw",
                           threshold: float = 88.0, caminho_marcas: Optional[Path] = None):
    """
    Adiciona colunas:
    - marca_canonica
    - marca_score
    - marca_metodo
    """
    if caminho_marcas is None:
        caminho_marcas = localizar_marcas_conhecidas()

    if caminho_marcas is None or not caminho_marcas.exists():
        # não quebra o pipeline: só adiciona colunas vazias
        df["marca_canonica"] = None
        df["marca_score"] = None
        df["marca_metodo"] = "marcas_conhecidas_nao_encontrado"
        return df

    marcas = carregar_marcas_conhecidas(caminho_marcas)
    matcher = MarcaMatcher(marcas, threshold=threshold)

    tit = df[col_titulo] if col_titulo in df.columns else None
    raw = df[col_marca_raw] if col_marca_raw in df.columns else None

    marcas_out: List[Optional[str]] = []
    scores_out: List[Optional[float]] = []
    metodos_out: List[str] = []

    if tit is None:
        # sem título, tenta com marca_raw
        for i in range(len(df)):
            marca_raw = str(raw.iloc[i]) if raw is not None and raw.iloc[i] is not None else ""
            mm = matcher.match("", marca_raw=marca_raw)
            marcas_out.append(mm.marca)
            scores_out.append(mm.score)
            metodos_out.append(mm.metodo)
    else:
        for i in range(len(df)):
            titulo = "" if tit.iloc[i] is None else str(tit.iloc[i])
            marca_raw = ""
            if raw is not None and i < len(raw) and raw.iloc[i] is not None:
                marca_raw = str(raw.iloc[i])
            mm = matcher.match(titulo, marca_raw=marca_raw)
            marcas_out.append(mm.marca)
            scores_out.append(mm.score)
            metodos_out.append(mm.metodo)

    df["marca_canonica"] = marcas_out
    # Padroniza capitalização (executivo)
    df["marca_canonica"] = [formatar_marca_titlecase(x) for x in df["marca_canonica"]]
    df["marca_score"] = scores_out
    df["marca_metodo"] = metodos_out
    return df
