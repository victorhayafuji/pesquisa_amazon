"""Configurações do módulo Amazon Search (SearchAPI).

Objetivo:
- Ser autocontido para evitar divergências com o módulo do Google Shopping.
- Manter padrões de projeto: cliente -> processador -> main -> CSV em outputs/.

Observação:
- A documentação oficial descreve parâmetros como `q`, `amazon_domain`, `language` e filtros via `rh`.
"""

from __future__ import annotations

from pathlib import Path

# Diretórios
PROJETO_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = PROJETO_DIR / "outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# SearchAPI
SEARCHAPI_BASE_URL = "https://www.searchapi.io/api/v1/search"
ENGINE_AMAZON_SEARCH = "amazon_search"

# Defaults (ajuste conforme seu mercado / necessidade)
AMAZON_DOMAIN_DEFAULT = "amazon.com.br"   # A doc indica amazon.com como default
LANGUAGE_DEFAULT = "pt_BR"             # pode variar por domínio/idioma suportado

# Paginação (depende do provedor; mantemos como parâmetro de projeto)
N_PAGINAS_DEFAULT = 3
