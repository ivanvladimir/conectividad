
# Conectividad

Webapp for the Conectividad project.

## Installing

To install use uv

    git clone
    cd conectividad
    uv sync

### Install meilisearch with podman-compose

Build image

    uvx podman-compose build meilisearch

Prepare the database indices and fill it. It requieres the folders: _data_ e _imgs_ and csv file _age_information.csv_ in _data_ folder:

1. Fill database with information from sentences `uv run python -m src.scripts.conectividad_docs get-info-sentencias`
2. Set filterable attributes for textual db  `uv run python -m src.scripts.conectividad_docs add-filterable type, sentence_num`
3. Set sortable attributes for textual db `uv run python -m src.scripts.conectividad_docs add-sortable document_id, order, sentence_num`
4. Extract information of 10 first sentences `uv run python -m src.scripts.conectividad_docs extract-sentencias --ini 0 --fin 10` (mins)
5. Extract information of rest sentences `uv run python -m src.scripts.conectividad_docs extract-sentencias --ini 10` (hours)

## Run for development

First run meilisearch

    uvx podman-compose up meilisearch

Second run Webapp

    uv run uvicorn src.app.main:app --reload
