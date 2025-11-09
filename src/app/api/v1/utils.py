from typing import Annotated, Any, cast, AsyncGenerator

from fastapi import APIRouter, Depends, Request, Form
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession

from ...api.dependencies import get_current_superuser, get_current_user
from ...core.db.database import async_get_db
from ...core.db.searchdocs import async_get_search
from ...core.exceptions.http_exceptions import ForbiddenException, NotFoundException
from ...core.utils.cache import cache
from ...crud.crud_users import crud_users
from ...schemas.user import UserRead
import re
import json
import markdown
import os

re_emph = re.compile(r"(<em>.*?</em>)")

templates = Jinja2Templates(directory="src/app/api/templates")

router = APIRouter(tags=["utils"])

@router.post("/search", status_code=201)
async def api_search(
    request: Request,
    query: Annotated[str, Form()],
    db: Annotated[AsyncSession, Depends(async_get_db)],
    searchdb: Annotated[AsyncGenerator, Depends(async_get_search)],
    page: int = 0,
    num_words: int = 15,
) -> HTMLResponse:

    if query:
        async with searchdb as client:
            index = client.index("conectividad_docs")
            data = await index.search(
                query,
                filter='type = "element"',
                page=page + 1,
                limit=20,
                attributes_to_highlight=["text"],
                attributes_to_search_on=["text"],
            )
            res = data
    else:
        res = {}



    results = [
        dict(d["_formatted"]) for d in res.hits
    ]

    results_ = []
    for r_ in results:
        for m in re_emph.finditer(r_["text"]):
            results_.append(r_.copy())
            r = results_[-1]
            text_before = (
                r["text"][: m.start()].replace("<em>", "").replace("</em>", "")
            )
            r["infix"] = r["text"][m.start() + 4 : m.end() - 5]
            text_after = r["text"][m.end() :].replace("<em>", "").replace("</em>", "")
            r["prefix"] = " ".join(
                text_before.split()[-num_words:] if text_before.split() else []
            )
            r["sufix"] = " ".join(
                text_after.split()[:num_words] if text_after.split() else []
            )
            r['hit']=r['infix']
            r['page']=int(r_['page'])
            r['polygon']=json.dumps([ {"x":float(l[0]), "y":float(l[1])} for l in r['polygon']])

    response = templates.TemplateResponse(
        request=request,
        name="search_results.html",
        context={
            "results": results_,
            "query": query,
            "page": page,
            "last_page": (page + 1) * 20 > res.total_hits,
        },
    )
    return response

@router.post("/docs", status_code=201)
async def api_docs(
    request: Request,
    db: Annotated[AsyncSession, Depends(async_get_db)],
    searchdb: Annotated[AsyncGenerator, Depends(async_get_search)],
    page: int = 0,
) -> HTMLResponse:

    page_size=60
    async with searchdb as client:
        index = client.index("conectividad_docs")
        docs = await index.get_documents(
            filter='type = "description"',
            offset=page*page_size,
            limit=page_size,
            sort=['sentence_num:desc']
        )

    response = templates.TemplateResponse(
        request=request,
        name="documents.html",
        context={
            "results": docs.results,
            "page": page,
            "last_page": ((page + 1) * page_size + 1) > docs.total,
            'active_page':'docs',
        },
    )
    return response

@router.post("/doc/{sentence_num}", status_code=201)
async def api_doc(
    request: Request,
    db: Annotated[AsyncSession, Depends(async_get_db)],
    searchdb: Annotated[AsyncGenerator, Depends(async_get_search)],
    sentence_num: int = 0,
) -> HTMLResponse:

    async with searchdb as client:
        index = client.index("conectividad_docs")
        doc_info = await index.get_documents(
            filter=f'type = "description" AND sentence_num = "{sentence_num}"',
            limit=1,
        )

        doc = await index.get_documents(
            filter=f'type = "original" AND sentence_num = {sentence_num}',
            limit=1
        )
        elements = await index.get_documents(
            filter=f'type = "element" AND sentence_num = "{sentence_num}"',
            limit=3000
        )

        doc = doc.results[0] if doc.results else {'text':""}

    response = templates.TemplateResponse(
        request=request,
        name="document_info.html",
        context={
            "sentence_num": sentence_num,
            "doc_info": doc_info.results[0] if doc_info.results else {},
            "elements": elements.results,
            "doc": doc,
        },
    )
    return response

@router.post("/general_stats", status_code=201)
async def api_general_stats(
    request: Request,
    db: Annotated[AsyncSession, Depends(async_get_db)],
    searchdb: Annotated[AsyncGenerator, Depends(async_get_search)],
) -> HTMLResponse:

    async with searchdb as client:
        index = client.index("conectividad_docs")
        facets = await index.search(
            "", facets=['type']
        )
        index_g = client.index("conectividad_graph")
        facets_g = await index_g.search(
            "", facets=['type']
        )
    response = templates.TemplateResponse(
        request=request,
        name="stats_general.html",
        context={
            'total_types':facets.facet_distribution['type'],
            'total_graphs':facets_g.facet_distribution['type']
        },
    )
    return response

@router.post("/per_country_stats", status_code=201)
async def api_per_country_stats(
    request: Request,
    db: Annotated[AsyncSession, Depends(async_get_db)],
    searchdb: Annotated[AsyncGenerator, Depends(async_get_search)],
) -> HTMLResponse:

    async with searchdb as client:
        index = client.index("conectividad_docs")
        facets = await index.search(
            "", facets=['country']
        )

        index_g = client.index("conectividad_graph")
        facets_g = await index_g.search(
            "", filter="type='sentence'", facets=['country'],
            limit=3000
        )
    

    total_docs = sum(facets.facet_distribution['country'].values())
    total_nodes = sum(facets_g.facet_distribution['country'].values())

    response = templates.TemplateResponse(
        request=request,
        name="stats_per_country.html",
        context={
            'total_countries':facets.facet_distribution['country'],
            'total_nodes_country':facets_g.facet_distribution['country'],
            'total_sentences':total_docs,
            'total_nodes':total_nodes
        },
    )
    return response



@router.post("/per_document_stats", status_code=201)
async def api_per_document_stats(
    request: Request,
    db: Annotated[AsyncSession, Depends(async_get_db)],
    searchdb: Annotated[AsyncGenerator, Depends(async_get_search)],
) -> HTMLResponse:

    async with searchdb as client:
        index = client.index("conectividad_docs")
        index_g = client.index("conectividad_graph")

        docs= await index.get_documents(filter='type="description"', sort=['sentence_num:desc'], limit=3000)
        docs_= [d['sentence_num'] for d in docs.results]
        originals= await index.get_documents(filter='type="original"', limit=3000)
        originals_={d['sentence_num']: True for d in originals.results}
        elements= await index.get_documents(filter='type = "element" AND oder = 1', limit=3000)
        elements_={d['sentence_num']: True for d in elements.results}
        nodes= await index_g.get_documents(filter='type="sentence"', limit=3000)
        nodes_={d['sentence_num']: True for d in nodes.results}

        pdf_files = set(os.listdir('src/data'))
        pdf_files = {d['sentence_num']:True for d in docs.results if d['links']['pdf'].split('/')[-1] in pdf_files}

        detail=[ (sentence_num, 
                 True if sentence_num in originals_ else False,
                 True if sentence_num in elements_ else False,
                 True if sentence_num in nodes_ else False,
                 True if sentence_num in pdf_files else False,
                  )    for sentence_num in docs_]
    response = templates.TemplateResponse(
        request=request,
        name="stats_per_document.html",
        context={
            'detail': detail,
            'active_page':'info'
        },
    )
    return response



  
@router.get("/pdf/{sentence_num}", response_class=FileResponse)
async def api_pdf(
    request: Request,
    db: Annotated[AsyncSession, Depends(async_get_db)],
    searchdb: Annotated[AsyncGenerator, Depends(async_get_search)],
    sentence_num: int = 0,
) -> FileResponse:

    async with searchdb as client:
        index = client.index("conectividad_docs")
        doc = await index.get_documents(
            filter=f'type = "description" AND sentence_num = "{sentence_num}"',
            limit=1,
        )

    if len(doc.results)==0:
        raise HTTPException(status_code=404, detail=f"Pdf for sentece num {senence_num} not found")

    return FileResponse(doc.results[0]["filenames"]['pdf'], media_type="application/pdf")


