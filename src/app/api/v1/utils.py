from typing import Annotated, Any, cast, AsyncGenerator

from fastapi import APIRouter, Depends, Request, Form
from fastapi.responses import HTMLResponse
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
import markdown

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
                filter='type = "parr"',
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
    for r in results:
        for m in re_emph.finditer(r["text"]):
            results_.append(r.copy())
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

    md = markdown.Markdown(extensions=["meta", "tables"])
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
        doc = doc.results[0] if doc.results else {'text':""}
        doc['text']=md.convert(doc['text'])

    response = templates.TemplateResponse(
        request=request,
        name="document_info.html",
        context={
            "doc_info": doc_info.results[0] if doc_info.results else {},
            "doc": doc,
        },
    )
    return response

   

