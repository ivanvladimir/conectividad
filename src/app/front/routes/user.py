import time
import os
import markdown
import json

from fastapi import APIRouter, Request, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from typing import Optional

templates = Jinja2Templates(directory="src/app/front/templates")

router = APIRouter()

@router.get("/")
async def main(request: Request) -> HTMLResponse:
    """
    Principal
    """
    start_time = time.time()

    if os.path.exists(os.path.join("./src/app/resources", f"main.md")):
        content = open(os.path.join("./src/app/resources", f"main.md")).read()
        md = markdown.Markdown(extensions=["meta", "tables"])
        content = md.convert(content)
        response = templates.TemplateResponse(
            request=request,
            name="public/page.html",
            context={
                "content": content,
                "metadata": md.Meta,
                "active_page":'main',
                "elapsed_time_seconds": f"{time.time() - start_time:2.3f}",
            },
        )
        return response
    else:
        raise HTTPException(status_code=404, detail="Page not found")

@router.get("/search")
async def search(request: Request) -> HTMLResponse:
    """
    Main user page
    """
    start_time = time.time()
    response = templates.TemplateResponse(
        request=request,
        name="user/search.html",
        context={"elapsed_time_seconds": f"{time.time() - start_time:2.3f}",
                 "active_page":'search'},
    )
    return response

@router.get("/documents")
async def docs(request: Request) -> HTMLResponse:
    """
    Main user page
    """
    start_time = time.time()
    response = templates.TemplateResponse(
        request=request,
        name="user/documents.html",
        context={"elapsed_time_seconds": f"{time.time() - start_time:2.3f}",
                 "active_page":'docs'},
    )
    return response

@router.get("/document/{sentence_num}")
async def doc(request: Request,
    sentence_num: int = 1,
    page: int = 1,
    q: Optional[str] = Query(None),
    polygon: Optional[str] = Query(None)
               ) -> HTMLResponse:
    """
    Main user page
    """
    start_time = time.time()
    if not polygon is None:
        polygon = json.loads(polygon)

    response = templates.TemplateResponse(
        request=request,
        name="user/document.html",
        context={"elapsed_time_seconds": f"{time.time() - start_time:2.3f}",
                 "sentence_num": sentence_num,
                 "page": page,
                 "query": q,
                 "polygon": polygon,
                 "active_page":'docs'},
    )
    return response


@router.get("/stats")
async def stats(request: Request) -> HTMLResponse:
    """
    Display statistics about the sentences
    """
    start_time = time.time()

    response = templates.TemplateResponse(
        request=request,
        name="user/stats.html",
        context={"elapsed_time_seconds": f"{time.time() - start_time:2.3f}"},
    )
    return response

@router.get("/graph/{kind}")
async def graph(
    request: Request,
    kind: str = 'sentence',
    ) -> HTMLResponse:
    """
    Grafica de grafo
    """
    start_time = time.time()
    response = templates.TemplateResponse(
        request=request,
        name="user/graph.html",
        context={"elapsed_time_seconds": f"{time.time() - start_time:2.3f}",
                 "query_params": request.query_params,
                 "kind": kind,
                 "active_page":'graph'},
    )
    return response

@router.get("/graphs_per_country")
async def graphs_per_country(
    request: Request,
    ) -> HTMLResponse:
    """
    Grafica por país
    """
    start_time = time.time()
    response = templates.TemplateResponse(
        request=request,
        name="user/graphs_per_country.html",
        context={"elapsed_time_seconds": f"{time.time() - start_time:2.3f}",
                 "active_page":'graph'},
    )
    return response
