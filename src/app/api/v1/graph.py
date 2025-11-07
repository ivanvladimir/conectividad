from typing import Annotated, Any, cast, AsyncGenerator

from fastapi import APIRouter, Depends, Request, Form
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
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

router = APIRouter(tags=["graph"])


def create_graph(nodes, edges, name="graph",self_loops=True, multi= True, graph_type='directed'):
    graph = {
        "nodes": nodes,
        "edges": edges
    }
    return jsonable_encoder(graph)

@router.post("/graph")
async def api_graph(
    request: Request,
) -> JSONResponse:
    response = JSONResponse(content={})
    return response
 
@router.post("/graph/sentence")
async def api_graph_sentence(
    request: Request,
    searchdb: Annotated[AsyncGenerator, Depends(async_get_search)],
    sentence_num: int = 1,
) -> JSONResponse:
    query_params = request.query_params
    sentence_num = int(query_params.get("num", 1))

    async with searchdb as client:
        index = client.index("conectividad_graph")
        main_node = await index.get_documents(
            filter=f'sentence_num = {sentence_num} AND type = "sentence"',
            limit=1,
        )
        main_node = [{"id": n["id"], 
                      "data": {"name": n["name"]}, 
                      "style" :{
                        "size": 6,
                        "fill": "green",
                        "labelText": n["name"],
                        }
                      } for n in main_node.results]

        links = await index.get_documents(
            filter=f'source = {main_node[0]['id']} AND type = "link"',
            limit=3000
        )

        ids=set(l['target'] for l in links.results)

        rest_nodes =  await index.get_documents(
            filter=f'id IN [{",".join(map(str,ids)) }] AND type = "citation"',
            limit=3000
        )

        rest_nodes = [{"id": n["id"], 
                       "data": {"label": n["name"]}, 
                       "style": {
                            "size": 5,
                            "fill": "yellow",
                            "labelText": n["name"],
                       }
                       } for n in rest_nodes.results]
        rest_nodes.append(main_node[0])

        links = [{ "source": l['source'], "target":l['target'], "data": {"type": "cites"}, "style":{"lineWidth": l['count']}} for l in links.results]
        data = create_graph(rest_nodes, links)

        response = JSONResponse(content=data)
    return response

@router.post("/graph/filtered")
async def api_graph_filtered(
    request: Request,
    searchdb: Annotated[AsyncGenerator, Depends(async_get_search)],
) -> JSONResponse:
    query_params = request.query_params
    filter_parts=[]
    for key in query_params.keys():
        print(key)
        value = query_params.get(key)
        if key == "min_count":
            filter_parts.append(f'count >= {value}')
        elif key == "max_count":
            filter_parts.append(f'count <= {value}')
        else:
            filter_parts.append(f'{key} = "{value}"')
    filter_str = " AND ".join(filter_parts)

    async with searchdb as client:
        index = client.index("conectividad_graph")
        links = await index.get_documents(
            filter=f'type = "link" AND {filter_str}' if len(filter_str) > 0 else 'type = "link"',
            limit=1000,
        )

        nodes_ids=set(int(l['source']) for l in links.results).union(set( int(l['target']) for l in links.results))
        nodes = await index.get_documents(ids=list(nodes_ids), limit=len(nodes_ids)+10)

        nodes = [{"id": n["id"], 
                    "data": {"label": n["name"]}, 
                    "style": {
                        "size": 5,
                        "fill": "yellow" if n["type"] == "citation" else "red",
                        "labelText": n["name"],
                    }
                    } for n in nodes.results]

        links = [{ "source": l['source'], "target":l['target'], "data": {"type": "cites"}, "style":{"lineWidth": l['count']}} for l in links.results]
        data = create_graph(nodes, links)

        response = JSONResponse(content=data)
    return response



