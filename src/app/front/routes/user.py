import time
import os
import markdown

from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

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


