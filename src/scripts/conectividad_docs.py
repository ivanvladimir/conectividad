import typer
import asyncio
import logging
import uuid
import re
import os
import dateparser
import requests
from rich.progress import track
from datetime import UTC, datetime
from playwright.sync_api import sync_playwright, expect

from meilisearch_python_sdk import AsyncClient
import pymupdf4llm
from marker.converters.pdf import PdfConverter
from marker.models import create_model_dict
from marker.output import text_from_rendered
from marker.config.parser import ConfigParser
import json
from dotenv import load_dotenv


from ..app.core.db.database import async_engine, local_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = typer.Typer(pretty_exceptions_show_locals=False)


re_title_sentencia=re.compile(r".*Corte (?P<corte>.*)\. Caso (?P<caso>.*)\. (?P<tipo>.*)\. (?:Resolución|Sentencia) +del? (?:la Corte de )?(?P<fecha>.*)\.? Serie (?P<serie>.*\d)\.?")

async def get_info_sentencias_(documents,update):
    load_dotenv()
    async with AsyncClient('http://localhost:7700', os.getenv("MEILI_MASTER_KEY")) as client:
        index = client.index("conectividad_docs")
        stats = await index.get_stats()
        for i,doc in enumerate(documents):
                doc['document_id']=stats.number_of_documents+i+1
        await index.add_documents(documents)
    return None

def crawl_sentencias_(main_url: str, update: bool = False):
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto(main_url)
        print("Waiting for results to show")
        page.wait_for_function('document.querySelectorAll("li.search-result").length > 2')

        documents=[]

        li_elements = page.locator('li.search-result').all()
        print("Total de sentencias",len(li_elements))
        for i in track(range(len(li_elements)), description="Crawling sentencias..."):
            data={}
            li = li_elements[i]
            full_text = li.text_content()
            m = re_title_sentencia.search(full_text)
            if not m:
                print(i, "Error",full_text.strip())
            sentence_num=int(m.group('serie').rsplit(' ',1)[-1])
            data['document_id']=sentence_num
            data['sentence_num']=sentence_num
            data['links']={}
            data.update(m.groupdict())
            tr_elements = li.locator('tr').all()
            flag_other_lang=False
            for i, tr in enumerate(tr_elements):
                tds = tr.locator('td').all()
                td_0 = tr.nth(0).text_content().strip()
                flag_other_lang = True if tr.text_content().strip().startswith('Inglés') else False
                if len(tds)<2:
                    continue
                if i==0:
                    links = tr.locator('a').all()
                    for j, link in enumerate(links):
                        href = link.get_attribute('href')
                        if href.endswith('.pdf'):
                            data['links']['pdf']=href
                        if href.endswith('.doc') or href.endswith('.docx'):
                            data['links']['doc']=href
                elif td_0.startswith('Resumen'):
                    links = tds[1].locator('a').all()
                    data['links']['resumen']=links[0].get_attribute('href').strip()
                elif not flag_other_lang:
                    links = tds[1].locator('a').all()
                    if len(links)>0:
                        data['links'][td_0]=links[0].get_attribute('href').strip()
            d=dateparser.parse(data['fecha'])
            if d:
                data['date']=d.isoformat()
            else:
                print(data['fecha'])
            data['type']="description"
            documents.append(data)
        browser.close()
    return documents

@app.command()
def get_info_sentencias(main_url: str = "https://www.corteidh.or.cr/casos_sentencias.cfm", update: bool = False):
    """Gets sentencias from the main page 

    Parameters:

    main_url(str): URL of the main page to crawl.
    update(bool): If True, updates the database with new sentencias.

    Returns:

    None"""
    sentencias=crawl_sentencias_(main_url=main_url)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(get_info_sentencias_(sentencias,update))

def download_file(url,odir,simulate=False):
    local_filename = url.split('/')[-1]
    # NOTE the stream=True parameter
    if not simulate:
        r = requests.get(url, stream=True)
        with open(os.path.join(odir,local_filename), 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk: # filter out keep-alive new chunks
                    f.write(chunk)
    return os.path.join(odir,local_filename)

re_page = re.compile(r'/page/(?P<page>\d+)/.*')

    
def flatten_blocks(block, data={},parent_page=None):
    """
    Recursively flatten all children blocks.
    """
    flattened = []
    clean = re.compile('<.*?>')
    
    # Get block type
    block_type = block.block_type if hasattr(block, 'block_type') else 'Unknown'
    block_id =  block.id if hasattr(block, 'id') else None
    m= re_page.search(block_id) if block_id else None
    if m:
        parent_page = int(m.group('page'))
    else:
        parent_page = None
    page_num = block.page_id if hasattr(block, 'page_id') else parent_page
    
    # Convert block to dict
    if hasattr(block, 'model_dump'):
        block_dict = block.model_dump()
    elif hasattr(block, 'dict'):
        block_dict = block.dict()
    else:
        block_dict = {}

    text= re.sub(clean, '', block.html if hasattr(block, 'html') else "")
    if len(text.strip())!=0:
        # Extract block information
        block_data = {
            "page": page_num,
            "block_type": block_type,
            "type":"element",
            "html": block.html if hasattr(block, 'html') else "",
            "text": text
        }
        block_data.update(data)
        
        flattened.append(block_data)
    # Recursively process children
    if hasattr(block, 'children') and block.children:
        for child in block.children:
            flattened.extend(flatten_blocks(child, data, page_num))
    
    return flattened

async def extract_sentencias_(ini, fin, update):
    load_dotenv()

    model_dict = create_model_dict()

    # Configure conversion parameters
    config = {
        "output_format": 'json',
    }

    # Create converter
    config_parser = ConfigParser(config)
    config_dict = config_parser.generate_config_dict()
    config_dict["pdftext_workers"] = 1
    converter = PdfConverter(
        config=config_dict,
        artifact_dict=model_dict,
        renderer=config_parser.get_renderer(),
    )

    async with AsyncClient('http://localhost:7700', os.getenv("MEILI_MASTER_KEY")) as client:
        index = client.index("conectividad_docs")

        docs=await index.get_documents(
            filter="type = 'description'",
            sort=["sentence_num:asc"],
            limit=3000)


        for doc in track(docs.results):
            if ini and doc['sentence_num']<ini:
                continue
            if fin and doc['sentence_num']>fin:
                break
            documents=[]
            file_path=download_file(doc['links']['pdf'],'src/data/',simulate=False)
            rendered = converter(file_path)
    
            extracted_blocks = []
            original = pymupdf4llm.to_markdown(file_path)
            with open(f'src/data/{doc["sentence_num"]}.md','w') as f:
                f.write(original)
            data={}
            data={'sentence_num':doc['sentence_num'],
                  'text':original,
                  'filenames': {'pdf': file_path},
                  'type':'original'}

            if hasattr(rendered, 'children'):
                root_blocks = rendered.children
            else:
                print("Available attributes:", dir(rendered))
                raise AttributeError("Cannot find blocks in rendered output")
            
            # Recursively flatten all blocks
            for block in root_blocks:
                page_num = block.page_id if hasattr(block, 'page_id') else None
                extracted_blocks.extend(flatten_blocks(block,{'sentence_num':doc['sentence_num']},page_num))

            documents.append(data)
            documents.extend(extracted_blocks)

            stats = await index.get_stats()
            for i,doc in enumerate(documents):
                doc['document_id']=stats.number_of_documents+i+1
                doc['oder']=i

            print(">>>> ", documents)
            await index.add_documents(documents)
        
    return None

@app.command()
def extract_sentencias(ini: int = None, fin: int = None, update: bool = False):
    """Extract sentencias, create records in database

    Parameters:

    Returns:

    None"""
    loop = asyncio.get_event_loop()
    loop.run_until_complete(extract_sentencias_(ini, fin, update))

re_section = re.compile(r'>\s*(?P<section>[IVXL]+)\s*<')
re_portanto = re.compile(r'^(?:<h\d><b>Por tanto,?\s+</b></h\d>|<h\d>\d+.<b>\s+POR\s+TANTO,\s+</b></h\d>)$')
re_par = re.compile(r'^<li block-type="ListItem">(?P<num>\d+)\.? ')
async def update_metadata_(ini, fin, update):
    load_dotenv()

    async with AsyncClient('http://localhost:7700', os.getenv("MEILI_MASTER_KEY")) as client:
        index = client.index("conectividad_docs")

        docs=await index.get_documents(
            filter="type = 'description'",
            sort=["sentence_num:asc"],
            limit=3000)

        for i,doc in track(enumerate(docs.results)):
            if ini and i+1<ini:
                continue
            if fin and i+1>fin:
                break
            section="preamble"
            par=None

            eles=await index.get_documents(
                filter=f'type = "element" AND sentence_num = {doc["sentence_num"]}',
                limit=5000)
            eles_=[]
            for ele in eles.results:
                print(">>",ele['text'],ele)
                m=re_section.search(ele['html'])
                if m:
                    section=m.group('section')
                else:
                    m=re_portanto.match(ele['html'])
                    if m:
                        section="conclusion"
                        par=None
                    else:
                        m=re_par.match(ele['html'])
                        if m:
                            par_=int(m.group('num').strip())
                            if (par and par_>par) or not par:
                                par=par_
                ele=ele.copy()
                ele['section']=section
                if not section in ["conclusion"] and par:
                    ele['num_par']=par
                eles_.append(ele)
            await index.update_documents(eles_)

@app.command()
def update_metadata(ini: int = None, fin: int = None, update: bool = False):
    """Update metadata for sentencias

    Parameters:

    Returns:

    None"""
    loop = asyncio.get_event_loop()
    loop.run_until_complete(update_metadata_(ini, fin, update))



async def add_filter_(filter:str):
    """ Adds filter for the database async

    Parameters:

    filter(str) Column of the database to allow to look for.

    Returns:

    None"""
    load_dotenv()

    async with AsyncClient('http://localhost:7700', os.getenv("MEILI_MASTER_KEY")) as client:
        index = client.index("conectividad_docs")
        results=await index.get_filterable_attributes()
        if results:
            await index.update_filterable_attributes(results+filter.split(","))
        else:
            await index.update_filterable_attributes(filter.split(","))


@app.command()
def add_filter(filter:str):
    """ Adds filter for the database

    Parameters:

    filter(str) Column of the database to allow to look for.

    Returns:

    None"""
 
    loop = asyncio.get_event_loop()
    loop.run_until_complete(add_filter_(filter))

async def add_sortable_(sortable:str):
    """ Adds _sortable_ for the database async

    Parameters:

    sortable(str) Column of the database to allow to look for.

    Returns:

    None"""
    load_dotenv()

    async with AsyncClient('http://localhost:7700', os.getenv("MEILI_MASTER_KEY")) as client:
        index = client.index("conectividad_docs")
        results=await index.get_sortable_attributes()
        await index.update_sortable_attributes(results+sortable.split(","))

@app.command()
def add_sortable(sortable:str):
    """ Adds _sortable_ for the database

    Parameters:

    sortable(str) Column of the database to allow to look for.

    Returns:

    None"""
 
    loop = asyncio.get_event_loop()
    loop.run_until_complete(add_sortable_(sortable))


async def show_info_():
    """ Show filter for the database async

    Parameters:

    filter(str) Column of the database to allow to look for.

    Returns:

    None"""
    load_dotenv()

    async with AsyncClient('http://localhost:7700', os.getenv("MEILI_MASTER_KEY")) as client:
        index = client.index("conectividad_docs")
        results=await index.get_filterable_attributes()
        print(f"Attibutos filterable: {", ".join(results)}")
        results=await index.get_sortable_attributes()
        print(f"Attibutos sortable: {", ".join(results)}")


@app.command()
def show_info():
    """ Shows filter for the database

    Parameters:

    filter(str) Column of the database to allow to look for.

    Returns:

    None"""
 
    loop = asyncio.get_event_loop()
    loop.run_until_complete(show_info_())

async def delete_all_():
    """ Delete all the documents

    Parameters:

    Returns:

    None"""
    load_dotenv()

    async with AsyncClient('http://localhost:7700', os.getenv("MEILI_MASTER_KEY")) as client:
        index = client.index("conectividad_docs")
        task =  await index.delete_all_documents()


@app.command()
def delete_all():
    """ Delete all the documents

    Parameters:

    Returns:

    None"""
 
    loop = asyncio.get_event_loop()
    loop.run_until_complete(delete_all_())


if __name__ == "__main__":
   app()


    

