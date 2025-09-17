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
from dotenv import load_dotenv


from ..app.core.db.database import async_engine, local_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = typer.Typer(pretty_exceptions_show_locals=False)


re_title_sentencia=re.compile(r".*Corte (?P<corte>.*)\. Caso (?P<caso>.*)\. (?P<tipo>.*)\. (?:Resolución|Sentencia) +del? (?:la Corte de )?(?P<fecha>.*)\.? Serie (?P<serie>.*)\. ")

async def get_info_sentencias_(documents,update):
    load_dotenv()
    async with AsyncClient('http://localhost:7700', os.getenv("MEILI_MASTER_KEY")) as client:
        index = client.index("conectividad_docs")
        if update:
            await index.update_documents(documents)
        else:
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
            document_id=int(m.group('serie').rsplit(' ',1)[-1])
            data['id']=i
            data['document_id']=document_id
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

re_pages=re.compile(r'\n\n(\d+)\n\n')
re_sections=re.compile(r'\*\*([IVXLC]+)\*\*')
re_por_tanto=re.compile(r'\*\*Por tanto,\*\*')

def segment_pages(markdown_text:str):
    # Find all page markers
    page_matches = list(re_pages.finditer(markdown_text))
    
    pages_limits=[]
    
    if not page_matches:
        # No page markers found, return entire content as page 1
        pages_limits.append(len(markdown_text))
        return pages_limits
    
    # Content before first page marker
    start_content = page_matches[0].start()
    if start_content:
        pages_limits.append(start_content)  # Page 0 for content before first page number
    
    # Process each page
    for i, match in enumerate(page_matches):
        page_num = int(match.group(1))
        
        # Start position after the page marker
        content_start = match.end()
        
        # End position (start of next page marker or end of document)
        if i < len(page_matches) - 1:
            content_end = page_matches[i + 1].start()
        else:
            content_end = len(markdown_text)
        
        pages_limits.append(content_end)
    
    return pages_limits

def segment_sections(markdown_text:str):
    # Find all page markers
    bits=list(re_por_tanto.finditer(markdown_text))
    sections = []
    conclusion_section=(bits[0].start(),len(markdown_text))
    markdown_text=markdown_text[:conclusion_section[0]]
    section_matches = list(re_sections.finditer(markdown_text))
 
    
    if not section_matches:
        # No page markers found, return entire content as page 1
        sections = [('',0,len(markdown_text))]
        return sections
    
    # Content before first page marker
    start_content = section_matches[0].start()
    if start_content:
        sections.append(('preambule',0,start_content))  # Page 0 for content before first page number
    
    # Process each page
    for i, match in enumerate(section_matches):
        section_num = match.group(1)
        
        # Start position after the page marker
        content_start = match.end()
        
        # End position (start of next page marker or end of document)
        if i < len(section_matches) - 1:
            content_end = section_matches[i + 1].start()
        else:
            content_end = len(markdown_text)
       
        sections.append((section_num,content_start,content_end))

    sections.append(('conclusion',conclusion_section[0],conclusion_section[1]))
    return sections


def extract_first_section(markdown_text:str,page_limits):
    documents=[]
    print(markdown_text)
    return documents

def extract_elements(md:str):
    documents=[]
    page_limits=segment_pages(md)
    sections=segment_sections(md)

    print(page_limits)
    print(sections)
    
    elements=extract_first_section(md[sections[0][1]:sections[0][2]],page_limits)
    print(elements)

    return documents
    

async def extract_sentencias_():
    load_dotenv()
    async with AsyncClient('http://localhost:7700', os.getenv("MEILI_MASTER_KEY")) as client:
        index = client.index("conectividad_docs")

        docs=await index.get_documents(
            filter="type = 'description'",
            sort=["document_id:asc"],
            limit=3000)

        documents=[]
        for doc in track(docs.results):
            file_path=download_file(doc['links']['pdf'],'/tmp',simulate=False)
            original = pymupdf4llm.to_markdown(file_path)
            data={}
            data={'document_id':doc['document_id'],
                  'text':original,
                  'type':'original'}
            documents.append(data)
            documents_=extract_elements(original)
            #print(original)
            break
        
    return None

@app.command()
def extract_sentencias(update: bool = False):
    """Extract sentencias, create records in database

    Parameters:

    Returns:

    None"""
    loop = asyncio.get_event_loop()
    loop.run_until_complete(extract_sentencias_())



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


if __name__ == "__main__":
   app()


    

