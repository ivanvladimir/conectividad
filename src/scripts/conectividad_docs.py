import typer
import asyncio
import logging
import uuid
import re
import os
import sys
import time
import hashlib
import html
import dateparser
import requests
from rich.progress import track
from datetime import UTC, datetime
from playwright.async_api import async_playwright, expect

from meilisearch_python_sdk import AsyncClient
import pymupdf4llm
from marker.converters.pdf import PdfConverter
from marker.models import create_model_dict
from marker.output import text_from_rendered
from marker.config.parser import ConfigParser
import json
from openai import OpenAI
from pydantic import BaseModel
from typing import Optional, List
from dotenv import load_dotenv
from collections import Counter

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

def download_file(url, odir, simulate=False):
    local_filename = url.split('/')[-1]
    output_path=os.path.join(odir,local_filename)
    return output_path

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
        try:
            block_dict = block.model_dump()
        except TypeError:
            print(block)
            sys.exit(1)
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
            "text": text,
            "polygon": block.polygon if hasattr(block, 'polygon') else None,
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
            if not file_path:
                print("Error downloading file for sentencia",doc['sentence_num'])
                continue
            rendered = converter(file_path)
    
            extracted_blocks = []
            original = pymupdf4llm.to_markdown(file_path)
            with open(f'src/data/{doc["sentence_num"]}.md','w') as f:
                f.write(original)
            data={}
            data={'sentence_num':doc['sentence_num'],
                  'text':original,
                  'type':'original'}

            doc.update({'filenames': {'pdf': file_path}})
            await index.update_documents([doc])

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
re_country = re.compile(r'[vV]s\.? (?P<country>.*)$')
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
            m=re_country.search(doc['caso'])
            if m:
                doc['country']=m.group('country').strip()
            else:
                doc['country']=None
            eles_.append(doc)

            await index.update_documents(eles_, primary_key = "document_id")

@app.command()
def update_metadata(ini: int = None, fin: int = None, update: bool = False):
    """Update metadata for sentencias

    Parameters:

    Returns:

    None"""
    loop = asyncio.get_event_loop()
    loop.run_until_complete(update_metadata_(ini, fin, update))


class Article(BaseModel):
    number_article: list[str]
    number_paragraphs: Optional[list[str]]

class Citation(BaseModel):
    match_text: str
    law_name: str
    date: Optional[str]
    articles: List[Article]
    additional_info: Optional[str]

class Citations(BaseModel):
    citations: List[Citation]

async def create_graph_(ini, fin, update):
    load_dotenv()

    gpt = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    
    with open('cached_citations.json') as f:
        cached_citations = json.load(f)

    async with AsyncClient('http://localhost:7700', os.getenv("MEILI_MASTER_KEY")) as client:
        index_docs = client.index("conectividad_docs")
        try:
            index = client.index("conectividad_graph")
            await index.get_documents(
                filter=f"name = 'AAAA'",
                limit=1)
        except:
            index = await client.create_index('conectividad_graph', primary_key= 'id')
            index = client.index("conectividad_graph")

        docs=await index_docs.get_documents(
            filter="type = 'description'",
            sort=["sentence_num:asc"],
            limit=3000)

        for i,doc in enumerate(docs.results):
            nodes=({},{})
            links=({},{})

            sentence_num=doc['sentence_num']
            if ini and sentence_num<ini:
                continue
            if fin and sentence_num>fin:
                break

            print("Sentence num",sentence_num)
            node=await index.get_documents(
                filter=f"sentence_num = {sentence_num} AND type = 'sentence'",
                limit=1)
            if len(node.results)==0:
                d=datetime.fromisoformat(doc['date'])
                node={
                    'type':'sentence',
                    'sentence_num':doc['sentence_num'],
                    'country':doc.get('country',None),
                    'name':html.escape(doc.get('caso',None)),
                    'year':d.year,
                    'count':1,
                }
                new_node_flag=True
            else:
                new_node_flag=False
                node= node.results[0]
                node['count']+=1
            source=node

            eles=await index_docs.get_documents(
                filter=f'type = "element" AND sentence_num = {sentence_num}',
                limit=10000)
            eles_=[]
            for i,ele in track(enumerate(eles.results), total=len(eles.results), description="Processing elements..."):
                id_string=hashlib.sha256(ele['text'].encode()).hexdigest()
                if id_string in cached_citations:
                    citations=cached_citations[id_string]
                else:
                    completion = gpt.chat.completions.parse(
                        model="gpt-4o-mini",
                        messages=[
                            {"role": "system", "content": "You are a helpful legal assistant that extracts legal citations from text."},
                            {"role": "user", 
                             "content": 
                                "Extract all legal citations from the following legal text in Spanish. Extract the name of the mentioned law in the law_name field. For the articles, extract only the article numbers into a list in the articles field. In case they use a dot format, report it in that format (e.g., 10.4). Do not include any textual information in articles. If a paragraph is explicitly mentioned, please associate it with the right article; only report the number, without any textual information. If the law mentions any other information that does not belong to any field, add it to the additional_info field, but it has to appear in the text. In match_text, put the minimal text from which the data is being extracted. If no citations are found, return an empty array.\n\nText: '''"+ele['text']+"'''"}

                        ],
                        response_format=Citations,
                    )
                    citations=completion.choices[0].message.parsed
                    time.sleep(1)
                    citations=citations.model_dump()
                    cached_citations[id_string]=citations
                if i%2==0:
                    json.dump(cached_citations, open('cached_citations.json','w'), indent=2)
                for c in citations['citations']:
                    citation_name=html.escape(f"{c['law_name']} {c['date']}" if c['date'] else f"{c['law_name']}")
                    node=await index.get_documents(
                        filter=f'name="{citation_name}"',
                        limit=1)

                    if len(node.results)==0:
                        cited_articles = {f"{a['number_article'][0]}":1 for a in c['articles']}
                        if citation_name in nodes[1]:
                            node=nodes[1][citation_name]
                            node['count']+=1
                            if 'articles' in node and len(node['articles'])>0:
                                c=Counter(node['articles'])
                                c.update(cited_articles)
                                articles = dict(c)
                            else:
                                articles = cited_articles
                            node['articles']=articles
                        else:
                            node={
                                'type':'citation',
                                'name': citation_name,
                                'count':1,
                                'articles':cited_articles
                            }
                            nodes[1][citation_name] = node
                    else:
                        node=node.results[0]
                        cited_articles = {f"{a['number_article'][0]}":1 for a in c['articles'] if len(a['number_article'])>0}
                        if citation_name in nodes[0]:
                            node=nodes[0][citation_name]
                            node['count']+=1
                            if 'articles' in node and len(node['articles'])>0:
                                c=Counter(node['articles'])
                                c.update(cited_articles)
                                articles = dict(c)
                            else:
                                articles = cited_articles
                            node['articles']=articles
                        else:
                            if 'articles' in node and len(node['articles'])>0:
                                c=Counter(node['articles'])
                                c.update(cited_articles)
                                articles = dict(c)
                            else:
                                articles = cited_articles
                            node['articles']=articles
                            nodes[0][citation_name] = node

            # updates
            stats = await index.get_stats()
            node_updates = list(nodes[0].values())
            new_nodes=[]
            source['id']=stats.number_of_documents+1
            for j,n in  enumerate(nodes[1].values()):
                n['id']=stats.number_of_documents+j+2
                new_nodes.append(n)
            if new_node_flag:
                new_nodes.append(source)
            else:
                node_updates.append(source)
            await index.update_documents(node_updates,primary_key = "id")
            await index.add_documents(new_nodes, primary_key = "id")
            links=({},{})
            for n in node_updates:
                link=await index.get_documents(
                        filter=f'type="link" AND source={source["id"]} AND target={n["id"]}',
                        limit=1)
                if len(link.results)==0:
                    d=datetime.fromisoformat(doc['date'])
                    link={
                        'type':'link',
                        'country':doc.get('country',None),
                        'source':source['id'],
                        'target':n['id'],
                        'count':1,
                        'year':d.year,
                    }
                    if (link['source'],link['target']) in links[1]:
                        links[1][link['source'],link['target']]['count']+=1
                    else:
                        links[1][link['source'],link['target']]=link
                else:
                    links[0][(link.results[0]['source'],link.results[0]['target'])]=link.results[0]
                    links[0][(link.results[0]['source'],link.results[0]['target'])]['count']+=1
            for n in new_nodes:
                link={
                    'type':'link',
                    'country':doc.get('country',None),
                    'source':source['id'],
                    'target':n['id'],
                    'year':d.year,
                    'count':1,
                }
                if (link['source'],link['target']) in links[1]:
                    links[1][(link['source'],link['target'])]['count']+=1
                else:
                    links[1][(link['source'],link['target'])]=link
            link_updates = links[0]
            await index.update_documents(list(link_updates.values()), primary_key = "id")
            new_links=links[1]
            #stats = await index.get_stats()
            new_links=[]
            for j,l in enumerate(links[1].values()):
                l['id']=stats.number_of_documents+len(new_nodes)+j+2
                new_links.append(l)
      
            await index.add_documents(new_links, primary_key = "id")
            json.dump(cached_citations, open('cached_citations.json','w'), indent=2)
            nodes=({},{})

@app.command()
def create_graph(ini: int = None, fin: int = None, update: bool = False):
    """Update metadata for sentencias

    Parameters:

    Returns:

    None"""
    loop = asyncio.get_event_loop()
    loop.run_until_complete(create_graph_(ini, fin, update))



async def add_filter_(filter:str, index: str):
    """ Adds filter for the database async

    Parameters:

    filter(str) Column of the database to allow to look for.

    Returns:

    None"""
    load_dotenv()

    async with AsyncClient('http://localhost:7700', os.getenv("MEILI_MASTER_KEY")) as client:
        index = client.index(index)
        results=await index.get_filterable_attributes()
        if results:
            await index.update_filterable_attributes(results+filter.split(","))
        else:
            await index.update_filterable_attributes(filter.split(","))


@app.command()
def add_filter(filter:str, index: str = "conectividad_docs"):
    """ Adds filter for the database

    Parameters:

    filter(str) Column of the database to allow to look for.

    Returns:

    None"""
 
    loop = asyncio.get_event_loop()
    loop.run_until_complete(add_filter_(filter, index))

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

async def delete_all_(index_name: str):
    """ Delete all the documents

    Parameters:

    Returns:

    None"""
    load_dotenv()

    async with AsyncClient('http://localhost:7700', os.getenv("MEILI_MASTER_KEY")) as client:
        index = client.index(index_name)
        task =  await index.delete_all_documents()



async def delete_segments_(ini: int, fin: int, index_name: str):
    """ Delete all the documents

    Parameters:

    Returns:

    None"""
    load_dotenv()

    async with AsyncClient('http://localhost:7700', os.getenv("MEILI_MASTER_KEY")) as client:
        index = client.index(index_name)
        if not ini:
            ini=1
        if not fin:
            fin=60000
        for sentence_num in track(range(ini, fin+1)):
            docs= await index.get_documents(filter=f'sentence_num = {sentence_num} AND type = "element"', limit=5000)
            original = await index.get_documents(filter=f'sentence_num = {sentence_num} AND type = "original"', limit=1)
            ixs = [d['document_id'] for d in docs.results]
            if len(ixs)>0:
                await index.delete_documents(ixs)
            if len(original.results)>0:
                await index.delete_documents([original.results[0]['document_id']])

@app.command()
def delete_segments(ini: int = None, fin:int = None, index: str = "conectividad_docs"):
    """ Delete all the documents

    Parameters:

    Returns:

    None"""
 
    loop = asyncio.get_event_loop()
    loop.run_until_complete(delete_segments_(ini,fin,index))



@app.command()
def delete_all(index: str = "conectividad_docs"):
    """ Delete all the documents

    Parameters:

    Returns:

    None"""
 
    loop = asyncio.get_event_loop()
    loop.run_until_complete(delete_all_(index))


if __name__ == "__main__":
   app()


    

