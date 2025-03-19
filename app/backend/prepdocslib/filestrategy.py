import logging
from typing import List, Optional

from azure.core.credentials import AzureKeyCredential

from .blobmanager import BlobManager
from .embeddings import ImageEmbeddings, OpenAIEmbeddings
from .fileprocessor import FileProcessor
from .listfilestrategy import File, ListFileStrategy
from .mediadescriber import ContentUnderstandingDescriber
from .searchmanager import SearchManager, Section
from .strategy import DocumentAction, SearchInfo, Strategy
from bs4 import BeautifulSoup as Soup
from langchain_community.document_loaders.recursive_url_loader import RecursiveUrlLoader
import io
import re


logger = logging.getLogger("scripts")


async def parse_file(
    file: File,
    file_processors: dict[str, FileProcessor],
    category: Optional[str] = None,
    image_embeddings: Optional[ImageEmbeddings] = None,
) -> List[Section]:
    key = file.file_extension().lower()
    processor = file_processors.get(key)
    if processor is None:
        logger.info("Skipping '%s', no parser found.", file.filename())
        return []
    logger.info("Ingesting '%s'", file.filename())
    pages = [page async for page in processor.parser.parse(content=file.content)]
    logger.info("Splitting '%s' into sections", file.filename())
    if image_embeddings:
        logger.warning("Each page will be split into smaller chunks of text, but images will be of the entire page.")
    sections = [
        Section(split_page, content=file, category=category) for split_page in processor.splitter.split_pages(pages)
    ]
    return sections

def process_url(url, max_depth):
    loader = RecursiveUrlLoader(
        url=url, max_depth=max_depth, extractor=lambda x: Soup(x, "html.parser").text
    )
    docs = loader.load()
    return docs

def sanitize_filename(url: str) -> str:
    """Convert a URL into a safe filename."""
    return re.sub(r'[\\/*?:"<>|]', "_", url)


async def parse_url(
    url: str,
    max_depth : int,
    file_processors: dict[str, FileProcessor],
    category: Optional[str] = None,
    image_embeddings: Optional[ImageEmbeddings] = None,
) -> List[Section]:
     
    processor = file_processors.get(".txt")
    if processor is None:
        logger.info("Skipping '%s', no parser found.", )
        return []
    logger.info("Ingesting '%s'", url)

    docs = process_url(url, max_depth)
    if not docs:
        logger.warning("No content extracted from '%s'.", url)
        return []
    
    full_text = "\n\n".join([doc.page_content for doc in docs])

    file_path = sanitize_filename(url)
    file = File(content=io.StringIO(full_text))
    file.content.name = file_path
    pages = [page async for page in processor.parser.parse(content=io.BytesIO(full_text.encode("utf-8")))]
    logger.warning("Each page will be split into smaller chunks of text, but images will be of the entire page.")
    sections = [
        Section(split_page, content=file, category=category) for split_page in processor.splitter.split_pages(pages)
    ]
    return sections, file

class FileStrategy(Strategy):
    """
    Strategy for ingesting documents into a search service from files stored either locally or in a data lake storage account
    """

    def __init__(
        self,
        list_file_strategy: ListFileStrategy,
        blob_manager: BlobManager,
        search_info: SearchInfo,
        file_processors: dict[str, FileProcessor],
        document_action: DocumentAction = DocumentAction.Add,
        embeddings: Optional[OpenAIEmbeddings] = None,
        image_embeddings: Optional[ImageEmbeddings] = None,
        search_analyzer_name: Optional[str] = None,
        use_acls: bool = False,
        category: Optional[str] = None,
        use_content_understanding: bool = False,
        content_understanding_endpoint: Optional[str] = None,
        url: Optional[str] = None,
        max_depth: Optional[int] = None
    ):
        self.list_file_strategy = list_file_strategy
        self.blob_manager = blob_manager
        self.file_processors = file_processors
        self.document_action = document_action
        self.embeddings = embeddings
        self.image_embeddings = image_embeddings
        self.search_analyzer_name = search_analyzer_name
        self.search_info = search_info
        self.use_acls = use_acls
        self.category = category
        self.use_content_understanding = use_content_understanding
        self.content_understanding_endpoint = content_understanding_endpoint
        self.url = url
        self.max_depth = int(max_depth) if max_depth is not None else 0

    async def setup(self):
        search_manager = SearchManager(
            self.search_info,
            self.search_analyzer_name,
            self.use_acls,
            False,
            self.embeddings,
            search_images=self.image_embeddings is not None,
        )
        await search_manager.create_index()

        if self.use_content_understanding:
            if self.content_understanding_endpoint is None:
                raise ValueError("Content Understanding is enabled but no endpoint was provided")
            if isinstance(self.search_info.credential, AzureKeyCredential):
                raise ValueError(
                    "AzureKeyCredential is not supported for Content Understanding, use keyless auth instead"
                )
            cu_manager = ContentUnderstandingDescriber(self.content_understanding_endpoint, self.search_info.credential)
            await cu_manager.create_analyzer()

    async def run(self):
        search_manager = SearchManager(
            self.search_info, self.search_analyzer_name, self.use_acls, False, self.embeddings
        )
        if self.document_action == DocumentAction.Add:
            if self.url:
                try:
                    sections, file= await parse_url(self.url, self.max_depth, self.file_processors, self.category, self.image_embeddings)
                    if sections:
                        # blob_sas_uris = await self.blob_manager.upload_blob(file)
                        blob_image_embeddings: Optional[List[List[float]]] = None
                        if self.image_embeddings and blob_sas_uris:
                            blob_image_embeddings = await self.image_embeddings.create_embeddings(blob_sas_uris)
                        await search_manager.update_content(sections, blob_image_embeddings, url=file.url, source_website_url=self.url)
                        print(f"Filename processed: {self.url}, Status: True")

                except Exception as e:
                    print(f"Filename processed: {self.url}, Status: False, Error: {e}")


            files = self.list_file_strategy.list()
            async for file in files:
               
                try:
                    sections = await parse_file(file, self.file_processors, self.category, self.image_embeddings)
                    if sections:
                        blob_sas_uris = await self.blob_manager.upload_blob(file)
                        blob_image_embeddings: Optional[List[List[float]]] = None
                        if self.image_embeddings and blob_sas_uris:
                            blob_image_embeddings = await self.image_embeddings.create_embeddings(blob_sas_uris)

                        await search_manager.update_content(sections, blob_image_embeddings, url=file.url)
                        print(f"Filename processed: {file.filename()}, Status: True")

                except Exception as e:
                    print(f"Filename processed: {file.filename()}, Status: False, Error: {e}")
                finally:
                    if file:
                        file.close()
        elif self.document_action == DocumentAction.Remove:
            paths = self.list_file_strategy.list_paths()
            async for path in paths:
                await self.blob_manager.remove_blob(path)
                await search_manager.remove_content(path)
        elif self.document_action == DocumentAction.RemoveAll:
            await self.blob_manager.remove_blob()
            await search_manager.remove_content()


class UploadUserFileStrategy:
    """
    Strategy for ingesting a file that has already been uploaded to a ADLS2 storage account
    """

    def __init__(
        self,
        search_info: SearchInfo,
        file_processors: dict[str, FileProcessor],
        embeddings: Optional[OpenAIEmbeddings] = None,
        image_embeddings: Optional[ImageEmbeddings] = None,
    ):
        self.file_processors = file_processors
        self.embeddings = embeddings
        self.image_embeddings = image_embeddings
        self.search_info = search_info
        self.search_manager = SearchManager(self.search_info, None, True, False, self.embeddings)

    async def add_file(self, file: File):
        if self.image_embeddings:
            logging.warning("Image embeddings are not currently supported for the user upload feature")
        sections = await parse_file(file, self.file_processors)
        if sections:
            await self.search_manager.update_content(sections, url=file.url)

    async def remove_file(self, filename: str, oid: str):
        if filename is None or filename == "":
            logging.warning("Filename is required to remove a file")
            return
        await self.search_manager.remove_content(filename, oid)
