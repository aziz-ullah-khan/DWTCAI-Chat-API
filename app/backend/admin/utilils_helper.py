import json
import os 
import asyncio
import shutil
import subprocess
from admin.table_storage import get_prompt_entity, get_api_configuration
from azure.identity.aio import DefaultAzureCredential
from azure.search.documents.aio import SearchClient
from azure.core.credentials import AzureKeyCredential
from azure.storage.blob.aio import BlobServiceClient
from approaches.chatreadretrieveread import ChatReadRetrieveReadApproach
import logging, re
from quart import current_app
from dotenv import load_dotenv


CONFIG_CHAT_APPROACH = "chat_approach"
CONFIG_OPENAI_CLIENT = "openai_client"

# AZURE_AI_SERVICE = os.environ["AZURE_AI_SERVICE"]
# AZURE_AI_API_KEY = os.environ["AZURE_AI_API_KEY"]
AZURE_STORAGE_ACCOUNT = os.environ["AZURE_STORAGE_ACCOUNT"] 
AZURE_STORAGE_KEY = os.environ.get("AZURE_STORAGE_KEY")
AZURE_SEARCH_KEY = os.environ.get("AZURE_SEARCH_KEY")
AZURE_SEARCH_SERVICE = os.environ["AZURE_SEARCH_SERVICE"]
# AZURE_OPENAI_CHATGPT_DEPLOYMENT = os.environ["AZURE_OPENAI_CHATGPT_DEPLOYMENT"]
# AZURE_OPENAI_CHATGPT_MODEL = os.environ["AZURE_OPENAI_CHATGPT_MODEL"]
# AZURE_OPENAI_EMB_DEPLOYMENT = os.environ["AZURE_OPENAI_EMB_DEPLOYMENT"]
# KB_FIELDS_CONTENT = os.getenv("KB_FIELDS_CONTENT", "content")
# KB_FIELDS_SOURCEPAGE = os.getenv("KB_FIELDS_SOURCEPAGE", "sourcepage")

async def load_environment_variables():
    load_dotenv()

async def load_table_environment_variables():
    try:
        api_config = await get_api_configuration()

        # Setting environment variables
        for key, value in api_config.items():
            if key=="SERVICES":
                os.environ[key] = value

    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
    except Exception as e:
        print(f"Error setting table environment variables: {e}")

async def install_sudo():
    try:
        print("Installing sudo...")
        subprocess.run(['apt-get', 'update'], check=True)
        subprocess.run(['apt-get', 'install', 'sudo'], check=True)
        print("sudo installed successfully.")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Error installing sudo: {e}")
    
async def install_libreoffice():
    if shutil.which('sudo') is None:
        await install_sudo()

    try:
        print("Installing LibreOffice...")
        subprocess.run(['sudo', 'apt-get', 'update'], check=True)
        subprocess.run(['sudo', 'apt-get', 'install', 'libreoffice'], check=True)
        print("LibreOffice installed successfully.")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Error installing LibreOffice: {e}")

def is_libreoffice_installed():
    return shutil.which('libreoffice') is not None

async def get_service_accessories(service):
    # Fetch services and parse JSON
    services = json.loads(os.getenv('SERVICES', '[]').replace('\\', ''))
    # service_info = next((x for x in services if x["service"] == service), None)
    service_info = next((x for x in services if x["service"].lower() == service.lower()), None)
    if service_info:
        azure_search_index = service_info.get("index", os.environ.get("AZURE_SEARCH_INDEX", "index"))
        azure_storage_container = service_info.get("blob", os.environ.get("AZURE_STORAGE_CONTAINER", "content"))
        service_prompt = service_info.get("prompt", None)
        if not service_prompt:
            service_prompt = await get_prompt_entity({"service": service })
        use_external_source = service_info.get("use_external_source", 0)
    else:
        return None
    return [azure_search_index, azure_storage_container, service_prompt['document_rag_prompt'], use_external_source]

async def generate_pdf_async(doc_path, output_path): #TODO: Implement alternate way wihtout libre office installation. 
    if not is_libreoffice_installed():
        await install_libreoffice()

    process_cmd = [
        'soffice',
        '--headless',
        '--convert-to',
        'pdf',
        '--outdir',
        output_path,
        doc_path
    ]

    try:
        process = await asyncio.create_subprocess_exec(
            *process_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        stdout, stderr = await process.communicate()

        if process.returncode == 0: 
            return True, os.path.join(output_path, os.path.basename(doc_path) + '.pdf')
        else:
            return False, f"Conversion failed. Error: {stderr.decode()}"
    except Exception as e:
        return False, f"An error occurred: {str(e)}"
    


def get_blob_container_client(azure_storage_container):

    azure_credential = DefaultAzureCredential(exclude_shared_token_cache_credential = True)
    storage_creds = azure_credential if AZURE_STORAGE_KEY is None else AZURE_STORAGE_KEY
    blob_client = BlobServiceClient(
        account_url=f"https://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net",
        credential=storage_creds)
    blob_container_client = blob_client.get_container_client(azure_storage_container)
    
    return blob_container_client


def get_search_client(azure_search_index):

    azure_credential = DefaultAzureCredential(exclude_shared_token_cache_credential = True)
    search_creds = azure_credential if AZURE_SEARCH_KEY is None else AzureKeyCredential(AZURE_SEARCH_KEY)
    # Set up clients for Cognitive Search and Storage
    search_client = SearchClient(
        endpoint=f"https://{AZURE_SEARCH_SERVICE}.search.windows.net",
        index_name=azure_search_index,
        credential=search_creds)
    
    return search_client


def is_valid_file_name(file_path):
    try:
        # Extract the file name from the path
        file_name = os.path.basename(file_path)
        
        # Define a regex pattern for invalid characters
        invalid_chars_pattern = r'[:<>"/\\|?*]'
        
        # Check for invalid characters
        if re.search(invalid_chars_pattern, file_name):
            logging.warning(f"Invalid characters found in file name: {file_name}")
            return False
        
        # Check if the name is not empty and does not consist solely of spaces
        if not file_name.strip():
            logging.warning(f"File name is empty or consists solely of spaces: {file_name}")
            return False
        
        # Check for valid file extension
        valid_extensions = ['.txt', '.pdf', '.docx', '.xlsx', '.png', '.jpg', '.jpeg', '.gif', '.csv', '.json', '.xml']
        if not any(file_name.endswith(ext) for ext in valid_extensions):
            logging.warning(f"Invalid file extension for file name: {file_name}")
            return False
        
        return True
    except Exception as e:
        logging.error(f"Error validating file name '{file_path}': {e}")
        return False
    
async def remove_index_blob(search_client, blob_container, filename):
    try:
        if filename:
            # remove index
            while True:
                if is_valid_file_name(filename):
                    filename = os.path.basename(filename)
                filter = None if filename is None else f"sourcefile eq '{filename}'"
                r = await search_client.search("", filter=filter, top=1000, include_total_count=True)
                doc_to_delete = [{ "id": d["id"] } async for d in r]
                r_count = await r.get_count()
                if r_count== 0:
                    break
                await search_client.delete_documents(documents=doc_to_delete)
                await asyncio.sleep(2)
            
            # remove blob
            if is_valid_file_name(filename):
                prefix = os.path.splitext(os.path.basename(filename))[0]
                blob_names = [b async for b in blob_container.list_blob_names(name_starts_with=os.path.splitext(os.path.basename(prefix))[0]) if re.match(f"{prefix}-\d+\.pdf", b)]
                for b in blob_names:
                    await blob_container.delete_blob(b)
    except Exception as e:
        # Handle exceptions here, e.g., log the error or perform specific actions.
        print(f"An error occurred: {e}")


def get_config_chat_approaches(azure_search_index):
    # Get the stored chat approach instance
    chat_approach = current_app.config.get(CONFIG_CHAT_APPROACH)
    
    if not chat_approach:
        raise ValueError("Chat approach is not initialized in current_app.config")

    # Update the search client dynamically
    chat_approach.search_client = get_search_client(azure_search_index)

    print(f"rrr:{chat_approach}")
    
    return {"rrr": chat_approach}