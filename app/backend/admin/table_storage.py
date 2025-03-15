from quart import current_app
import logging
import uuid
from datetime import datetime
import json
from azure.data.tables import UpdateMode
CHATLOG_TABLE = 'chatLog'

CONFIG_TABLE_SERVICE_CLIENT = 'table_service_client'
PROMPT_TABLE = 'servicePrompts'


async def upsert_chatlog_entity(selected_service, user_name, api_function, chat_history, is_deleted):
    table_client = current_app.config[CONFIG_TABLE_SERVICE_CLIENT].create_table_if_not_exists(CHATLOG_TABLE)

    unique_id = str(uuid.uuid4())
    timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')

    entity = {
        'PartitionKey': selected_service,
        'RowKey': f'{timestamp}_{unique_id}',
        'UserName': user_name,
        'ApiFunction': api_function,
        'ChatHistory': json.dumps(chat_history),
        'IsDeleted': is_deleted
    }

    if api_function == 'process':
        query_filter = f"PartitionKey eq '{selected_service}' and ApiFunction eq 'process' and IsDeleted eq 0"
        matching_entities = table_client.query_entities(query_filter=query_filter)

        for existing_entity in matching_entities:
            existing_chat_history = json.loads(existing_entity['ChatHistory'])
            if existing_chat_history == chat_history:
                # Update the previous entry
                existing_entity['IsDeleted'] = 1
                table_client.update_entity(mode=UpdateMode.MERGE, entity=existing_entity)

    # Insert or update the entity
    table_client.upsert_entity(entity=entity)

async def get_prompt_entity(search_criteria):
    try:
        table_client = current_app.config[CONFIG_TABLE_SERVICE_CLIENT].create_table_if_not_exists(PROMPT_TABLE)
        service = search_criteria.get('service', None)
        query_filter = f"RowKey eq '{service}'"

        # Retrieve entities from the Azure table
        entities = table_client.query_entities(query_filter=query_filter)

        # Iterate over the entities (assuming you are expecting one entity)
        for entity in entities:
            document_rag_prompt = entity['DocumentRAGPrompt']
            sql_agent_prompt = entity['SQLAgentPrompt']
            user_intent_classifier_prompt = entity['UserIntentClassifierPrompt']
            return {"document_rag_prompt": document_rag_prompt,
                    "sql_agent_prompt": sql_agent_prompt,
                    "user_intent_classifier_prompt": user_intent_classifier_prompt}

        # If no entities are found
        return {"document_rag_prompt": ""}

    except Exception as e:
        error_message = "An error occurred while retrieving prompt entries."
        logging.exception(error_message)
        return {"error": error_message, "details": str(e)}
    

async def update_is_deleted(partition_key, row_key):
    try:
        table_client = current_app.config[CONFIG_TABLE_SERVICE_CLIENT].create_table_if_not_exists(CHATLOG_TABLE)

        entity = table_client.get_entity(partition_key = partition_key, row_key=row_key)

        # Update the 'IsDeleted' field to 1
        entity['IsDeleted'] = 1

        # Save the updated entity back to the table
        table_client.update_entity(entity=entity)

    except Exception as e:
        # Handle any errors that may occur during the update
        print(f"Error updating IsDeleted: {str(e)}")
