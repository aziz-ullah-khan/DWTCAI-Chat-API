from quart import current_app
import logging
import uuid
from datetime import datetime
import json
from azure.data.tables import UpdateMode
CHATLOG_TABLE = 'chatLog'

CONFIG_TABLE_SERVICE_CLIENT = 'table_service_client'
PROMPT_TABLE = 'servicePrompts'
FEEDBACK_TABLE = 'feedbackTable'
APICONFIGURATION_TABLE = 'apiConfiguration'


async def upsert_api_configuration(key, value):
    try:
        table_client = current_app.config[CONFIG_TABLE_SERVICE_CLIENT].create_table_if_not_exists(APICONFIGURATION_TABLE)
        entity = {
            'PartitionKey': 'api',
            'RowKey': key,
            'Value': value,
        }
        # Insert or update the entity
        table_client.upsert_entity(entity=entity)
        return {"message": "Configuration stored or updated successfully"}
    except Exception as e:
            error_message = f"An error occurred while adding or updating the value '{value}'."
            logging.exception(error_message)
            return {"error": error_message, "details": str(e)}


async def get_api_configuration():
    try:
        table_client = current_app.config[CONFIG_TABLE_SERVICE_CLIENT].create_table_if_not_exists(APICONFIGURATION_TABLE)

        # Retrieve entities from the Azure table
        entities = table_client.query_entities(query_filter=None)

        api_configuration = {}
        for entity in entities:
            api_configuration[entity['RowKey']] = entity['Value']

        return api_configuration
    except Exception as e:
        error_message = "An error occurred while retrieving API configuration."
        logging.exception(error_message)
        return {"error": error_message, "details": str(e)}
    
async def delete_api_configuration(key):
    try:
        table_client = current_app.config[CONFIG_TABLE_SERVICE_CLIENT].create_table_if_not_exists(APICONFIGURATION_TABLE)
        table_client.delete_entity(partition_key='api', row_key=key)            
        return {"message": f"'{key}' deleted successfully"}
    except Exception as e:
        error_message = f"An error occurred while deleting the API configuration with key '{key}'."
        logging.exception(error_message)
        return {"error": error_message, "details": str(e)}

async def upsert_prompt_entity(service, user_intent_classifier_prompt, document_rag_prompt, sql_agent_prompt):
    table_client = current_app.config[CONFIG_TABLE_SERVICE_CLIENT].create_table_if_not_exists(PROMPT_TABLE)

    entity = {
        'PartitionKey': 'service',
        'RowKey': service,
        'DocumentRAGPrompt': document_rag_prompt,
        'SQLAgentPrompt': sql_agent_prompt,
        'UserIntentClassifierPrompt': user_intent_classifier_prompt
    }

    # Insert or update the entity
    table_client.upsert_entity(entity=entity)

    return {"message": "Prompt stored or updated successfully"}

async def get_feedback_entries(search_criteria):
    try:
        table_client = current_app.config[CONFIG_TABLE_SERVICE_CLIENT].create_table_if_not_exists(FEEDBACK_TABLE)

        start_date = search_criteria.get('start_date', None)
        end_date = search_criteria.get('end_date', None)
        service = search_criteria.get('service', None)
        user_name = search_criteria.get('user_name', None)
        is_deleted = search_criteria.get('is_deleted', 0)
        key_mapping = {
                        "feedback_flag": "FeedbackFlag",
                        "user_name": "UserName",
                        "is_deleted": "IsDeleted"
                        }
        for old_key, new_key in key_mapping.items():
            if old_key in search_criteria:
                search_criteria[new_key] = search_criteria.pop(old_key)
                
        query_filters = []
        for key, value in search_criteria.items():
            if key not in ['start_date', 'end_date', 'service']:
                if isinstance(value, int):
                    query_filters.append(f"{key} eq {value}")
                else:
                    query_filters.append(f"{key} eq '{value}'")

        if start_date:
            query_filters.append(f"Timestamp ge datetime'{start_date}'")
        if end_date:
            query_filters.append(f"Timestamp le datetime'{end_date}'")

        if service:
            query_filters.append(f"PartitionKey eq '{service}'")
        query_filter = " and ".join(query_filters)
        # Retrieve entities from the Azure table
        entities = table_client.query_entities(query_filter=query_filter)

        feedback_list = []
        for entity in entities:
            feedback = {
                'TimeStamp': entity._metadata["timestamp"],
                'UserName': entity['UserName'],
                'FeedbackFlag': entity['FeedbackFlag'],
                'Feedback': entity['Feedback'],
                'ChatHistory': json.loads(entity['ChatHistory']),
                'IsDeleted': entity['IsDeleted']
            }
            feedback_list.append(feedback)

        return feedback_list

    except Exception as e:
        error_message = "An error occurred while retrieving feedback entries."
        logging.exception(error_message)
        return {"error": error_message, "details": str(e)}

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

async def upsert_feedback_entity(service, user_name, feedback_flag, feedback, chat_history, is_deleted):
    table_client = current_app.config[CONFIG_TABLE_SERVICE_CLIENT].create_table_if_not_exists(FEEDBACK_TABLE)

    unique_id = str(uuid.uuid4())
    timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')

    entity = {
        'PartitionKey': service,
        'RowKey': f'{timestamp}_{unique_id}',
        'UserName': user_name,
        'FeedbackFlag': feedback_flag,
        'Feedback': feedback,
        'ChatHistory': json.dumps(chat_history),
        'IsDeleted': is_deleted
    }

    # Insert or update the entity
    table_client.upsert_entity(entity=entity)

    return {"message": "Feedback stored successfully"}

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
