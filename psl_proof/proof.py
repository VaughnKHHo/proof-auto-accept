import json
import logging
import os
from typing import Dict, Any
import requests

from datetime import datetime, timezone
from psl_proof.models.proof_response import ProofResponse
from psl_proof.utils.hashing_utils import salted_data, serialize_bloom_filter_base64, deserialize_bloom_filter_base64
from psl_proof.models.cargo_data import SourceChatData, CargoData, SourceData, DataSource, MetaData, DataSource
from psl_proof.utils.validate_data import validate_data, get_total_score
from psl_proof.utils.submission import submit_data
from psl_proof.utils.verification import verify_token, VerifyTokenResult
from psl_proof.models.submission_dtos import ChatHistory, SubmissionChat, SubmissionHistory
from psl_proof.utils.submission import get_submission_historical_data


class Proof:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.proof_response = ProofResponse(dlp_id=config['dlp_id'])

    def generate(self) -> ProofResponse:
        current_timestamp = datetime.now(timezone.utc)
        data_revision = "01.01"

        self.proof_response.ownership = 1.0
        self.proof_response.authenticity = 1.0
        self.proof_response.quality = 1.0
        self.proof_response.uniqueness = 1.0
        self.proof_response.valid = True
        self.proof_response.score = 0.05

        metadata = MetaData(
            source_id = "dummy_id",
            dlp_id = self.config['dlp_id']
        )

        self.proof_response.attributes = {
            'score': 0.05,
            'did_score_content': True,
            'source': "hardcoded_source",
            'revision': data_revision,
            'submitted_on': current_timestamp.isoformat()
        }
        self.proof_response.metadata = metadata

        return self.proof_response

def get_telegram_data(
    submission_timestamp : datetime,
    input_content: dict,
    source_chat_data: 'SourceChatData'
):
    chat_type = input_content.get('@type')
    if chat_type == "message":
        # Extract user ID
        chat_user_id = input_content.get("sender_id", {}).get("user_id", "")
        #print(f"chat_user_id: {chat_user_id}")
        source_chat_data.add_participant(chat_user_id)

        message_date = submission_timestamp
        # Extract and convert the Unix timestamp to a datetime object
        date_value = input_content.get("date", None)
        if date_value:
            message_date = datetime.utcfromtimestamp(date_value)  # Convert Unix timestamp to datetime
            message_date = message_date.astimezone(timezone.utc)

        #print(f"message_date: {message_date}")

        # Extract the message content
        message = input_content.get('content', {})
        if isinstance(message, dict) and message.get("@type") == "messageText":
            content = message.get("text", {}).get("text", "")
            #print(f"Extracted content: {content}")
            source_chat_data.add_content(
                content,
                message_date,
                submission_timestamp
            )


def get_telegram_miner(
    submission_timestamp : datetime,
    input_content: dict,
    source_chat_data: 'SourceChatData'
):
    #print(f"get_telegram_miner - input_content: {input_content}")
    chat_type = input_content.get('className')
    #print(f"chat_type: {chat_type}")
    if chat_type == "Message":
        # Extract user ID
        chat_user_id = input_content.get("peerId", {}).get("userId", "")
        #print(f"chat_user_id: {chat_user_id}")
        source_chat_data.add_participant(chat_user_id)

        message_date = submission_timestamp
        # Extract and convert the Unix timestamp to a datetime object
        date_value = input_content.get("date", None)
        if date_value:
            message_date = datetime.utcfromtimestamp(date_value)  # Convert Unix timestamp to datetime
            message_date = message_date.astimezone(timezone.utc)

        #print(f"message_date: {message_date}")

        # Extract the message content
        content = input_content.get('message', '')
        #print(f"content: {content}")
        if content :
            source_chat_data.add_content(
                content,
                message_date,
                submission_timestamp
            )

def get_source_data(
    input_data: Dict[str, Any],
    submission_timestamp: datetime,
 ) -> SourceData:

    revision = input_data.get('revision', '')
    if (revision and revision != "01.01"):
       raise RuntimeError(f"Invalid Revision: {revision}")

    input_source_value = input_data.get('source', '').upper()
    input_source = None

    if input_source_value == 'TELEGRAM':
        input_source = DataSource.telegram
    elif input_source_value == 'TELEGRAMMINER':
        input_source = DataSource.telegramMiner
    else:
        raise RuntimeError(f"Unmapped data source: {input_source_value}")
    print(f"input_source: {input_source}")

    submission_token = input_data.get('submission_token', '')
    #print("submission_token: {submission_token}")

    input_user = input_data.get('user')
    #print(f"input_user: {input_user}")

    source_data = SourceData(
        source=input_source,
        user = input_user,
        submission_token = submission_token,
        submission_date = submission_timestamp
    )

    input_chats = input_data.get('chats', [])
    source_chats = source_data.source_chats

    for input_chat in input_chats:
        chat_id = input_chat.get('chat_id')
        input_contents = input_chat.get('contents', [])
        if chat_id and input_contents:
            source_chat = SourceChatData(
                chat_id=chat_id
            )
            for input_content in input_contents:
                if input_source == DataSource.telegram:
                    get_telegram_data(
                        submission_timestamp,
                        input_content,
                        source_chat
                    )
                elif input_source == DataSource.telegramMiner:
                    get_telegram_miner(
                        submission_timestamp,
                        input_content,
                        source_chat
                    )
                else:
                    raise RuntimeError(f"Unhandled data source: {input_source}")
            source_chats.append(
                source_chat
            )
    return source_data
