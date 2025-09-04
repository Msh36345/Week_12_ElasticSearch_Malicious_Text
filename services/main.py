import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

from services.loader import load_data_to_elastic, reset_index
from services.processor import process_sentiment_batch,process_weapons_batch, delete_safe_documents
from services.fast_api import run

def main():
    reset_index()
    load_data_to_elastic(os.environ.get("TWEETS_FILE", "data/tweets.csv"))
    process_sentiment_batch()
    process_weapons_batch()
    delete_safe_documents()
    run()


if __name__ == "__main__":
    main()