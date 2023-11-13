import pika
import json
from pymongo import MongoClient
from bson import ObjectId


DB_URL = "mongodb://localhost:27017/"
DB_NAME = "val_db"
DB_COLLECTION = "polls"

COLLECTION_TITLE = "title"
COLLECTION_CHOICES = "choices"
COLLECTION_VOTE_COUNT = "voteCount"

MSG_URL = "localhost"
MSG_QUEUE_NAME = "polls_queue"

def db_conn():
    conn = MongoClient(DB_URL)

    db = conn[DB_NAME]

    return db[DB_COLLECTION]



def establish_consumer():

    conn = pika.BlockingConnection(pika.ConnectionParameters(MSG_URL))

    channel = conn.channel()

    channel.queue_declare(queue=MSG_QUEUE_NAME)

    channel.basic_consume(queue=MSG_QUEUE_NAME, on_message_callback=consumer, auto_ack=True)

    return channel


def consumer(ch, method, properties, body):
    data = json.loads(body.decode("utf-8"))
    title = data.get(COLLECTION_TITLE)
    choices = data.get(COLLECTION_CHOICES, [])

    existing_poll = db_conn().find_one({COLLECTION_TITLE: title})

    if existing_poll:
        # Update the scores if the poll already exists
        existing_choices = existing_poll.get(COLLECTION_CHOICES, [])

        for new_choice in choices:
            existing_choice = next(
                (c for c in existing_choices if c[COLLECTION_TITLE] == new_choice[COLLECTION_TITLE]),
                None,
            )

            if existing_choice:
                existing_choice[COLLECTION_VOTE_COUNT] = existing_choice.get(COLLECTION_VOTE_COUNT, 0) + new_choice.get("voteCount", 0)
            else:
                existing_choices.append(new_choice)

        # Update the existing poll with the modified choices
        db_conn().update_one({"_id": ObjectId(existing_poll["_id"])}, {"$set": {COLLECTION_CHOICES: existing_choices}})
        print(f"> Updated poll with title: {title}")
    else:
        # Insert a new poll if it doesn't exist
        db_conn().insert_one(data)
        print(f"> Inserted new poll: {data}")



def main():

    channel = establish_consumer()

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("> Interrupted. Exiting...")
    except Exception as e:
        print("=" * 20)
        print(f"\nError:\n{e}\n")
        print("=" * 20)
    finally:
        channel.close()
        print("> Connection closed")
    
    print("> Exited")
