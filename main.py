import asyncio
import uvicorn
from fastapi import FastAPI, Request
from sse_starlette.sse import EventSourceResponse
from confluent_kafka.error import KafkaError, KafkaException
from streaming.consumer import create_consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from dotenv import load_dotenv
import os

load_dotenv(verbose=True)


STREAM_DELAY = 1
RETRY_TIMEOUT = 15000

app = FastAPI()

@app.get("/")
async def root():
    return {"message":"Hello World!"}

@app.get("/stream")
async def message_stream(request: Request):
    cr_config = {
        "bootstrap.servers":os.getenv("KAFKA_BROKER"),
        "security.protocol":os.getenv("SECURITY_PROTOCOL"),
        "sasl.mechanism":os.getenv("SASL_MECHANISM"),
        "sasl.username":os.getenv("SASL_USERNAME"),
        "sasl.password":os.getenv("SASL_PASSWORD"),
        "sr_url":os.getenv("SCHEMA_REGISTRY_URL"),
        "sr_api_key":os.getenv("SCHEMA_REGISTRY_API_KEY"),
        "sr_secret_key":os.getenv("SCHEMA_REGISTRY_SECRET_KEY"),
    }
    consumer, avro_deserializer = create_consumer(cr_config)

    async def event_generator():
        try:
            while True:
                if await request.is_disconnected():
                    break

                msg = await asyncio.to_thread(consumer.poll, 1.0)  # Poll Kafka message asynchronously

                if msg is None:
                    await asyncio.sleep(STREAM_DELAY)
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        raise KafkaException(msg.error())

                order = await asyncio.to_thread(avro_deserializer, msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
                if order:
                    yield {
                        "event": "new_order",
                        "data": order
                    }

                await asyncio.sleep(STREAM_DELAY)

        except Exception as e:
            # Handle exceptions
            print("Error:", e)
            raise

        finally:
            consumer.close()

    return EventSourceResponse(event_generator())