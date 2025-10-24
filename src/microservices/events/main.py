import json
import os
import threading
import time
from contextlib import asynccontextmanager
from typing import Optional, List
from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
import pydantic
from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

broker = os.environ.get("KAFKA_BROKERS")


@asynccontextmanager
async def lifespan(app: FastAPI):
    thr = threading.Thread(target=consume)
    thr.start()
    yield
    thr.join()


app = FastAPI(lifespan=lifespan)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    exc_str = f'{exc}'.replace('\n', ' ').replace('   ', ' ')
    print(f"{request.body}: {exc_str}")
    content = {'status_code': 10422, 'message': exc_str, 'data': None}
    return JSONResponse(content=content, status_code=status.HTTP_422_UNPROCESSABLE_ENTITY)


@app.get("/api/events/health", status_code=200)
async def health():
    return {"status": True}


class Movie(pydantic.BaseModel):
    movie_id: int
    title: str
    action: str
    user_id: Optional[int] = None
    rating: Optional[float] = None
    genres: Optional[List[str]] = None
    description: Optional[str] = None


@app.post("/api/events/movie", status_code=201)
async def movie(item: Movie):
    produce(item, 'movie-events')
    return {"status": 'success'}


class User(pydantic.BaseModel):
    user_id: int
    username: Optional[str] = None
    email: Optional[str] = None
    action: str
    timestamp: str

@app.post("/api/events/user", status_code=201)
async def user(item: User):
    produce(item, 'user-events')
    return {"status": 'success'}


class Payment(pydantic.BaseModel):
    payment_id: int
    user_id: int
    amount: float
    status: str
    timestamp: str
    method_type: Optional[str] = None


@app.post("/api/events/payment", status_code=201)
async def payment(item: Payment):
    produce(item, 'payment-events')
    return {"status": 'success'}


def produce(item, topic):
    producer = KafkaProducer(bootstrap_servers=broker)
    producer.send(topic, json.dumps(item.model_dump()).encode('utf-8'))
    producer.flush()
    producer.close()


def consume():
    connected = False
    while not connected:
        try:
            consumer = KafkaConsumer(bootstrap_servers=broker, group_id='events')
            connected = True
        except NoBrokersAvailable:
            time.sleep(5)
            continue
    consumer.subscribe(['movie-events', 'user-events', 'payment-events'])
    for msg in consumer:
        print(f'Consumed: {msg.value}', flush=True)
    consumer.close()
