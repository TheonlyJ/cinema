import http.client
import os
import random

import fastapi
import requests

monolith_url = os.environ.get('MONOLITH_URL')
movies_url = os.environ.get('MOVIES_SERVICE_URL')
gradual = True if os.environ.get('GRADUAL_MIGRATION', False) == 'true' else False
movies_migration_percent = int(os.environ.get('MOVIES_MIGRATION_PERCENT', 50))
app = fastapi.FastAPI()


def select_backend():
    if not gradual:
        return movies_url
    else:
        roll = random.randint(0, 100)
        if roll < movies_migration_percent:
            return movies_url
        else:
            return monolith_url


@app.get("/api/movies")
async def get_movies():
    backend_url = select_backend()
    response = requests.get(backend_url+'/api/movies')
    return response.json()

@app.post("/api/movies")
async def post_movies(item):
    backend_url = select_backend()
    response = requests.post(backend_url+'/api/movies', data=item)
    return response.json()

@app.get("/api/users")
async def get_users():
    response = requests.get(monolith_url+'/api/users')
    return response.json()

@app.post("/api/users")
async def post_users(item):
    backend_url = select_backend()
    response = requests.post(backend_url+'/api/users', data=item)
    return response.json()


@app.get("/health")
async def get_health(status_code=200):
    return {'status': True}
