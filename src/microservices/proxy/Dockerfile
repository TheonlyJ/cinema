FROM python:3.13-slim-bookworm
LABEL authors="Artem P"

COPY main.py .
RUN pip install fastapi[standard] requests

ENTRYPOINT ["uvicorn", "--host=0.0.0.0", "--port=8000", "main:app"]