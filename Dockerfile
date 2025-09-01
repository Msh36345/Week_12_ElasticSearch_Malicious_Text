FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN python -m nltk.downloader vader_lexicon

COPY services/ ./services/
COPY data/ ./data/

ENV PYTHONPATH=/app

CMD ["python", "-m", "services.main"]
