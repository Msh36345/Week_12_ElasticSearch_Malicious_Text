# FastAPI and Elasticsearch Project

This project uses **FastAPI** and **Elasticsearch**.

## Features

- Loads tweets from a CSV file.
- Finds if tweets are antisemitic.
- Finds if tweets talk about weapons.
- Finds the feeling (sentiment) of tweets.
- Saves all data in Elasticsearch.
- Shows data with a web API.

## How to run

### Option A1 – Run with Docker Compose (from Docker Hub image)

```
docker-compose up --build
```

### Option A2 – Run locally with existing Elasticsearch container

1. Make sure Elasticsearch is running on the `elasticsearch-stack_elastic_network` network at `http://elasticsearch:9200`.
2. Build the Docker image:
   ```
   docker build -t malicious-text-app .
   ```
3. Run the Docker container:
   ```
   docker run -it --rm \
     --name malicious-text-container \
     --net elasticsearch-stack_elastic_network \
     -p 8000:8000 \
     malicious-text-app
   ```

Open your browser and navigate to:
- [http://localhost:8000/multiple_weapons](http://localhost:8000/multiple_weapons)
- [http://localhost:8000/antisemitic_with_weapons](http://localhost:8000/antisemitic_with_weapons)
