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

1. Run Elasticsearch on the network `elasticsearch-stack_elastic_network` at `http://elasticsearch:9200`.
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
