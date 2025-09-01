docker build -t malicious-text-app .

docker run -it --rm \
  --name malicious-text-container \
  --net elasticsearch-stack_elastic_network \
  -p 8000:8000 \
  malicious-text-app
