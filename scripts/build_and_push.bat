REM Build Docker image, push to Docker Hub, then remove local image

docker build -t msh36345/fast_api_week_12 .
docker push msh36345/fast_api_week_12
docker rmi msh36345/fast_api_week_12