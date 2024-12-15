# Stock-Analyst

## Quick start with Docker

```
docker build -t stock-analyst .

docker run -d \
    --name sec-filings-app \
    -p 8501:8501 \
    -p 8502:8502 \
    -e DB_HOST=host.docker.internal \
    -e DB_PORT= \
    -e DB_USER=root \
    -e DB_PASSWORD= \
    -e DB_NAME=sec \
    -e LLAMA_CLOUD_API_KEY=llx- \
    -e GAIA_API_KEY=GAIA \
    -e GAIA_API_URL=https://llama.us.gaianet.network/v1 \
    -e MODEL_NAME=llama \
    -e TAVILY_API_KEY=tavily_api_key \
    stock-analyst
```

## Build and run 

```
git clone https://github.com/LlamaEdge/stock-analyst.git
cd stock-analyst
pip install -r requirements.txt
```

Set env variables

```
export DB_HOST=localhost
export DB_PORT=
export DB_USER=root
export DB_PASSWORD=
export DB_NAME=
export LLAMA_CLOUD_API_KEY=llama_cloud_api_key
export GAIA_API_KEY=gaia_api_key
export GAIA_API_URL=gaia_api_url
export MODEL_NAME=llama
export TAVILY_API_KEY=tavily_api_key
```

To start the database manager and chatbot 

```
streamlit run app2.py  & streamlit run app3.py 
```
