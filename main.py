import uvicorn
from fastapi import FastAPI

from market_pipeline.api import create_app
from market_pipeline.model import PipelineConfig

app = create_app(PipelineConfig(data_dir="./data"))


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
