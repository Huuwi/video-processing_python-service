
from fastapi import FastAPI
from contextlib import asynccontextmanager
from app.services.consumer import rabbit_consumer

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    print("Starting RabbitMQ Consumer...")
    rabbit_consumer.start()
    yield
    # Shutdown
    print("Shutting down...")

app = FastAPI(title="Video Processing Service", lifespan=lifespan)

@app.get("/")
def read_root():
    return {"status": "ok", "service": "Video Processing Worker"}

@app.get("/health")
def health_check():
    return {"status": "healthy"}
