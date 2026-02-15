
import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # App
    APP_NAME: str = "Video Processing Worker"
    
    # RabbitMQ
    RABBITMQ_URL: str = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672")
    
    # MongoDB
    MONGO_URI: str = os.getenv("MONGO_URI", "mongodb://localhost:27018/")
    DB_NAME: str = "video-processing"
    
    # MinIO
    MINIO_ENDPOINT: str = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    MINIO_ACCESS_KEY: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    MINIO_SECRET_KEY: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    MINIO_BUCKET: str = "videos"
    MINIO_SECURE: bool = False
    
    # External APIs
    DOUYIN_API: str = os.getenv("DOUYIN_API", "https://douyin-tiktok-download-api-sdui.onrender.com/")

    MAX_DURATION_MINUTES: int = 60
    CHUNK_DURATION_MINUTES: int = 5
    
    # Video Processing
    SPEED_RATE: float = 1.25  # Default speed multiplier for downloaded videos
    
    # Vbee API
    VBEE_APP_ID: str = ""
    VBEE_TOKEN: str = ""

    # Concurrency
    CONCURRENT_WORKERS: int = int(os.getenv("CONCURRENT_WORKERS", 4))

    class Config:
        env_file = ".env"
        extra = "ignore"

settings = Settings()
