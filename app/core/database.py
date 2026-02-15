
from pymongo import MongoClient
from minio import Minio
from app.core.config import settings

# MongoDB
mongo_client = MongoClient(settings.MONGO_URI)
db = mongo_client[settings.DB_NAME]
video_collection = db['videos']
audio_collection = db['audios']
voice_chunks_collection = db['voice_chunks']
user_collection = db['users']

# MinIO
minio_client = Minio(
    settings.MINIO_ENDPOINT,
    access_key=settings.MINIO_ACCESS_KEY,
    secret_key=settings.MINIO_SECRET_KEY,
    secure=settings.MINIO_SECURE
)

# Ensure bucket exists
if not minio_client.bucket_exists(settings.MINIO_BUCKET):
    minio_client.make_bucket(settings.MINIO_BUCKET)
