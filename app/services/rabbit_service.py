import pika
import json
from app.core.config import settings

def publish_event(queue_name: str, message: dict):
    """
    Publishes a message to a specific RabbitMQ queue.
    Encapsulates connection, channel declaration, and publishing.
    """
    connection = None
    try:
        # Create a blocking connection
        params = pika.URLParameters(settings.RABBITMQ_URL)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        
        # Declare queue (durable=True for persistence)
        channel.queue_declare(queue=queue_name, durable=True)
        
        # Publish message
        channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            )
        )
        print(f" [x] Sent '{message}' to '{queue_name}'")
        
    except Exception as e:
        print(f"Failed to publish message to {queue_name}: {e}")
        raise e
    finally:
        if connection and not connection.is_closed:
            connection.close()
