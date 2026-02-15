
import pika
import threading
import time
from app.core.config import settings
from app.services.speech import process_speech_to_audio
from app.services.video import process_download, process_edit

class RabbitMQConsumer:
    def __init__(self):
        self.url = settings.RABBITMQ_URL
        self._threads = []
        self._stop_event = threading.Event()

    def connect(self):
        params = pika.URLParameters(self.url)
        # Each thread will call this to get its own connection
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        
        # Assert queues
        channel.queue_declare(queue='topic_download', durable=True)
        channel.queue_declare(queue='topic_edit_process', durable=True)
        channel.queue_declare(queue='topic_speech_to_audio', durable=True)
        
        # Prefetch
        channel.basic_qos(prefetch_count=1)
        return connection, channel

    def start(self):
        num_workers = settings.CONCURRENT_WORKERS
        print(f"Starting {num_workers} RabbitMQ Consumer Workers...")
        for i in range(num_workers):
            thread = threading.Thread(target=self._run, args=(i,))
            thread.daemon = True
            thread.start()
            self._threads.append(thread)
    
    def _run(self, worker_id):
        while not self._stop_event.is_set():
            connection = None
            try:
                connection, channel = self.connect()
                print(f"Worker {worker_id}: RabbitMQ Connected. Waiting for messages...")
                
                # Use partial or a wrapper to pass the connection/channel to the callback if needed
                # But here _process_with_heartbeat is used, so it needs the connection
                
                channel.basic_consume(
                    queue='topic_download', 
                    on_message_callback=lambda ch, m, p, b, conn=connection, wid=worker_id: self._on_message(ch, m, p, b, process_download, conn, wid, "download")
                )
                channel.basic_consume(
                    queue='topic_edit_process', 
                    on_message_callback=lambda ch, m, p, b, conn=connection, wid=worker_id: self._on_message(ch, m, p, b, process_edit, conn, wid, "edit")
                )
                channel.basic_consume(
                    queue='topic_speech_to_audio', 
                    on_message_callback=lambda ch, m, p, b, conn=connection, wid=worker_id: self._on_message(ch, m, p, b, process_speech_to_audio, conn, wid, "speech")
                )
                
                channel.start_consuming()
            except pika.exceptions.ConnectionClosedByBroker:
                print(f"Worker {worker_id}: Connection closed by broker. Retrying...")
                time.sleep(5)
            except pika.exceptions.AMQPChannelError as err:
                print(f"Worker {worker_id}: Channel error: {err}, stopping worker...")
                break
            except pika.exceptions.AMQPConnectionError:
                print(f"Worker {worker_id}: Connection failed, retrying...")
                time.sleep(5)
            except Exception as e:
                print(f"Worker {worker_id}: Unexpected error: {e}")
                time.sleep(5)
            finally:
                if connection and not connection.is_closed:
                    connection.close()

    def _process_with_heartbeat(self, processing_func, body, connection):
        """
        Runs the processing function in a separate thread to allow
        the main thread to keep the Pika connection alive with heartbeats.
        """
        thread = threading.Thread(target=processing_func, args=(body,))
        thread.daemon = True
        thread.start()

        while thread.is_alive():
            try:
                connection.process_data_events()
                time.sleep(1)
            except Exception as e:
                print(f"Error maintaining heartbeat: {e}")
                break

    def _on_message(self, ch, method, properties, body, processing_func, connection, worker_id, task_type):
        print(f"Worker {worker_id}: Received {task_type} task")
        self._process_with_heartbeat(processing_func, body, connection)
        ch.basic_ack(delivery_tag=method.delivery_tag)

rabbit_consumer = RabbitMQConsumer()
