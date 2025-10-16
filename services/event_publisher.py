from abc import ABC, abstractmethod
import time
from azure.servicebus import ServiceBusClient, ServiceBusMessage
import os
import json
import pika
from dotenv import load_dotenv

class EventPublisher(ABC):

    @abstractmethod
    def send_event(self, events):
        pass

    @abstractmethod
    def close(self):
        pass

class MockEventPublisher(EventPublisher):

    def __init__(self):
        print("CONNECTED TO FAKE BROKER")

    def send_event(self, events):
        for event in events:
            print(f'{event['event_name']} succefully sent')
        time.sleep(1.5)

    def close(self):
        print("Closing BROKER Connection")
        time.sleep(1)
        print("Connection Closed")

class RabbitMQEventPublisher(EventPublisher):

    def __init__(self):
        load_dotenv()
        host = os.getenv("RABBIT_URL")
    
        print("Connecting to RMQ")

        try:

            parameters = pika.ConnectionParameters(host=host)
            self.connection = pika.BlockingConnection(parameters=parameters)

            self.channel = self.connection.channel()

        except Exception as e:
            print("Error while connecting")

        print("success")


    def send_event(self, events):
        

        for event in events:
            self.channel.basic_publish(
                exchange='events',
                routing_key='',
                body=json.dumps(event)
            )

    def close(self):
        self.connection.close()


class AzureEventPublisher(EventPublisher):

    def __init__(self):
        load_dotenv()
        connection_string = os.getenv("SERVICE_BUS_CONNECTION_STRING")
        topic_name = os.getenv("SERVICE_BUS_TOPIC_NAME")
        self.client = ServiceBusClient.from_connection_string(connection_string)
        self.sender = self.client.get_topic_sender(topic_name)
        print("CONNECTED TO AZURE")
    
    def send_event(self, events):
        if not events:
            return

        try:
            batch = self.sender.create_message_batch()
            for event in events:

                message = ServiceBusMessage(json.dumps(event))
                
                try:

                    batch.add_message(message)
                except ValueError:
                   
                    print("Batch is full. Sending current batch and starting a new one.")
                    self.sender.send_messages(batch)
                    batch = self.sender.create_message_batch()
                    batch.add_message(message)

            if batch:
                self.sender.send_messages(batch)
            
            print(f"Successfully sent a batch of {len(events)} events to Azure.")

        except Exception as e:
            print(f"Error sending event batch to Azure: {e}")

    def close(self):
        print("Closing Azure Service Bus sender...")
        self.sender.close()
        self.service_bus_client.close()
        print("Azure sender closed.")

