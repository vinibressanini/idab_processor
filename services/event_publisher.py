from azure.servicebus import ServiceBusClient, ServiceBusMessage
import os
import json
from dotenv import load_dotenv

class EventPublisher():

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
            # 1. Create a batch object
            batch = self.sender.create_message_batch()

            for event in events:
                # 2. Convert each event dictionary to a JSON string and create a message
                message = ServiceBusMessage(json.dumps(event))
                
                # 3. Try to add the message to the batch
                try:
                    batch.add_message(message)
                except ValueError:
                    # This error occurs if the message is too large for the batch.
                    # We send the current batch and start a new one.
                    print("Batch is full. Sending current batch and starting a new one.")
                    self.sender.send_messages(batch)
                    batch = self.sender.create_message_batch()
                    batch.add_message(message) # Add the message that was too large

            # 4. Send any remaining messages in the final batch
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
