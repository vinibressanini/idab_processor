from event_publisher import EventPublisher
from outbox import fetch_unpublished, mark_published, mark_failed
import time
import logging
from typing import Optional


class OutboxRelay:
    def __init__(
        self,
        sleep_interval: int = 5,
        batch_size: int = 50,
        ttl_seconds: int = 86400,      # TTL: 24 hours
        max_retries: int = 5,          # Backoff: Max attempts
        base_delay_seconds: int = 2,   # Backoff: Initial delay
    ):
        self.sleep_interval = sleep_interval
        self.batch_size = batch_size
        self.ttl_seconds = ttl_seconds
        self.max_retries = max_retries
        self.base_delay_seconds = base_delay_seconds
        self.running = True
        self.sender: Optional[EventPublisher] = None

    # In outbox_relay.py

    def publish_outbox_events(self) -> None:
        now = int(time.time())
        events = list(fetch_unpublished(limit=self.batch_size))

        if not events:
            return

        
        events_to_publish = []
        events_to_mark_failed = []

        for event in events:
            event_age = now - event['created_at']
            if event_age > self.ttl_seconds:
                error_msg = f"Event expired after {event_age} seconds (TTL is {self.ttl_seconds}s)."
                print(f"🗑️ Event {event['id']}: {error_msg}")
                events_to_mark_failed.append((event, error_msg))
            else:
                events_to_publish.append({
                    "id": event['id'],
                    "event_name": event['event_name'],
                    "payload": event['payload'],
                    "created_at": event['created_at']
                })

        try:
            if events_to_publish:
                print(f"Publishing a batch of {len(events_to_publish)} events...")
                
                self.sender.send_event(events_to_publish)
                
                for event in events_to_publish:
                    mark_published(event['id'])
                
                print(f"Batch of {len(events_to_publish)} events published successfully.")

        except Exception as e:
            error_msg = str(e)
            print(f"Entire batch failed to publish: {error_msg}")
            for event in events_to_publish:
                events_to_mark_failed.append((event, error_msg))

        for event, error_msg in events_to_mark_failed:
            mark_failed(
                event_id=event['id'],
                error=error_msg,
                current_attempts=event['attempts'],
                max_retries=self.max_retries,
                base_delay=self.base_delay_seconds
            )

    def start(self):
        print("Starting Outbox Relay service...")
        self.sender = EventPublisher()
        
        while self.running:
            try:
                self.publish_outbox_events()
                time.sleep(self.sleep_interval)
            except KeyboardInterrupt:
                print("Shutting down Outbox Relay...")
                break
            except Exception as e:
                logging.error(f"Unexpected error: {e}")
                time.sleep(self.sleep_interval)  # Wait before retrying

if __name__ == "__main__":
    relay = OutboxRelay()
    relay.start()