import asyncio
import websockets
import json
import logging
import os
import sys
import subprocess
from dotenv import load_dotenv

class AsyncProcessManager:
    """A non-blocking process manager for an asyncio application."""
    def __init__(self, script_to_run):
        self.script_to_run = script_to_run
        self.process = None
        self.logger = logging.getLogger(__name__)

    async def start_generator(self):
        """Starts the event generator script as a child process."""
        if self.process and self.process.returncode is None:
            self.logger.info("Generator is already running.")
            return
            
        self.logger.info(f"Starting {self.script_to_run}...")
        # Use asyncio's non-blocking subprocess creation
        self.process = await asyncio.create_subprocess_exec(
            sys.executable, self.script_to_run
        )
        self.logger.info(f"Generator started with PID: {self.process.pid}")

    async def stop_generator(self):
        """Stops the event generator script gracefully."""
        if not self.process or self.process.returncode is not None:
            self.logger.info("Generator is not running.")
            return
            
        self.logger.info(f"Stopping generator with PID: {self.process.pid}...")
        self.process.terminate()
        # Use asyncio's non-blocking wait
        await self.process.wait()
        self.process = None
        self.logger.info("Generator stopped.")

    async def restart_generator(self):
        """Stops the old process and starts a new one."""
        self.logger.info("--- Restart sequence initiated ---")
        await self.stop_generator()
        await asyncio.sleep(1) # Give a moment before restarting
        await self.start_generator()
        self.logger.info("--- Restart sequence complete ---")

async def listen_config(manager, config_path):
    """Listens for new configs and manages the generator process."""
    while True:
        try:
            async with websockets.connect(WS_URL) as websocket:
                logging.info("Connected to config server via WebSocket")
                # Start the generator for the first time on successful connection
                await manager.start_generator()

                async for message in websocket:
                    logging.info("New configuration received!")
                    data = json.loads(message)

                    # Write the new config file
                    with open(config_path, "w") as config_file:
                        json.dump(data.get("config"), config_file, indent=4)
                    logging.info(f"Configuration file '{config_path}' successfully updated.")
                    
                    # Restart the generator to apply the new config
                    await manager.restart_generator()
                    
                    # Send confirmation back to the server
                    await websocket.send(
                        json.dumps({"status": 1, "idplant": data.get("idplant"), "iddeploy": data.get("iddeploy")})
                    )

        except (websockets.exceptions.ConnectionClosed, ConnectionRefusedError) as e:
            logging.error(f"WebSocket connection error: {e}. Retrying in 10 seconds...")
            await manager.stop_generator() # Stop the generator if connection is lost
            await asyncio.sleep(10)
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}", exc_info=True)
            await manager.stop_generator()
            await asyncio.sleep(10)


if __name__ == "__main__":
    load_dotenv()

    # --- Configuration ---
    LOCAL_CONFIG_PATH = os.getenv("config_file", "config.json")
    LOG_FILE_PATH = os.getenv("log_file", "agent.log")
    WS_URL = os.getenv("ws_config_url")
    GENERATOR_SCRIPT = "main.py"

    # --- Logging Setup ---
    os.makedirs(os.path.dirname(LOG_FILE_PATH), exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(LOG_FILE_PATH), logging.StreamHandler(sys.stdout)],
    )

    if not WS_URL:
        logging.error("WebSocket URL (ws_config_url) not set in .env file. Exiting.")
        sys.exit(1)

    manager = AsyncProcessManager(script_to_run=GENERATOR_SCRIPT)
    try:
        asyncio.run(listen_config(manager, LOCAL_CONFIG_PATH))
    except KeyboardInterrupt:
        logging.info("Agent shutting down manually.")
    finally:
        # Ensure the generator is stopped when the agent exits
        if manager.process:
            logging.info("Cleaning up child process on exit...")
            # Create a new loop to run the final cleanup task
            asyncio.run(manager.stop_generator())