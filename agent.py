import asyncio
import websockets
import json
import logging
import os
import sys
from dotenv import load_dotenv


async def listen_config():
    os.makedirs(os.path.dirname(LOG_FILE_PATH), exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE_PATH),
            logging.StreamHandler(sys.stdout),
        ],
    )

    while True:
        try:
            async with websockets.connect(WS_URL) as websocket:
                logging.info("Connected to config server via WebSocket")

                async for message in websocket:
                    logging.info("New configuration received!")
                    data = json.loads(message)

                    logging.info(
                        f"Fetched config for plant {data.get('idplant')}: {data.get('config')}"
                    )

                    with open(LOCAL_CONFIG_PATH, "w") as config_file:
                        json.dump(data.get("config"), config_file, indent=4)

                    logging.info("Configuration successfully updated!")

                    await asyncio.sleep(10)

                    await websocket.send(
                        json.dumps({"status": 1, "idplant": data.get("idplant"), "iddeploy" : data.get("iddeploy")})
                    )

        except Exception as e:
            logging.error(
                f"Connection error: {e}, retrying in 5 seconds...", exc_info=True
            )
            await websocket.send(
                json.dumps({"status": 2, "idplant": data.get("idplant")})
            )


if __name__ == "__main__":
    load_dotenv()

    LOCAL_CONFIG_PATH = os.getenv("config_file")
    LOG_FILE_PATH = os.getenv("log_file")
    WS_URL = os.getenv("ws_config_url")

    asyncio.run(listen_config())
