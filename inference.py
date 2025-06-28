import os
import json
import base64
import cv2
import numpy as np
import random
import asyncio
import asyncpg
from aiokafka import AIOKafkaConsumer

DB_CONFIG = {
    'user': 'postgres',
    'password': 'mysecretpassword',
    'database': 'postgres',
    'host': 'localhost',
    'port': 5432,
}

KAFKA_TOPIC = "inference_frames"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
OBJECTS = ["dog", "cat", "door", "tree", "car", "person"]

os.makedirs("frames", exist_ok=True)


async def decode_and_save_image(scenario_id, frame_index, image_base64):
    def process():
        img_bytes = base64.b64decode(image_base64)
        img_np = np.frombuffer(img_bytes, dtype=np.uint8)
        img = cv2.imdecode(img_np, cv2.IMREAD_COLOR)
        filename = f"frames/{scenario_id}_{frame_index}.jpg"
        cv2.imwrite(filename, img)
        return random.choice(OBJECTS)
    return await asyncio.to_thread(process)


async def handle_message(msg, conn):
    data = msg.value
    scenario_id = data["scenario_id"]
    frame_index = data["frame_index"]
    image_base64 = data["image_base64"]

    obj = await decode_and_save_image(scenario_id, frame_index, image_base64)

    async with conn.acquire() as cur:
        if frame_index == 0:
            await cur.execute(
                "UPDATE scenarios SET predict = $1 WHERE id = $2;",
                obj, scenario_id
            )
        else:
            row = await cur.fetchrow("SELECT predict FROM scenarios WHERE id = $1;", scenario_id)
            current = row["predict"] or ""
            updated = f"{current} {obj}".strip()
            await cur.execute(
                "UPDATE scenarios SET predict = $1 WHERE id = $2;",
                updated, scenario_id
            )

    print(f"[Inference] Frame {frame_index} of scenario {scenario_id} → predicted: {obj}")


async def main():
    conn = await asyncpg.create_pool(**DB_CONFIG)

    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id='inference-group',
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    await consumer.start()

    print("[Inference] Waiting for frames...")
    try:
        async for msg in consumer:
            await handle_message(msg, conn)
    finally:
        await consumer.stop()
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
