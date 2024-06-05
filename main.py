from telethon import TelegramClient, events
import asyncio
import pandas as pd
import os
import time
import logging  

# https://my.telegram.org/auth
API_ID: str = os.getenv('API_ID', '')
API_HASH: str = os.getenv('API_HASH', '')
GROUP_ID: str = os.getenv('GROUP_ID', '')
PHONE_NUMBER: str = os.getenv('PHONE_NUMBER', '')
DOWNLOAD_PATH: str = os.getenv('DOWNLOAD_PATH', '')
CSV_FILE_PATH: str = os.getenv('CSV_FILE_PATH', '')

INTERVAL = 15

async def send_message(client, GROUP_ID, message):
    await client.send_message(GROUP_ID, message)

def validate_cpf(cpf):
    cpf_str = str(cpf)
    if len(cpf_str) < 11:
        return cpf_str.zfill(11)
    else: 
        return cpf

async def process_file(client):
    df = pd.read_csv(CSV_FILE_PATH)
    cpfs = df.iloc[:, 2].tolist()
    for cpf in cpfs:
        valid_cpf = validate_cpf(cpf)
        if valid_cpf:
            message = f"/cpf {valid_cpf}"
            await send_message(client, GROUP_ID, message)
            logging.warning(f"CPF {valid_cpf} solicitado")
            time.sleep(INTERVAL)

async def start_handler(client):
    @client.on(events.NewMessage(chats=GROUP_ID))
    async def handler(event):
        if event.message.file:
            file_path = os.path.join(DOWNLOAD_PATH, event.message.file.name)
            await event.message.download_media(file=file_path)
            logging.warning(f"Arquivo {event.message.file.name} baixado.")
            await process_file(client)

    logging.warning(f"Escutando novas mensagens no grupo {GROUP_ID}...")
    await client.run_until_disconnected()

async def run_client():
    client = TelegramClient('anon', API_ID, API_HASH)
    await client.start(phone=PHONE_NUMBER)
    await asyncio.gather(
        start_handler(client),
        process_file(client)
    )

if __name__ == '__main__':
    asyncio.run(run_client())
