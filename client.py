import asyncio
import json
import sys

name_server_host = "127.0.0.1"
name_server_port = 6000
class Client:
    def __init__(self,reader, writer,service,text):
        self.reader=reader
        self.writer=writer
        self.service=service
        self.text=text
    async def run(self):
        self.writer.write(f"{json.dumps({'cmd':'lookup', 'service':self.service})}\n".encode())
        await self.writer.drain()
        ricezione=await self.reader.readline()
        if not ricezione:
            print("disconnessione dal NameServer")
            return
        ricezione=json.loads(ricezione.decode().strip("\n"))
        if ricezione["status"]=="error":
            print(f"errore: {ricezione['message']}")
            return
        host=ricezione["host"]
        port=ricezione["port"]
        self.writer.close()
        await self.writer.wait_closed()
        self.reader, self.writer = await asyncio.open_connection(host, port)
        if self.service=="echo":
            self.writer.write(f"{json.dumps({'cmd':'echo', 'text':self.text})}\n".encode())
            await self.writer.drain()
            ricezione=await self.reader.readline()
            if not ricezione:
                print("disconnessione dal server echo")
                return
            ricezione=json.loads(ricezione.decode().strip("\n"))
            if ricezione["status"]=="error":
                print(f"errore: {ricezione['message']}")
                return
            print(f"risposta dal server echo: {ricezione['text']}")
        if self.service=="time":
            self.writer.write(f"{json.dumps({'cmd':'time'})}\n".encode())
            await self.writer.drain()
            ricezione=await self.reader.readline()
            if not ricezione:
                print("disconnessione dal server time")
                return
            ricezione=json.loads(ricezione.decode().strip("\n"))
            if ricezione["status"]=="error":
                print(f"errore: {ricezione['message']}")
                return
            print(f"risposta dal server time: {ricezione['time']}")
async def main():
    service=sys.argv[1]
    if service=="echo":
        text=sys.argv[2]
    else:
        text=None
    reader, writer = await asyncio.open_connection(name_server_host, name_server_port)
    await Client(reader, writer, service, text).run()

try:
    asyncio.run(main())
except KeyboardInterrupt:
    print("Chiusura del client...")
