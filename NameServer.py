import asyncio
import datetime
import json
HOST = "127.0.0.1"
services={
    "time": (HOST, 6001),
    "echo": (HOST, 6002),
}

class NameServer:
    def __init__(self, reader, writer):
        self.reader=reader
        self.writer=writer
        self.cmd=None
        self.service=None
        self.ricezione=None
    async def run(self):
        addr=self.writer.get_extra_info("peername")[0]
        print(f"nuovo client connesso con ip @{addr}")
        while True:
            try:
                self.ricezione=await self.reader.readline()
                if not self.ricezione:
                    print(f"client @{addr} disconnesso")
                    break
                self.ricezione=json.loads(self.ricezione.decode().strip("\n"))
                self.cmd=self.ricezione["cmd"].lower()
                if self.cmd!="lookup":
                    self.writer.write(f"{json.dumps({"status":"error", "message":"comando non valido"})}\n".encode())
                    await self.writer.drain()
                    break
                self.service=self.ricezione["service"].lower()
                if self.service!="echo" and self.service!="time":
                    self.writer.write(f"{json.dumps({"status":"error", "message":"servizio non valido"})}\n".encode())
                    await self.writer.drain()
                    break
                if self.service=="echo":
                    port=services["echo"][1]
                elif self.service=="time":
                    port=services["time"][1]
                self.writer.write(f"{json.dumps({"status":"ok","service":self.service, "host":HOST,"port":port})}\n".encode())
                await self.writer.drain()
            except(asyncio.IncompleteReadError, ConnectionResetError):
                print(f"client @{addr} disconnesso")
            
                break
            except(json.JSONDecodeError):
                self.writer.write(f"{json.dumps({'status':'error', 'message':'formato JSON non valido'})}\n".encode())
                await self.writer.drain()
        self.writer.close()
        await self.writer.wait_closed()
class TimeServer:
    def __init__(self, reader, writer):
        self.reader=reader
        self.writer=writer
        self.cmd=None
        self.ricezione=None
    async def run(self):
        addr=self.writer.get_extra_info("peername")[0]
        print(f"nuovo client connesso con ip @{addr}")
        while True:
            try:
                self.ricezione=await self.reader.readline()
                if not self.ricezione:
                    print(f"client @{addr} disconnesso")
                    break
                self.ricezione=json.loads(self.ricezione.decode().strip("\n"))
                self.cmd=self.ricezione["cmd"].lower()
                if self.cmd!="time":
                    self.writer.write(f"{json.dumps({'status':'error', 'message':'comando non valido'})}\n".encode())
                    await self.writer.drain()
                    break
                self.writer.write(f"{json.dumps({'status':'ok', 'time':str(datetime.datetime.now())})}\n".encode())
                await self.writer.drain()
                
                break
            except(asyncio.IncompleteReadError, ConnectionResetError):
                print(f"client @{addr} disconnesso")
                break
            except(json.JSONDecodeError):
                self.writer.write(f"{json.dumps({'status':'error', 'message':'formato JSON non valido'})}\n".encode())
                await self.writer.drain()
        self.writer.close()
        await self.writer.wait_closed()
class EchoServer:
    def __init__(self, reader, writer):
        self.reader=reader
        self.writer=writer
        self.cmd=None
        self.text=None
        self.ricezione=None
    async def run(self):
        addr=self.writer.get_extra_info("peername")[0]
        print(f"nuovo client connesso con ip @{addr}")
        while True:
            try:
                self.ricezione=await self.reader.readline()
                if not self.ricezione:
                    print(f"client @{addr} disconnesso")
                    break
                self.ricezione=json.loads(self.ricezione.decode().strip("\n"))
                self.cmd=self.ricezione["cmd"].lower()
                if self.cmd!="echo":
                    self.writer.write(f"{json.dumps({'status':'error', 'message':'comando non valido'})}\n".encode())
                    await self.writer.drain()
                    break
                self.text=self.ricezione["text"]
                self.writer.write(f"{json.dumps({'status':'ok', 'text':self.text})}\n".encode())
                await self.writer.drain()
                break
            except(asyncio.IncompleteReadError, ConnectionResetError):
                print(f"client @{addr} disconnesso")
                break
            except(json.JSONDecodeError):
                self.writer.write(f"{json.dumps({'status':'error', 'message':'formato JSON non valido'})}\n".encode())
                await self.writer.drain()
        self.writer.close()
        await self.writer.wait_closed()
async def name_handler(reader, writer):
    await NameServer(reader, writer).run()

async def echo_handler(reader, writer):
    await EchoServer(reader, writer).run()

async def time_handler(reader, writer):
    await TimeServer(reader, writer).run()

async def main():
    name_server = await asyncio.start_server(name_handler, "", 6000)
    echo_server = await asyncio.start_server(echo_handler, "", services["echo"][1])
    time_server = await asyncio.start_server(time_handler, "", services["time"][1])

    async with name_server, echo_server, time_server:
        await asyncio.gather(
            name_server.serve_forever(),
            echo_server.serve_forever(),
            time_server.serve_forever(),
        )

asyncio.run(main())