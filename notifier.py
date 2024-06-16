#!/app/notifier/venv/bin/python3
import asyncio
import struct
import sys
import os
import logging
import dns.message
import dns.query
import dns.name
import dns.rrset
from netaddr import IPNetwork as CIDR, IPAddress as IP
from multiprocessing import Process, cpu_count, Pipe, current_process, Manager
from asyncio.selector_events import _SelectorSocketTransport as TCP
from threading import Thread

def udp_sender(Q:dns.message.Message, ip):
    dns.query.udp(Q,ip)

def tcp_sender(Q:dns.message.Message, ip):
    dns.query.tcp(Q,ip,0.1)

def handle(data:bytes, addr:tuple, transport, netmask):
    Q = dns.message.from_wire(data)
    if Q.question[0].name == dns.name.from_text('example.com.'):
        R = dns.message.make_response(Q)
        record = dns.rrset.from_text('example.com.', 60, 'IN', 'A', '127.0.0.1')
        R.answer.append(record)
        return R.to_wire()
    if isinstance(transport, TCP):
        pass
    else:
        for ip in netmask:
            Thread(target=udp_sender,args=(Q,str(ip))).start()
    print(Q.opcode())
    
# --- UDP socket ---
class UDPserver(asyncio.DatagramProtocol):

    def __init__(self, netmask: CIDR) -> None:
        super().__init__()
        self.netmask = netmask

    def connection_made(self, transport:asyncio.DatagramTransport,):
        self.transport = transport

    def datagram_received(self, data, addr):
        result = handle(data, addr, self.transport, self.netmask)
        if result: data = result
        self.transport.sendto(data, addr)


# -- TCP socket --
class TCPServer(asyncio.Protocol):

    def __init__(self, netmask: CIDR) -> None:
        super().__init__()   
        self.netmask = netmask
    
    def connection_made(self, transport:asyncio.Transport):
        self.transport = transport

    def data_received(self, data):
        addr = self.transport.get_extra_info('peername')
        result = handle(data[2:], addr, self.transport, self.netmask)
        if result:
            l = struct.pack('>H',len(result))
            self.transport.write(l+result)

def listener(ip, port, netmask: CIDR, isudp:bool=True,):
    try:
        loop = asyncio.new_event_loop()
        if isudp is True:
            addr = (ip, port)
            listen = loop.create_datagram_endpoint(lambda: UDPserver(netmask), addr, reuse_port=True)
            transport, protocol = loop.run_until_complete(listen)
            logging.info(f'Started listen at {ip, port} UDP')
        else:
            listen = loop.create_server(lambda: TCPServer(netmask),ip,port,reuse_port=True)
            transport = loop.run_until_complete(listen)
            logging.info(f'Started listen at {ip, port} TCP')
        loop.run_forever()
        transport.close()
        loop.run_until_complete(transport.wait_closed())
        loop.close()
    except KeyboardInterrupt:
        pass
    except:
        logging.critical(f'Start new listener is fail {current_process().name}.', exc_info=True)
        sys.exit(1)

def start(pool, socket: str):
    try:
        netmask = CIDR(pool,4)
        ip, port = socket.split(':')
        IP(ip,4)
        Stream = []
        p1 = Process(target=listener, args=(ip, port, netmask, True), name='UDP.Process.Notifier')
        p2 = Process(target=listener, args=(ip, port, netmask, False), name='TCP.Process.Notifier')
        p1.start()
        p2.start()
        Stream.append(p1)
        Stream.append(p2)
        for p in Stream:
            p.join()
    except Exception as e:
        logging.critical(msg=e, exc_info=True)
    

if __name__ == "__main__":
    logging.root.setLevel(logging.INFO)
    try: logging.root.setLevel(int(sys.argv[3]))
    except: logging.root.setLevel(logging.ERROR)
    print('logging level is %s' % logging.root.level)
    if len(sys.argv) < 3:
        logging.critical(f'''Usage: 
    {os.path.abspath(__file__)} netmask socket [loglevel]
        netmask - netmask of DNS slaves like 192.168.1.0/24
        socket - ip and port of listen notify from master like 127.0.0.1:5310
        loglevel - level of logging (10=*DEBUG*|20=*INFO*|40=*ERROR*|50=*CRITICAL*)''')
        sys.exit(1)
    start(sys.argv[1], sys.argv[2])
