import socket
s = socket.socket()
try:
    s.settimeout(2)
    s.connect(("127.0.0.1", 6379))
    print(" conectado 127.0.0.1 6379")
except Exception as e:
    print(" no conectado ", e)
finally:
    s.close()