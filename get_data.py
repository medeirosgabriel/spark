import json
import websocket
import datetime
import socket

def ws_trades(c):

    socket = f'wss://stream.binance.com:9443/ws/ethusdt@trade'

    def on_message(wsapp, message):  
        json_message = json.loads(message)
        data = handle_trades(json_message)
        data = str(data)
        c.send(data.encode('utf-8'))

    def on_error(wsapp, error):
        wsapp.close()
        print(error)

    wsapp = websocket.WebSocketApp(socket, on_message=on_message, on_error=on_error)
    wsapp.run_forever()
    
def handle_trades(json_message):
    date_time = datetime.datetime.fromtimestamp(json_message['E']/1000).strftime('%Y/%m/%d-%H:%M:%S')
    #data = {"Symbol": json_message['s'], "PRICE": json_message['p'], "QTY": json_message['q'], "TIMESTAMP": str(date_time)}
    data = str(date_time) + " " + json_message['p'] + "\n"
    return data

def start():
    s = socket.socket()    # Create a socket object
    host = "127.0.0.1"     # Get local machine name
    port = 5000            # Reserve a port for your service.
    s.bind((host, port))

    print("Listening on port: %s" % str(port))

    s.listen(5)            # Now wait for client connection.
    c, addr = s.accept()   # Establish connection with client.

    print( "Received request from: " + str( addr ) )
    ws_trades(c)

while True:
    start()
