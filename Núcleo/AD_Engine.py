import sys
import socket
import threading
import mysql.connector
import time
from confluent_kafka import Producer, Consumer, KafkaError, TopicPartition
import tkinter as tk

HEADER = 64
FORMAT = 'utf-8'
SERVER = socket.gethostbyname(socket.gethostname())
FIN = "FIN"

STX = chr(0x02)
ETX = chr(0x03)

class AD_Engine:
    
    max_drones = 0
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    class DroneMap(tk.Tk):
        def __init__(self):
            super().__init__()
            self.title("Mapa de Drones")

            self.canvas = tk.Canvas(self, width=500, height=500, bg='white')
            self.canvas.pack()

            self.cell_width = 500 // 21
            self.cell_height = 500 // 21
            self.dronesmap = {}

            for row in range(1, 22):
                for col in range(1, 22):
                    x1 = (col - 1) * self.cell_width
                    y1 = (row - 1) * self.cell_height
                    x2 = x1 + self.cell_width
                    y2 = y1 + self.cell_height
                    self.canvas.create_rectangle(x1, y1, x2, y2, fill='white', outline='black')

            for i in range(1, 21):
                y = i * self.cell_height + self.cell_height // 2
                self.canvas.create_text(5, y, text=str(i), anchor='w')

            for i in range(1, 21):
                x = i * self.cell_width + self.cell_width // 2
                self.canvas.create_text(x, 5, text=str(i), anchor='n')
        
        def update_drone_position(self, row, col, id): 
            x1 = int(self.dronesmap[id][1]) * self.cell_width
            y1 = int(self.dronesmap[id][2]) * self.cell_height
            x2 = x1 + self.cell_width
            y2 = y1 + self.cell_height
            
            drone_x = row * self.cell_width
            drone_y = col * self.cell_height
            self.canvas.delete("drone" + "." + str(id))
            self.canvas.create_rectangle(drone_x, drone_y, drone_x + self.cell_width, drone_y + self.cell_height, fill='red', tags=("drone" + "." + str(id)))
            self.dronesmap[id] = (id, row, col)
            print("Drones_map en para la id " + str(id) + "es: " + str(self.dronesmap[id]))
        
        def add_drone(self, id):
            drone_x = 1 * self.cell_width
            drone_y = 1 * self.cell_height
            self.canvas.create_rectangle(drone_x, drone_y, drone_x + self.cell_width, drone_y + self.cell_height, fill='red', tags=("drone" + "." + str(id)))
            print("El id es: ", id)
            self.dronesmap[id] = (id, -1, -1)
            
        def positionReached(self, id):
            drone_x = self.dronesmap[id][1] * self.cell_width
            drone_y = self.dronesmap[id][2] * self.cell_height
            
            self.canvas.delete("drone" + "." + str(id))
            self.canvas.create_rectangle(drone_x, drone_y, drone_x + self.cell_width, drone_y + self.cell_height, fill='green', tags=("drone" + "." + str(id)))
        
        def stop_engine(self):
            self.engine.stop()
            
    def package_message(self, data):
        lrc = self.calculate_lrc(data)
        print("LRC: ", lrc)
        return STX + data + ETX + chr(lrc)
    
    def procesar_argumentos(self):
        
        args = sys.argv
        
        if len(args) != 9:
            print("Error en los argumentos")
            print("Uso: python AD_Engine.py <puerto_escucha> <max_drones> <ip_kafka> <puerto_kafka> <ip_weather> <puerto_weather> <ip_bd> <puerto_bd>")
            sys.exit(1)
        
        self.puerto_escucha = int(args[1])
        self.max_drones = int(args[2])
        self.ip_kafka = args[3]
        self.puerto_kafka = int(args[4])
        self.ip_weather = args[5]
        self.puerto_weather = int(args[6])
        self.ip_bd = args[7]
        self.puerto_bd = int(args[8])        
    
    def database(self, token):
        conexion = mysql.connector.connect(
            host=self.ip_bd,
            port=self.puerto_bd,
            user="root",
            password="1573",
            database="test"
        )
        
        cursor = conexion.cursor() 
        
        exists = "Select count(*) from drones where token = '%s'" % (token)
        cursor.execute(exists)
        exists = cursor.fetchone()[0]
        
        if exists == 1:
            myid = "Select id from drones where token = '%s'" %(token)
            cursor.execute(myid)
            id = cursor.fetchone()[0]
        
        return id
    
    def get_drones_from_file(self, file_path):
        datos_drones = []
        nombre_figura = None
        figura = []
        
        try: 
            with open(file_path, 'r') as file: 
                lines = file.readlines()
                                            
                for line in lines: 
                    if line.strip().startswith('</'):
                        if len(datos_drones) == 0: 
                            print("ERROR: No se han leído los datos de los drones")
                            sys.exit(1)
                        elif len(datos_drones) > self.max_drones:
                            print("ERROR: El número de drones a registrar supera el máximo permitido")
                            sys.exit(1)
                        else: 
                            figura = [nombre_figura, datos_drones]
                            self.figures.append(figura)
                            nombre_figura = None
                            datos_drones = []   
                    elif line.strip().startswith('<'):
                        parts = line.strip("<>\n").split('><')
                        if len(parts) == 3: 
                            id_dron, coordenada_x, coordenada_y = parts
                            datos_drones.append((id_dron, coordenada_x, coordenada_y))
                        elif len(parts) == 1: 
                            nombre_figura = parts[0]
                            print("Nombre de la figura: ", nombre_figura)

        except FileNotFoundError: 
            print(f"El fichero {file_path} no se encuentra")
        except Exception as e: 
            print(f"Error al leer los datos de los drones: {str(e)}")
            
    def recibir_mensaje(self, figura, drone_map):
        ADDR = (SERVER, self.puerto_escucha)
        self.server.bind(ADDR)
            
        self.server.listen()
        print(f"[LISTENING] Servidor a la escucha en {SERVER}, {self.puerto_escucha}")
        CONEX_ACTIVAS = threading.active_count() - 2
        print("Hay las siguientes conexiones activas: ", CONEX_ACTIVAS)
        
        while not self.stop_flag:
            conn, addr = self.server.accept()
            CONEX_ACTIVAS = threading.active_count() - 1
            if CONEX_ACTIVAS <= self.max_drones: 
                thread = threading.Thread(target=self.handle_client, args=(conn, addr, figura, drone_map))
                thread.start()
                print(f"[CONEXIONES ACTIVAS] {CONEX_ACTIVAS}")
                print("CONEXIONES RESTANTES PARA CERRAR EL SERVICIO", self.max_drones - CONEX_ACTIVAS)
            else:
                print("OOppsss... DEMASIADAS CONEXIONES. ESPERANDO A QUE ALGUIEN SE VAYA")
                conn.send("OOppsss... DEMASIADAS CONEXIONES. Tendrás que esperar a que alguien se vaya".encode(FORMAT))
                conn.close()
    
    def readDrones(self):
        file_path = "../Drones.txt"
        self.figures = []
        self.get_drones_from_file(file_path)
        print(self.figures)
        maxdrones = 0
        
        for figura in self.figures:
            if len(figura[1]) > maxdrones:
                self.max_drones = len(figura[1])
             
    def start(self, drone_map):
        file_path = "../Drones.txt"
        self.figures = []
        self.get_drones_from_file(file_path)
        print(self.figures)
        
        for figura in self.figures:
            if len(figura[1]) > self.max_drones:
                self.maxdrones = len(figura[1])
                
        self.recibir_mensaje(self.figures, drone_map)
        
    def calculate_lrc(self, data):
        lrc = 0
        for i in range(len(data)):
            lrc = lrc ^ ord(data[i])
        return lrc
    
    def readDrone(self, conn):
        msg = conn.recv(2048).decode(FORMAT)
        print("Mensaje recibido del servidor de drone:", msg)
        return msg

    def readRegistry(self, conn):
        msg = conn.recv(2048).decode(FORMAT)
        print("Mensaje recibido del servidor de registro:", msg)
        return msg
    
    def sendRegistry(self, conn, msg):
        conn.send(msg.encode(FORMAT))
        print("Mensaje enviado al servidor de registro: ", msg)
    
    def sendDrone(self, conn, msg):
        conn.send(msg.encode(FORMAT))
        print("Mensaje enviado al servidor de drone: ", msg)
    
    def comprobarLRC(self, msg):
        lrc = 0
        for i in range(len(msg)):
            lrc = lrc ^ ord(msg[i])
            
        print ("MSG: ", msg)
        print ("LRC: ", lrc)
        return lrc
    
    def delivery_report(self, err, msg):
        if err is not None:
            print(f'Error al enviar mensaje: {err}')
        else:
            print(f'Mensaje enviado a {msg.topic()} [{msg.partition()}]')
    
    def kafkaProducer(self, msg, id):
        conf = {'bootstrap.servers': self.ip_kafka + ":" + str(self.puerto_kafka)}
        producer = Producer(conf)
        
        msg = str(msg)
        
        if msg:
            topic = "drones"
            key = None
            msg_bytes = msg.encode('utf-8')
            producer.produce(topic, key = key, value = msg_bytes, callback=self.delivery_report, partition = id)
            
            print("Mensaje enviado al topic: ", topic, " con el mensaje: ", msg, " y la partición: ", id)
            producer.flush()
            
    def kafkaConsumer(self, id):
        conf = {'bootstrap.servers': self.ip_kafka + ":" + str(self.puerto_kafka),
                'group.id': "my-group",
                'auto.offset.reset': 'earliest'}
        
        consumer = Consumer(conf)
        
        tp = TopicPartition("movements", int(id))
        print(f"Asignado a la partición {int(id)} del topic movements")
        consumer.assign([tp])
        mensaje = ""
        
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                print("La partición está vacía")
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            if msg.value() is not None:
                msg = msg.value().decode('utf-8')
                print(f'Received message: {msg}')
                consumer.commit()
                break
            
        consumer.close()
        
        return msg
        
    def getPos(self, msg):
        print("Mensaje recibido: ", msg)
        msg = str(msg)
        
        if "'" in msg:
            msg = msg.strip("()").split(",")
            msg[0] = msg[0].strip().strip("'")
            msg[1] = msg[1].strip().strip("'")
        else:
            msg = msg.strip("()").split(", ")
            
        x = int(msg[0])
        y = int(msg[1])
        
        print("Posición x: ", x)
        print("Posición y: ", y)
        
        return x, y

    def moveDrones(self, conn, figura, pos, posDest, id, last, drone_map):
        self.kafkaProducer(posDest, id)
        self.kafkaProducer(pos, id)
        print("Posición actual del dron: ", pos)
        print("Posición destino del dron: ", posDest)
        
        if drone_map.dronesmap.get(id) is None:
            drone_map.add_drone(id)
            
        posDest = self.getPos(posDest)
        pos = self.getPos(pos)
        print("Posición actual del dron: ", pos)
        print("Posición destino del dron: ", posDest)
        time.sleep(1)
        
        while pos != posDest:
            pos = self.kafkaConsumer(id)
            pos = self.getPos(pos)
            print("Posición actual del dron: ", pos)
            print("Posición destino del dron: ", posDest)
            drone_map.update_drone_position(pos[0], pos[1], id)
            self.kafkaProducer(pos, id)
            time.sleep(1)
            
        drone_map.positionReached(id)
        self.handleArrival(conn)
        return pos

    def handleArrival(self, conn):
        print("Esperando a que todos los drones lleguen a su destino...")
        while (msg := self.readDrone(conn)) == "":
            pass
        if msg == "ARRIVAL":
            pass
        self.barrier.wait()
        print("Todos los drones han llegado a su destino. Procediendo con la tarea...")
        time.sleep(5)
        self.sendDrone(conn, self.package_message("CONTINUAR"))

    def manejoDrone(self, conn, figuras, id, drone_map):
        last = False
        pos = ('-1', '-1')
        
        for figura in figuras:
            datos_drone = figura[1]
            if figura == figuras[-1]:
                last = True
            
            if id > self.max_drones or id > len(datos_drone):
                self.sendDrone(conn, self.package_message("No participas en la figura"))
                while (msg := self.readDrone(conn)) == "":
                    pass
                
                if msg == "OK":
                    posDest = (-1, -1)
                    pos = self.moveDrones(conn, figura, pos, posDest, id, last, drone_map)
                else:
                    print("El dron no ha recibido el mensaje de no participas")
                    break
            else:
                self.sendDrone(conn, self.package_message("Nos movemos"))
                
                while (msg := self.readDrone(conn)) == "":
                    pass
                
                if msg == "OK":
                    print("Soy el id ", id)
                    posDest = (datos_drone[id-1][1], datos_drone[id-1][2])
                    print("Posición destino: ", posDest)
                    pos = self.moveDrones(conn, figura, pos, posDest, id, last, drone_map)
                else:
                    print("El dron no ha recibido el mensaje de nos movemos")
                    break
                
        self.sendDrone(conn, self.package_message("Nos movemos"))
        while (msg := self.readDrone(conn)) == "":
            pass
        
        if msg == "OK":
            print("Soy el id ", id)
            posDest = ('-1', '-1')
            pos = self.moveDrones(conn, figura, pos, posDest, id, last, drone_map)
            
        self.sendDrone(conn, self.package_message("FIN"))
        while (msg := self.readDrone(conn)) == "":
            pass
        
        if msg == "OK":
            return last
                
    def comunicacion_drone(self, figuras, conn, addr):
        while (msg := self.readDrone(conn)) == "":
            pass
        
        if msg == "<SOLICITUD>":
            self.sendDrone(conn, "OK")
            
            while (msg := self.readDrone(conn)) == "":
                pass
            
            msg = msg.split(ETX)
            lrc = msg[1]
            msg = msg[0].split(STX)
            msg = msg[1]
            
            if msg == FIN:
                return FIN
            else:
                if self.comprobarLRC(msg) == ord(lrc):
                    print("LRC correcto")
                    token = msg.split(".")[1]
                    id = self.database(token)
                    self.sendDrone(conn, "OK")
                    return id
                else:
                    print("LRC incorrecto")
                    self.sendDrone(conn, "DENIED")
                    return "ERROR"
        else:
            self.sendDrone(conn, "DENIED")
            return "ERROR"
            
    def handle_client(self, conn, addr, figuras, drone_map):
        print(f"[NUEVA CONEXION] {addr} conectado.")
        last = False
        connected = True
        self.drone_connections.append(conn)  # Add the connection to the list
        
        while connected:
            id = self.comunicacion_drone(figuras, conn, addr)
            
            if id == "ERROR":
                connected = False
                break
            elif last:
                connected = False
                break
            else:
                last = self.manejoDrone(conn, figuras, id, drone_map)
                connected = False
        
        print(f"[CONEXION CERRADA] {addr} desconectado.")  
        self.drone_connections.remove(conn)   
        conn.close()
        
    def stop(self):
        self.stop_event.set()
        self.server.close()
        print("Servidor cerrado")
        for id in range(1, self.max_drones + 1):
            self.kafkaProducer("STOP", id)
        
    def broadcast_message(self, message):
        for conn in self.drone_connections:
            self.sendDrone(conn, self.package_message(message))
        
    def listen_for_stop(self):
        while True:
            command = input("Introduce 'S' para detener el Engine: ")
            if command.strip().upper() == "S":
                self.stop()
                break
    
    def main(self):
        self.stop_flag = False
        self.drone_connections = []
        self.procesar_argumentos()
        drone_map = self.DroneMap()
        self.readDrones()
        print ("Max drones: ", self.max_drones)
        self.barrier = threading.Barrier(self.max_drones)  # Inicializa la barrera
        
        server_thread = threading.Thread(target=self.start, args=(drone_map,))
        server_thread.start()
        
        stop_thread = threading.Thread(target=self.listen_for_stop)
        stop_thread.start()
        
        
        drone_map.mainloop()
        
AD_Engine().main()
