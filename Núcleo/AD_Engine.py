import sys
import socket
import threading
import mysql.connector
import time

HEADER = 64
FORMAT = 'utf-8'
SERVER = socket.gethostbyname(socket.gethostname())
FIN = "FIN"

STX = chr(0x02)
ETX = chr(0x03)
class AD_Engine:
    
    max_drones = 0
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
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
        
        if (exists == 1):
            myid = "Select id from drones where token = '%s'" %(token)
            cursor.execute(myid)
            id = cursor.fetchone()[0]
        
        return id
    
    def get_drones_from_file(self, file_path):
        
        datos_drones = [] # Lista de listas con los datos de los drones
        nombre_figura = None # Nombre de la figura a realizar
        figura = [] # Tupla con los datos de la figura (nombre, lista de drones)
        
        try: 
            with open(file_path, 'r') as file: 
                lines = file.readlines()
                                            
                for line in lines: 
                    if line.strip().startswith('</'):
                        if len(datos_drones) == 0: 
                            print("ERROR: No se han leído los datos de los drones")
                            sys.exit(1)
                        elif len(datos_drones) > self.max_drones:
                            print("ERROR: El número de drones a registrar supera el máximio permitido")
                            sys.exit(1)
                        else: 
                            figura = [nombre_figura, datos_drones]
                            self.figures.append(figura)
                            nombre_figura = None
                            datos_drones = []   
                    elif line.strip().startswith('<'):
                        parts = line.strip("<>\n").split('><')
                        if len(parts) == 3: 
                            id_dron, coordenada_x, cordenada_y = parts
                            datos_drones.append((id_dron, coordenada_x, cordenada_y))
                        elif len(parts) == 1: 
                            nombre_figura = parts[0]
                            print("Nombre de la figura: ", nombre_figura)

        except FileNotFoundError: 
            print(f"El fichero {file_path} no se encuentra")
        except Exception as e: 
            print(f"Error al leer los datos de los drones: {str(e)}")
            
    
    def recibir_mensaje(self, figura):
        
        ADDR = (SERVER, self.puerto_escucha)
            
        self.server.bind(ADDR)
        mapa=0
            
        self.server.listen()
        print(f"[LISTENING] Servidor a la escucha en {SERVER},  {self.puerto_escucha}")
        CONEX_ACTIVAS = threading.active_count()-2
        print("Hay las siguientes conexiones activas: ", CONEX_ACTIVAS)
        while True:
            conn, addr = self.server.accept()
            CONEX_ACTIVAS = threading.active_count()
            if (CONEX_ACTIVAS <= self.max_drones): 
                thread = threading.Thread(target=self.handle_client, args=(conn, addr, figura))
                thread.start()
                print(f"[CONEXIONES ACTIVAS] {CONEX_ACTIVAS}")
                print("CONEXIONES RESTANTES PARA CERRAR EL SERVICIO", self.max_drones-CONEX_ACTIVAS)
                 
            else:
                print("OOppsss... DEMASIADAS CONEXIONES. ESPERANDO A QUE ALGUIEN SE VAYA")
                conn.send("OOppsss... DEMASIADAS CONEXIONES. Tendrás que esperar a que alguien se vaya".encode(FORMAT))
                conn.close()
                CONEX_ACTUALES = threading.active_count()-1
                
    def start(self):
        file_path = "../Drones.txt"
        self.figures = []
        self.get_drones_from_file(file_path)
        print(self.figures)
        
        for figura in self.figures:
            if(len(figura[1]) > self.max_drones):
                self.maxdrones = len(figura[1])
                
        self.recibir_mensaje(self.figures)
        
    def readDrone(self,conn):
        msg = conn.recv(2048).decode(FORMAT)
        print("Mensaje recibido del servidor de registro:", msg)
        return msg

    def readRegistry(self,conn):
        msg = conn.recv(2048).decode(FORMAT)
        print("Mensaje recibido del servidor de registro:", msg)
        return msg
    
    def sendRegistry(self,conn, msg):
        conn.send(msg.encode(FORMAT))
        print("Mensaje enviado al servidor de registro: ", msg)
    
    def sendDrone(self, conn, msg):
        conn.send(msg.encode(FORMAT))
        print("Mensaje enviado al servidor de registro: ", msg)
    
    def comprobarLRC(self, msg):
        lrc = 0
        for i in range(len(msg)):
            lrc = lrc ^ ord(msg[i])
            
        print ("MSG: ", msg)
        print ("LRC: ", lrc)
        return lrc
    
    def moveDrones(self, figura, id, last):
        pass
    
    def manejoDrone(self, conn, figuras, id):
        
        
        
        for figura in figuras:
            if figura == figuras[-1]:
                last = True
            
            if (id>self.maxdrones):
                self.sendDrone(conn, self.package_message("No participas en la figura"))
            else:
                self.sendDrone(conn, self.package_message("Nos movemos"))
                self.moveDrones(figura, id, last)
                

                
                
    def comunicacion_drone(self, figuras, conn, addr):
        
        while((msg := self.readDrone(conn)) == ""):
            pass
        
        if (msg == "<SOLICITUD>"):
            self.sendDrone(conn, "OK")
            
            while((msg := self.readDrone(conn)) == ""):
                pass
            
            msg = msg.split(ETX)
            lrc = msg[1]
            msg = msg[0].split(STX)
            msg = msg[1]
            
            if(msg == FIN):
                return FIN
            else:
                if (self.comprobarLRC(msg) == ord(lrc)):
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
            
            
            
            
    def handle_client(self, conn, addr, figuras):
        
        print(f"[NUEVA CONEXION] {addr} conectado.")
        
        connected = True
        while connected:
            
            id = self.comunicacion_drone(figuras, conn, addr)
            
            if (id == "ERROR"):
                connected = False
                break
            else:
                self.manejoDrone(conn, figuras, id)
                
    
    def main(self):
        
        self.procesar_argumentos()
        self.start()
        
AD_Engine().main()