import sys
import socket
import threading
import mysql.connector

HEADER = 64
FORMAT = 'utf-8'
SERVER = socket.gethostbyname(socket.gethostname())
FIN = "FIN"

class AD_Engine:
    
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
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
            
    def recibir_mensaje(self, figura, drone_map):
        
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
                thread = threading.Thread(target=self.handle_client, args=(conn, addr, figura, drone_map))
                thread.start()
                print(f"[CONEXIONES ACTIVAS] {CONEX_ACTIVAS}")
                print("CONEXIONES RESTANTES PARA CERRAR EL SERVICIO", self.max_drones-CONEX_ACTIVAS)
                 
            else:
                print("OOppsss... DEMASIADAS CONEXIONES. ESPERANDO A QUE ALGUIEN SE VAYA")
                conn.send("OOppsss... DEMASIADAS CONEXIONES. Tendrás que esperar a que alguien se vaya".encode(FORMAT))
                conn.close()
                CONEX_ACTUALES = threading.active_count()-1

    def handle_client(self, conn, addr, figura, drone_map):
        
        print(f"[NUEVA CONEXION] {addr} conectado.")
        connected = True
        while connected:
            msg_length = conn.recv(HEADER).decode(FORMAT)
            if msg_length:
                msg_length = int(msg_length)
                msg = conn.recv(msg_length).decode(FORMAT)
                
                if msg == FIN:
                    connected = False
                    print("Cerrando conexión")
                    conn.close()
                else:
                    print(f"[{addr}] {msg}")
                    conn.send("Mensaje recibido".encode(FORMAT))
                    drone_map[msg] = addr[1]
                    print("MAPA DE DRONES: ", drone_map)
                    print("FIGURA: ", figura)
                    if len(drone_map) == len(figura[1]):
                        self.enviar_mensaje(drone_map, figura)
                        drone_map.clear()
                        print("MAPA DE DRONES: ", drone_map)
                        print("FIGURA: ", figura)