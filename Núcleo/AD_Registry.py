import socket 
import threading
import secrets
import mysql.connector
import string
import sys
import time


HEADER = 64
SERVER = socket.gethostbyname(socket.gethostname())

STX = chr(0x02)
ETX = chr(0x03)
FORMAT = 'utf-8'
FIN = "FIN"
MAX_CONEXIONES = 10100

class AD_Registry:
    
    alias = "" ## puede que se tenga que eliminar (gestiona varios drones simultaneamente y esto lia)
    Server_BD = "localhost"
    Port_BD = 3306
    myid = 0 #lo mismo que con alias
    
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    def calculate_lrc(data):
        lrc = 0
        for i in range(len(data)):
            lrc = lrc ^ ord(data[i])
        return lrc
    
    def package_message(self, data):
        lrc = self.calculate_lrc(data)
        print("LRC: ", lrc)
        return STX + data + ETX + chr(lrc)
    
    def registrarDrone(self, conexion, conn_drone, alias):
        token = self.generar_token()
        
        cursor = conexion.cursor()

        create_table = "CREATE TABLE IF NOT EXISTS drones (id INT AUTO_INCREMENT PRIMARY KEY, alias VARCHAR(255), token VARCHAR(255))"
        cursor.execute(create_table)
        conexion.commit()
        
        insert_data = "INSERT INTO drones (alias, token) VALUES ('%s', '%s')" % (alias, token)
        cursor.execute(insert_data)
        conexion.commit()
        
        myid = "SELECT id FROM drones WHERE token = '%s'" % (token)
        cursor.execute(myid)
        id = cursor.fetchone()[0]
        
        msg = str(id) + "." + token
        msg = self.package_message(self, msg)
        self.sendDrone(self, conn_drone, msg)
        print("Drone registrado correctamente")
        print("ID: ", id)
        print("Token: ", token)
        print("Alias: ", alias)
        
    def modificarAlias(self, conexion, conn_drone, id, nuevoAlias):
        cursor = conexion.cursor()
        
        update_alias = "UPDATE drones SET alias = '%s' WHERE id = '%s'" % (nuevoAlias, id)
        cursor.execute(update_alias)
        conexion.commit()
        
        msg = "OK"
        conn_drone.send(msg.encode(FORMAT))
        print("Alias cambiado correctamente a ", nuevoAlias, " con id ", id)
        
    def borrarDrone(self, conexion, conn_drone, id):
        cursor = conexion.cursor()
        
        delete_drone = "DELETE FROM drones WHERE id = '%s'" % (id)
        cursor.execute(delete_drone)
        conexion.commit()
        
        msg = "OK"
        conn_drone.send(msg.encode(FORMAT))
        print("Drone borrado correctamente")
        
    def dataBase(self, opcion, conn_drone): ##ahora opcion antes token
            
            conexion = mysql.connector.connect(
                host=self.Server_BD,
                port=self.Port_BD,
                user="root",
                password="1573",
                database="test"
            )

            if opcion.split(".")[0] == "1": #Registrar drone
                alias = opcion.split(".")[1]
                self.registrarDrone(self, conexion, conn_drone, alias)
            elif opcion.split(".")[0] == "2": #Modificar alias
                id = opcion.split(".")[1]
                nuevoAlias = opcion.split(".")[2]
                self.modificarAlias(self, conexion, conn_drone, id, nuevoAlias)
            elif opcion.split(".")[0] == "3": #Darse de baja
                id = opcion.split(".")[1]
                self.borrarDrone(self, conexion, conn_drone, id)  
                   

    def generar_token(length=32):
        caracteres = string.ascii_letters + string.digits
        token = ''.join(secrets.choice(caracteres) for i in range(length))
        return token
    
    def comprobarLRC(msg):
        lrc = 0
        for i in range(len(msg)):
            lrc = lrc ^ ord(msg[i])
            
        print ("MSG: ", msg)
        print ("LRC: ", lrc)
        return lrc

    def sendDrone(self, conn, msg):
        conn.send(msg.encode(FORMAT))
        print("Mensaje enviado al drone", msg)
        
    def readDrone(self, conn):
        msg = conn.recv(2048).decode(FORMAT)
        print("Mensaje recibido del drone", msg)
        return msg
    
        
    def recibirMensaje(self, conn):
        
        while ((msg := self.readDrone(self, conn)) == ""):
            pass
                
        if (msg == "<SOLICITUD>"):
            self.sendDrone(self, conn, "OK")
            
            while ((msg := self.readDrone(self, conn)) == ""):
                pass
            
            msg = msg.split(ETX)
            lrc = msg[1]
            msg = msg[0].split(STX)
            msg = msg[1]
            
            if (msg == FIN):
                return FIN
            else:
                #comprobar LRC, el mensaje es correcto si el LRC es igual al que se ha calculado
                if (self.comprobarLRC(msg) == ord(lrc)):
                    print("LRC correcto")
                    self.sendDrone(self, conn, "OK")
                    return msg
                else:
                    print("LRC incorrecto")
                    self.sendDrone(self, conn, "DENIED")
                    return "ERROR"
        else:
            self.sendDrone(self, conn, "DENIED")
                    
            
            
    def handle_client(self, conn, addr):
        print(f"[NUEVA CONEXION] {addr} connected.")

        connected = True
        while connected:
           
            resp = self.recibirMensaje(self, conn)
            print("Respuesta: ", resp)
            if (resp == FIN):
                connected = False
                break
            
            opcion = resp.split(".")[0]
        
            if (opcion == "1" or opcion == "2" or opcion == "3"):
                self.dataBase(self, resp, conn) 
                
                while ((msg := self.readDrone(self, conn)) == ""):
                    pass
                
                if (msg == "OK"):
                    
                    while ((msg := self.readDrone(self, conn)) == ""):
                        pass
                    
                    if (msg == "EOT"):
                        connected = False #Esto no se si hace falta pero es para salir del bucle por ahora.
                        break
                
                elif (msg == "DENIED"):
                    print("El servidor de registro no ha recibido correctamente el mensaje")
                    connected = False
                    break
                
        print("ADIOS.")
        conn.close()
        
            
    def main(self):
        
        if (len(sys.argv) == 4):
            PORT = int(sys.argv[1])
            Server_BD = sys.argv[2]
            Port_BD = int(sys.argv[3])
            
            
            ADDR = (SERVER, PORT)
            
            self.server.bind(ADDR)
            
            self.server.listen()
            print(f"[LISTENING] Servidor a la escucha en {SERVER}")
            CONEX_ACTIVAS = threading.active_count()-1
            print("Conexiones activas: ", CONEX_ACTIVAS)
            while True:
                conn, addr = self.server.accept()
                CONEX_ACTIVAS = threading.active_count()
                if (CONEX_ACTIVAS <= MAX_CONEXIONES): 
                    thread = threading.Thread(target=self.handle_client, args=(self, conn, addr))
                    thread.start()
                    print(f"[CONEXIONES ACTIVAS] {CONEX_ACTIVAS}")
                    print("CONEXIONES RESTANTES PARA CERRAR EL SERVICIO", MAX_CONEXIONES-CONEX_ACTIVAS)
                    
                else:
                    print("OOppsss... DEMASIADAS CONEXIONES. ESPERANDO A QUE ALGUIEN SE VAYA")
                    conn.send("OOppsss... DEMASIADAS CONEXIONES. Tendrás que esperar a que alguien se vaya".encode(FORMAT))
                    conn.close()
                    CONEX_ACTUALES = threading.active_count()-1
        else: 
            print("ERROR: Debes incluir el puerto del servidor de Registry y la dirección IP y el puerto del servidor de BD")
                    
AD_Registry.main(AD_Registry)
    

    