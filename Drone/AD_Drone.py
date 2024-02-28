import socket
import sys
import time

HEADER = 64
PORT = 5050
FORMAT = 'utf-8'
FIN = "FIN"

STX = chr(0x02)
ETX = chr(0x03)

client_registry = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_engine = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

class AD_Drone:
    
    def comprobarLRC(msg):
        lrc = 0
        for i in range(len(msg)):
            lrc = lrc ^ ord(msg[i])
            
        print ("MSG: ", msg)
        print ("LRC: ", lrc)
        return lrc
    
    def procesarArgumentos():
        if (len(sys.argv) == 7):
            Server_Registry  = sys.argv[1]
            Port_Registry = int(sys.argv[2])
            ADDR_Registry = (Server_Registry, Port_Registry)
            Server_Engine = sys.argv[3]
            Port_Engine = int(sys.argv[4])
            ADDR_Engine = (Server_Engine, Port_Engine)
            Server_Kafka = sys.argv[5]
            Port_Kafka = int(sys.argv[6])
        
        else:
            print ("ERROR: Debes incluir la dirección y puerto del servidor de registro, el servidor de Engine y el servidor de Kafka")
            sys.exit()
        
        return ADDR_Registry, ADDR_Engine, Server_Kafka, Port_Kafka
    
    def calculate_lrc(data):
        lrc = 0
        for i in range(len(data)):
            lrc = lrc ^ ord(data[i])
        return lrc
    
    def package_message(self, data):
        lrc = self.calculate_lrc(data)
        print("LRC: ", lrc)
        return STX + data + ETX + chr(lrc)
    
    def sendRegister(msg):
        client_registry.send(msg.encode(FORMAT))
        print("Mensaje enviado al servidor de registro: ", msg)
        
    def readRegister():
        msg = client_registry.recv(2048).decode(FORMAT)
        print("Mensaje recibido del servidor de registro:", msg)
        return msg
        
    def sendEngine(msg):
        client_engine.send(msg.encode(FORMAT))
        print("Mensaje enviado al servidor de Engine: ", msg)
        
    def readEngine():
        msg = client_engine.recv(2048).decode(FORMAT)
        print("Mensaje recibido del servidor de Engine", msg)
        return msg
    
    def registrarDrone(self):
        
        print("Solicitud de inicio de comunicación")
        sol = "<SOLICITUD>"
        self.sendRegister(sol)
        
        while ((msg := self.readRegister()) == ""):
            pass
        
        if (msg == "OK"):

            alias = input("Introduce el alias del drone: ")
            print("Envio al servidor el alias: ", alias)
            
            if (alias == "FIN"):
                msg = self.package_message(self, FIN)
                self.sendRegister(msg)
                sys.exit()
            else: 
                msg = "1." + alias
                msg = self.package_message(self, msg)
                self.sendRegister(msg)
            
            while ((msg := self.readRegister()) == ""):
                pass
            
            if (msg == "OK"):
                
                while ((msg := self.readRegister()) == ""):
                    pass
                
                self.token = msg
                
                print("Token recibido: ", self.token)
                self.token = self.token.split(ETX)
                lrc = self.token[1]
                self.token = self.token[0].split(STX)
                self.token = self.token[1]
                
                if (self.comprobarLRC(self.token) == ord(lrc)):
                    print("LRC correcto")
                    self.sendRegister("OK")   
                    self.sendRegister("EOT")  
                else:
                    print("LRC incorrecto")
                    self.sendRegister("DENIED")
                    sys.exit()
            
            elif (msg == "DENIED"):
                print("El servidor de registro no ha recibido correctamente el mensaje")
                sys.exit()
                
        elif (msg == "DENIED"):
            print("El servidor de registro ha denegado la solicitud de inicio de comunicación")
            sys.exit()
                
    def unirse_al_espectaculo(self):
        
        msg = "<SOLICITUD>"
        self.sendEngine(msg)
        
        while ((msg := self.readEngine()) == ""):
            pass
        
        if (msg == "OK"):
            msg = self.id + "." + self.token
            msg = self.package_message(self, msg)
            self.sendEngine(msg)
            
            while ((msg := self.readEngine()) == ""):
                pass
            
            if (msg == "OK"):
                #Nos quedamos a la espera de órdenes
                while ((msg := self.readEngine()) == ""):
                    pass
                
                msg = msg.split(ETX)
                lrc = msg[1]
                msg = msg[0].split(STX)
                msg = msg[1]
                
                
                
                if (self.comprobarLRC(lrc) and msg == "Nos movemos"):
                    print("Nos movemos")
                    #Aquí iría el código para mover el drone
                    self.sendEngine("OK")
                    #Aquí comenzaremos con el consumo y produccion en kafka.
                    
                elif(msg == "No participas en la figura"):
                    self.sendEngine("OK")
                    #Comenzaremos con al comunicación kafka para
                    #volver a la base o no movernos si ya estamos en la base.
                    

        elif (msg == "DENIED"):
            print("El servidor de Engine ha denegado la solicitud de inicio de comunicación")
            sys.exit()
            
            
        
    
    def main(self):
        
        ADDR_Registry, ADDR_Engine, Server_Kafka, Port_Kafka = self.procesarArgumentos()
        
        client_registry.connect(ADDR_Registry)
        
    
        print("Conexión establecida con el servidor de registro")
        print("Conexión establecida con el servidor de Engine")
        
        self.registrarDrone(self)
        self.id = self.token.split(".")[0]
        self.token = self.token.split(".")[1]
        print("ID: ", self.id)
        print("Token: ", self.token)
        
        
        client_engine.connect(ADDR_Engine)
        self.unirse_al_espectaculo(self)
        
        
        client_registry.close()
        client_engine.close()
        
AD_Drone.main(AD_Drone)