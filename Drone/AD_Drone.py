import socket
import sys

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
        print("Mensaje enviado al servidor de registro")
        
    def readRegister():
        msg = client_registry.recv(2048).decode(FORMAT)
        print("Mensaje recibido del servidor de registro")
        return msg
        
    def registrarDrone(self):
        alias = "Drone"
        print("Envio al servidor el alias: ", alias)
        msg = "1." + alias
        msg = self.package_message(self, msg)
        self.sendRegister(msg)
        token = self.readRegister()
        
        print("Token recibido: ", token)
        token = token.split(ETX)
        lrc = token[1]
        token = token[0].split(STX)
            
        print("MSG: ", token[1])
        print("LRC: ", ord(lrc))
        
        if (self.comprobarLRC(token[1]) == ord(lrc)):
            print("LRC correcto")   
        else:
            print("LRC incorrecto")
            sys.exit()
        
        
    
    def main(self):
        
        ADDR_Registry, ADDR_Engine, Server_Kafka, Port_Kafka = self.procesarArgumentos()
        
        client_registry.connect(ADDR_Registry)
        #client_engine.connect(ADDR_Engine)
    
        print("Conexión establecida con el servidor de registro")
        print("Conexión establecida con el servidor de Engine")
        
        self.registrarDrone(self)
        
        client_registry.close()
        client_engine.close()
        
AD_Drone.main(AD_Drone)