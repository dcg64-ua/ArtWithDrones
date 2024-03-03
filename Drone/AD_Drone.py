import socket
import sys
import time
import threading
import secrets
import string
from confluent_kafka import Producer, Consumer, KafkaError, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaError

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
    
    def kafkaConsumer(self):
        conf = {'bootstrap.servers': self.Server_Kafka + ":" + str(self.Port_Kafka),
                'group.id': "my-group",
                'auto.offset.reset': 'earliest'}
        
        consumer = Consumer(conf)
        
        tp = TopicPartition("drones", int(self.id))
        print("Partición: drones", int(self.id))
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
                print('Received message: {}'.format(msg))
                consumer.commit()
                break
            
        consumer.close()
        
        return msg
        
    
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
            
    def calculateMovement(self):
        
        target_pos = (self.posDestX, self.posDestY)
        current_pos = (self.posX, self.posY)
        
        x_diff = target_pos[0] - current_pos[0]
        y_diff = target_pos[1] - current_pos[1]
        
        if x_diff > 0 and y_diff > 0:
            current_pos = (current_pos[0] + 1, current_pos[1] + 1)
                
        elif x_diff > 0:
            current_pos = (current_pos[0] + 1, current_pos[1])
            
        elif y_diff > 0:
            current_pos = (current_pos[0], current_pos[1] + 1)
            
        elif x_diff < 0 and y_diff < 0 and current_pos[0] > 0 and current_pos[1] > 0:
            current_pos = (current_pos[0] - 1, current_pos[1] - 1)
            
        elif x_diff < 0 and current_pos[0] > 0:
            current_pos = (current_pos[0] - 1, current_pos[1])
            
        elif y_diff < 0 and current_pos[1] > 0:
            current_pos = (current_pos[0], current_pos[1] - 1)
        else:
            current_pos = (-1,-1)
            self.kafkaProducer(str(current_pos))
            
        
        if current_pos[0] == 0 and current_pos[1] != 0:
            current_pos = (current_pos[0] + 1, current_pos[1])
        elif current_pos[1] == 0 and current_pos[0] != 0:
            current_pos = (current_pos[0], current_pos[1] + 1)
        
        self.kafkaProducer(self, str(current_pos))  
    
    def getPos(self, msg):
        
        msg = msg.split(",")
        msg[0] = msg[0].split("(")[1]
        msg[1] = msg[1].split(")")[0]
        x = int(msg[0].strip("'"))
        y = msg[1].strip("'")
        y = y.split("'")[1]
        x= int(x)
        y = int(y)
        
        return x, y
                
    def moveDrone(self):
        msg = self.kafkaConsumer(self)
        
        x, y = self.getPos(self, msg)
        
        self.posDestX = x
        self.posDestY = y
        
        msg = self.kafkaConsumer(self)
        x, y = self.getPos(self, msg)
        
        self.posX = x
        self.posY = y
        
        self.calculateMovement(self)
        
        
        
    def delivery_report(self, err, msg):
        
        if err is not None:
            print(f'Error al enviar mensaje: {err}')
        else:
            print(f'Mensaje enviado a {msg.topic()} [{msg.partition()}]')
        
    def kafkaProducer(self, msg):
        err = None
        conf = {'bootstrap.servers': self.Server_Kafka + ":" + str(self.Port_Kafka)}
        producer = Producer(conf)
        
        msg = str(msg)
        
        if msg: 
            topic = "drones"
            key = None
            msg_bytes = msg.encode('utf-8')
            producer.produce(topic, key=key, value=msg_bytes, callback=self.delivery_report(self, err, msg), partition = int(self.id))
            producer.flush()
        
    
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
                
                while True:
            
                    while ((msg := self.readEngine()) == ""):
                        pass
                    
                    msg = msg.split(ETX)
                    lrc = msg[1]
                    msg = msg[0].split(STX)
                    msg = msg[1]
                        
                    if (self.comprobarLRC(lrc) and msg == "Nos movemos"):
                        #Aquí iría el código para mover el drone
                        #Recibiré la posicion de destino y la guardaré en una variable self.
                        #Los movimientos los quiero hacer de uno en uno, de modo que, el drone
                        #Envia el movimiento a engine, este realiza el movimiento y me devuelve
                        #Un mensaje de confirmación o algo así, aunque en esto dudo.
                        self.sendEngine("OK")                  
                        self.moveDrone(self)
                            
                            
                    elif(msg == "No participas en la figura"):
                        self.sendEngine("OK")
                        self.moveDrone(self)
                                                
                        #Aquí comenzaremos con el consumo y produccion en kafka.
                        #Comenzaremos con al comunicación kafka para
                        #volver a la base o no movernos si ya estamos en la base.
                        
                    elif(msg == "FIN"):
                        self.sendEngine("OK")
                        sys.exit()
                
            else:
                print("El servidor de Engine no ha recibido correctamente el mensaje")
                sys.exit()

        elif (msg == "DENIED"):
            print("El servidor de Engine ha denegado la solicitud de inicio de comunicación")
            sys.exit()
            
            
        
    
    def main(self):
        
        ADDR_Registry, ADDR_Engine, self.Server_Kafka, self.Port_Kafka = self.procesarArgumentos()
        
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