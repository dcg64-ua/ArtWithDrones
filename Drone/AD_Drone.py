import socket
import sys
import time
import threading
from confluent_kafka import Producer, Consumer, KafkaError, TopicPartition

HEADER = 64
PORT = 5050
FORMAT = 'utf-8'
FIN = "FIN"
STX = chr(0x02)
ETX = chr(0x03)

client_registry = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_engine = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

class AD_Drone:
    def __init__(self, server_kafka, port_kafka, id):
        self.Server_Kafka = server_kafka
        self.Port_Kafka = port_kafka
        self.id = id
        self.posX = 0
        self.posY = 0
        self.posDestX = 0
        self.posDestY = 0

    def comprobarLRC(self, msg):
        lrc = 0
        for i in range(len(msg)):
            lrc = lrc ^ ord(msg[i])
        print("MSG: ", msg)
        print("LRC: ", lrc)
        return lrc

    @staticmethod
    def calculate_lrc(data):
        lrc = 0
        for i in range(len(data)):
            lrc = lrc ^ ord(data[i])
        return lrc

    def package_message(self, data):
        lrc = self.calculate_lrc(data)
        print("LRC: ", lrc)
        return STX + data + ETX + chr(lrc)

    def sendRegister(self, msg):
        client_registry.send(msg.encode(FORMAT))
        print("Mensaje enviado al servidor de registro: ", msg)

    def readRegister(self):
        msg = client_registry.recv(2048).decode(FORMAT)
        print("Mensaje recibido del servidor de registro:", msg)
        return msg

    def sendEngine(self, msg):
        client_engine.send(msg.encode(FORMAT))
        print("Mensaje enviado al servidor de Engine: ", msg)

    def readEngine(self):
        msg = client_engine.recv(2048).decode(FORMAT)
        print("Mensaje recibido del servidor de Engine", msg)
        return msg

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

    def delivery_report(self, err, msg):
        if err is not None:
            print(f'Error al enviar mensaje: {err}')
        else:
            print(f'Mensaje enviado a {msg.topic()} [{msg.partition()}]')

    def kafkaProducer(self, msg):
        conf = {'bootstrap.servers': self.Server_Kafka + ":" + str(self.Port_Kafka)}
        producer = Producer(conf)
        msg = str(msg)
        id = int(self.id)

        if msg:
            topic = "movements"
            key = None
            msg_bytes = msg.encode('utf-8')
            producer.produce(topic, key=key, value=msg_bytes, partition=id, callback=self.delivery_report)
            producer.flush()
            print(f"Mensaje enviado al topic {topic}, partición {id}: {msg}")

    def registrarDrone(self):
        print("Solicitud de inicio de comunicación")
        sol = "<SOLICITUD>"
        self.sendRegister(sol)

        while ((msg := self.readRegister()) == ""):
            pass

        if msg == "OK":
            alias = input("Introduce el alias del drone: ")
            print("Envio al servidor el alias: ", alias)

            if alias == "FIN":
                msg = self.package_message(FIN)
                self.sendRegister(msg)
                sys.exit()
            else:
                msg = "1." + alias
                msg = self.package_message(msg)
                self.sendRegister(msg)

            while ((msg := self.readRegister()) == ""):
                pass

            if msg == "OK":
                while ((msg := self.readRegister()) == ""):
                    pass

                self.token = msg
                print("Token recibido: ", self.token)
                self.token = self.token.split(ETX)
                lrc = self.token[1]
                self.token = self.token[0].split(STX)
                self.token = self.token[1]

                if self.comprobarLRC(self.token) == ord(lrc):
                    print("LRC correcto")
                    self.sendRegister("OK")
                    self.sendRegister("EOT")
                else:
                    print("LRC incorrecto")
                    self.sendRegister("DENIED")
                    sys.exit()

            elif msg == "DENIED":
                print("El servidor de registro no ha recibido correctamente el mensaje")
                sys.exit()

        elif msg == "DENIED":
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
            current_pos = (-1, -1)

        if current_pos[0] == 0 and current_pos[1] != 0:
            current_pos = (current_pos[0] + 1, current_pos[1])
        elif current_pos[1] == 0 and current_pos[0] != 0:
            current_pos = (current_pos[0], current_pos[1] + 1)

        print("Mi id: ", self.id)
        self.kafkaProducer(str(current_pos))
        self.posX = current_pos[0]
        self.posY = current_pos[1]

    def getPos(self, msg):
        print("Mensaje recibido: ", msg)
        if "'" in msg:
            msg = msg.split(",")
            msg[0] = msg[0].split("(")[1]
            msg[1] = msg[1].split(")")[0]
            x = int(msg[0].strip("'"))
            y = msg[1].strip("'")
            y = y.split("'")[1]

            x = int(x)
            y = int(y)
        else:
            msg = msg.split(",")
            msg[0] = msg[0].split("(")[1]
            msg[1] = msg[1].split(")")[0]
            msg[1] = msg[1].split(" ")[1]
            x = int(msg[0])
            y = int(msg[1])

        return x, y

    def moveDrone(self):
        msg = self.kafkaConsumer()
        if msg == "STOP":
            self.handleStop()
            return

        x, y = self.getPos(msg)
        self.posDestX = x
        self.posDestY = y

        msg = self.kafkaConsumer()
        if msg == "STOP":
            self.handleStop()
            return

        x, y = self.getPos(msg)
        self.posX = x
        self.posY = y

        while self.posX != self.posDestX or self.posY != self.posDestY:
            self.calculateMovement()
            time.sleep(1)
            self.kafkaConsumer()
            
            if msg == "STOP":
                self.handleStop()
                return

        print("El drone ha llegado a su destino")
        self.handleArrival()

    def handleArrival(self):
        # Notificar al servidor de Engine que el dron ha llegado a su destino
        self.sendEngine("ARRIVAL")
        
        # Esperar mensaje de CONTINUAR desde el servidor de Engine
        print(f"Dron {self.id} esperando mensaje de CONTINUAR...")
        while True:
            msg = self.readEngine()
            if msg == self.package_message("CONTINUAR"):
                print(f"Dron {self.id} recibió mensaje de CONTINUAR")
                break
            elif msg == "STOP":
                self.handleStop()
                return
            time.sleep(1)

        # Actualizar el estado del dron
        print(f"Dron {self.id} ha llegado a ({self.posX}, {self.posY}). Esperando nuevas órdenes...")

    def handleStop(self):
        print(f"Dron {self.id} ha recibido la orden de STOP. Regresando a la base.")
        self.moveToBase()
        sys.exit()

    def moveToBase(self):
        # Implementa la lógica para mover el dron a la base
        self.posDestX, self.posDestY = 0, 0  # Asumiendo que la base está en (0, 0)
        while self.posX != self.posDestX or self.posY != self.posDestY:
            self.calculateMovement()
            time.sleep(1)
            self.kafkaProducer(str((self.posX, self.posY)))
        print(f"Dron {self.id} ha llegado a la base.")

    def unirse_al_espectaculo(self):
        msg = "<SOLICITUD>"
        self.sendEngine(msg)

        while (msg := self.readEngine()) == "":
            pass

        if msg == "OK":
            msg = self.id + "." + self.token
            msg = self.package_message(msg)
            self.sendEngine(msg)

            while (msg := self.readEngine()) == "":
                pass

            if msg == "OK":
                while True:
                    while (msg := self.readEngine()) == "":
                        pass

                    msg = msg.split(ETX)
                    lrc = msg[1]
                    msg = msg[0].split(STX)
                    msg = msg[1]

                    if self.comprobarLRC(msg):
                        if msg == "Nos movemos":
                            self.sendEngine("OK")
                            self.moveDrone()
                        elif msg == "No participas en la figura":
                            self.sendEngine("OK")
                            self.moveDrone()
                        elif msg == "FIN":
                            self.sendEngine("OK")
                            sys.exit()
                        elif msg == "STOP":
                            self.handleStop()
                            break
            else:
                print("El servidor de Engine no ha recibido correctamente el mensaje")
                sys.exit()

        elif msg == "DENIED":
            print("El servidor de Engine ha denegado la solicitud de inicio de comunicación")
            sys.exit()


    @staticmethod
    def procesarArgumentos():
        if len(sys.argv) == 7:
            Server_Registry = sys.argv[1]
            Port_Registry = int(sys.argv[2])
            ADDR_Registry = (Server_Registry, Port_Registry)
            Server_Engine = sys.argv[3]
            Port_Engine = int(sys.argv[4])
            ADDR_Engine = (Server_Engine, Port_Engine)
            Server_Kafka = sys.argv[5]
            Port_Kafka = int(sys.argv[6])
        else:
            print("ERROR: Debes incluir la dirección y puerto del servidor de registro, el servidor de Engine y el servidor de Kafka")
            sys.exit()

        return ADDR_Registry, ADDR_Engine, Server_Kafka, Port_Kafka

    @staticmethod
    def main():
        ADDR_Registry, ADDR_Engine, Server_Kafka, Port_Kafka = AD_Drone.procesarArgumentos()
        
        drone = AD_Drone(Server_Kafka, Port_Kafka, 1)
        client_registry.connect(ADDR_Registry)
        client_engine.connect(ADDR_Engine)

        print("Conexión establecida con el servidor de registro")
        print("Conexión establecida con el servidor de Engine")

        drone.registrarDrone()
        drone.id = drone.token.split(".")[0]
        drone.token = drone.token.split(".")[1]
        print("ID: ", drone.id)
        print("Token: ", drone.token)

        drone.unirse_al_espectaculo()

        client_registry.close()
        client_engine.close()

if __name__ == "__main__":
    AD_Drone.main()
