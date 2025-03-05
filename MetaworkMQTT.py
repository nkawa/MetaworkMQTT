# メタワーク用の MQTT manager

# internal instruction
# https://uclab.esa.io/posts/8825 (privavte)


import paho.mqtt.client as mqtt
import json
import time

class MetaworkMQTT:
    def __init__(self, host, port): #, username, password):
        self.host = host
        self.port = port
#        self.username = username
#        self.password = password
        self.client = mqtt.Client()
#        self.client.username_pw_set(username, password)
        self.client.connect(host, port, 60)
        
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
       
        self.devices = []
        
        self.client.loop_start()
        
    def on_connect(self, client, userdata, flags, rc):
        print("Connected with result code "+str(rc))
        client.subscribe("mgr/register")
        client.subscribe("mgr/request")

    def on_message(self, client, userdata, msg):
#        print(msg.topic+" "+str(msg.payload))
        if msg.topic == "mgr/register":
            self.register(msg)
        elif msg.topic == "mgr/request":    
            self.request(msg)
            
    def register(self, msg):
        print("register")
        data = json.loads(msg.payload)
        # 同じIDのデバイスがあるかを確認
        for d in self.devices:
            if d["devId"] == data["devId"]:
                self.devices.remove(d) # あれば、そのデータを消したうえで
                break
        self.devices.append({
            "type": data["codeType"],
            "devId": data["devId"],
            "devType": data["devType"],
            "date": data["date" ]
        })
        print(data)
        
#        print("register")
#        self.client.publish("mgr/register", json.dumps(data))

#   希望するタイプのデバイスがあるかを確認
    def request(self, msg):
        data = json.loads(msg.payload)
        for d in self.devices:
            if d["devType"] == "robot" and d["type"] == data["type"]:
                print("found") # 本当は、現在使われているか、オーバライドか、などの情報を保持すべき
                self.client.publish("dev/"+data["devId"], json.dumps(d))                
                return
        print("not found")
        self.client.publish("dev/"+data["devId"], json.dumps({"devId": "none"}))
            
    def print_devices(self):
        for i,r in enumerate(self.devices):
            print(i,r)
            


if __name__ == "__main__":
    host = "192.168.207.22"
    port = 1883
    mq = MetaworkMQTT(host, port)
    
    while True:
        time.sleep(1)
        print("---- "+time.ctime()+" -------------")
        mq.print_devices()
