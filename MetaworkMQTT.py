# メタワーク用の MQTT manager

# internal instruction
# https://uclab.esa.io/posts/8825 (privavte)


import paho.mqtt.client as mqtt
import json
import time

TIMEOUT_HOUR = 1  # for More than 1 hour, we need to clear the device db.
#client robots should update there info for each 30min.
class MetaworkMQTT:
    def __init__(self, host, port): #, username, password):
        self.host = host
        self.port = port
        self.mod = False
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
        client.subscribe("mgr/unregister")
        client.subscribe("mgr/request")

    def on_message(self, client, userdata, msg):
#        print(msg.topic+" "+str(msg.payload))
        if msg.topic == "mgr/register":
            self.update_status()
            self.register(msg)
        elif msg.topic == "mgr/unregister":    
            self.update_status()
            self.unregister(msg)
        elif msg.topic == "mgr/request":    
            self.request(msg)
        
    # we need to flush obsolute devices after TIMEOUT_HOUR
    def update_status(self):
        for d in self.devices:
            if d["registered"]-(time.time()) > TIMEOUT_HOUR*3600:
                print("TIMEOUT: ",d) 
                self.devices.remove(d) # あれば、そのデータを消す            
            
    def register(self, msg):
        data = json.loads(msg.payload)
        ver = data.get("version", "none")
        if "device" in data:
            print("register:", data["devId"][:4]+"-"+data["devId"][-4:],ver,data["device"]["agent"])
        else:
            print("register:", data["devId"][:4]+"-"+data["devId"][-4:],ver)
            
        # 同じIDのデバイスがあるかを確認
        for d in self.devices:
            if d["devId"] == data["devId"]:
                self.devices.remove(d) # あれば、そのデータを消したうえで
                break
        self.devices.append({
            "type": data["codeType"],
            "version":ver,
            "devId": data["devId"],
            "devType": data["devType"],
            "date": data["date" ],
            "registered": int(time.time())
        })
        self.mod = True

    def unregister(self, msg):
        data = json.loads(msg.payload)
        print("unregister:", data["devId"])
        # 同じIDのデバイスがあるかを確認
        for d in self.devices:
            if d["devId"] == data["devId"]:
                self.devices.remove(d) # あれば、そのデータを消したうえで
                break
        self.mod = True
        
        
#        print("register")
#        self.client.publish("mgr/register", json.dumps(data))

#   希望するタイプのデバイスがあるかを確認
    def request(self, msg):
        data = json.loads(msg.payload)
        for d in self.devices:
            if d["devType"] == "robot" and d["type"] == data["type"]:
                print("Request found",d) # 本当は、現在使われているか、オーバライドか、などの情報を保持すべき
                self.client.publish("dev/"+data["devId"], json.dumps(d))                
                return
        print("not found request ", data["type"])
        self.client.publish("dev/"+data["devId"], json.dumps({"devId": "none"}))
            
    def print_devices(self):
        for i,r in enumerate(self.devices):
            print(i,r)
        self.mod = False
            


if __name__ == "__main__":
    host = "192.168.207.22"
    port = 1883
    mq = MetaworkMQTT(host, port)
    
    while True:
        time.sleep(1)
        if mq.mod:
            print("---- "+time.ctime()+" -------------")
            mq.print_devices()
