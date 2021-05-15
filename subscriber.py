import paho.mqtt as mqtt
import paho.mqtt.subscribe as subscribe
#from azure.iot.device.aio import IoTHubDeviceClient


class Subscriber:

    def __init__(self, url, port, dev_id, user, pw, return_data_callback, topic="unitec/iot/testing", qos=1):
        self.url = url
        self.port = port
        self.topic = topic
        self.qos = qos
        self.dev_id = dev_id
        self.user = user
        self.pw = pw
        self.client = mqtt.client.Client(
            client_id=self.dev_id, clean_session=True, protocol=mqtt.client.MQTTv311)
        #self.client.username_pw_set(self.user, self.pw)
        # self.client.tls_insecure_set(True)

        self.isConnected = False
        self.handoff_data = return_data_callback

        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        subscribe._on_message_callback = self.on_message
        subscribe._on_message_simple = self.on_message

        print(self.client.connect("ec2-54-82-2-238.compute-1.amazonaws.com", 8883))
        #self.client.connect(self.url, self.port)
        self.client.subscribe(self.topic, self.qos)
        #self.deviceConnectionString = "HostName=pollutant.azure-devices.net;DeviceId=pollutant_listener;SharedAccessKey=t+EzOgW5RYrpuKpBsdERkW4GFkgXvjyy+7kVkWRSQ7s="

        #device_client = IoTHubDeviceClient.create_from_connection_string(self.deviceConnectionString, websockets=True)

    def start(self):
        #self.client.subscribe(self.topic, self.qos)
        self.client.loop_start()

    def stop(self):
        # self.client.unsubscribe(self.topic)
        self.client.loop_stop()

    def on_message(self, client, userdata, message):
        print(f"{message.topic}: {message.payload}")
        self.handoff_data(message.payload)

    def on_connect(self, client, userdata, flags, rc):
        self.isConnected = True
        print(f"Connected with result code {str(rc)}")

    def on_disconnect(self, client, userdata, rc):
        self.isConnected = False
        if rc != 0:
            print("Unexpected disconnect from broker")
        else:
            print("client disconnected from broker successfully")
