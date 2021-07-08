# subscriber
import paho.mqtt.client as mqtt
import pymysql.cursors

# sql prameters
DatabaseHostName = '127.0.0.1'
DatabaseUserName = 'root'
DatabasePassword = ''
DatabaseName = 'mqtt'
DatabasePort = 3306

count = 0
data = {
	"temperature": "0",
  	"moisture":"0",
  	"light": "0",
  	"conductivity": "0"
}

print("Connecting to database")
connection = pymysql.connect(
	host = DatabaseHostName,
	user = DatabaseUserName,
	password = DatabasePassword,
	db = DatabaseName,
	charset = 'utf8mb4',
	cursorclass = pymysql.cursors.DictCursor,
	port = DatabasePort
)

# sql insertion function
def insertIntoDatabase():
	global data
	"Inserts the mqtt data into the database"
	with connection.cursor() as cursor:
		print("Inserting data: " + data["temperature"] + ";" + data["moisture"] + ";" + data["light"] + ";" + data["conductivity"])
		cursor.callproc('InsertIntoMQTTTable', [str(data["temperature"]), str(data["moisture"]), str(data["light"]), str(data["conductivity"])])
		# cursor.callproc('InsertIntoMQTTTable', [str(message.topic), str(message.payload)[2:][:-1], int(message.qos)])
		connection.commit()

def combine(message):
	global count
	global data
	if(str(message.topic[29:]) == "temperature"):
		data["temperature"]=str(message.payload)[2:][:-1]
		count=1
	elif(str(message.topic[29:]) == "moisture"):
		data["moisture"]=str(message.payload)[2:][:-1]
		count=2
	elif(str(message.topic[29:]) == "light"):
		data["light"]=str(message.payload)[2:][:-1]
		count=3
	elif(str(message.topic[29:]) == "conductivity"):
		data["conductivity"]=str(message.payload)[2:][:-1]
		count=4
	if(count == 4):
		count = 0
		insertIntoDatabase()	


def on_connect(client, userdata, flags, rc):
    print("Connected to a broker!")
    client.subscribe("data/flora/80:EA:CA:89:5C:DD/conductivity")

def on_message(client, userdata, message):
    # print("Received message '" + str(message.payload)[2:][:-1] + "' on topic '"+ message.topic + "' with QoS " + str(message.qos))
	combine(message)
    # print(message.payload.decode())

def on_message_msgs(mosq, obj, msg):
    # This callback will only be called for messages with topics that match
	combine(msg)
	# insertIntoDatabase(msg)


client = mqtt.Client()
# adding multiple subscribers
client.message_callback_add("data/flora/80:EA:CA:89:5C:DD/temperature", on_message_msgs)
client.message_callback_add("data/flora/80:EA:CA:89:5C:DD/moisture", on_message_msgs)
client.message_callback_add("data/flora/80:EA:CA:89:5C:DD/light", on_message_msgs)
client.message_callback_add("data/flora/80:EA:CA:89:5C:DD/conductivity", on_message_msgs)
# connect to mqttt broker
client.connect('broker.hivemq.com', 1883)
client.subscribe("data/flora/80:EA:CA:89:5C:DD/#", 0)

# while True:
client.on_connect = on_connect
client.on_message = on_message
# loop untill message comes
client.loop_forever()