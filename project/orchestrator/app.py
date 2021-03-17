from flask import Flask
from flask import request
from flask import jsonify
from flask import url_for
from flask import redirect
import json
from datetime import datetime
import pika
import uuid
import docker
import time
import threading
import shlex
import subprocess
from kazoo.client import KazooClient, KazooState


app = Flask(__name__)
client = docker.from_env()
number_of_reads = 0
sync_messages = []

sleepTime = 20
print(' [*] Sleeping for ', sleepTime, ' seconds.')
time.sleep(sleepTime)

connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq', heartbeat=600))
write_channel = connection.channel()
write_channel.queue_declare(queue='WRITEQ', durable=True)
correl_id = 0
is_first_write = 1
is_scaling = 0
is_first_time = 1

zk = KazooClient(hosts='Zookeeper:2181')
zk.start()

def my_listener(state):
    if state == KazooState.LOST:
        
        print("Connection to Zookeeper lost")
    elif state == KazooState.SUSPENDED:
        
        print("Zookeeper Disconnected")
    else:
        
        print("Zookeeper Connected Sucessfully")
	
zk.add_listener(my_listener)
if(zk.exists("/workers/slaves")):
	zk.delete("/workers/slaves", recursive=True)
	zk.delete("/workers/master", recursive=True)
zk.ensure_path("/workers/slaves")
zk.ensure_path("/workers/master")

def is_master(container):
	
	if("MASTER" in container.name or "master" in container.name):
		return True
	else:
		return False

def update_zookeeper():
	print("Entered Update Zookeeper")
	while(True):
		global zk
		global is_scaling
		global is_first_time
		if(is_first_time == 1):
			for container in client.containers.list():
				if(str(container.image) == "<Image: 'project_slave:latest'>" and not(is_master(container))):
					
					zk.create("/workers/slaves/"+str(container.name), str(container.id).encode('utf-8'))
				elif(str(container.image) == "<Image: 'project_master:latest'>"):
					
					zk.create("/workers/master/"+str(container.name), str(container.id).encode('utf-8'))
			is_first_time = 0
		elif(is_scaling == 0):
			slave_names = [str(container.name) for container in client.containers.list() if str(container.image) == "<Image: 'project_slave:latest'>" and not(is_master(container))]
			for zk_name in zk.get_children("/workers/slaves"):
				if zk_name not in slave_names:
					if(zk.exists("/workers/slaves/"+str(zk_name))):
						print("ZOOKEEPER OBSERVED THAT SLAVE "+zk_name+" FAILED")
						zk.delete("/workers/slaves/"+str(zk_name))
						container = client.containers.run("project_slave:latest", network = "project_default", environment =["IS_MASTER=False","IS_FIRST_SLAVE=False"], detach = True)
						zk.create("/workers/slaves/"+str(container.name), str(container.id).encode('utf-8'))
						print("ZOOKEEPER CREATED SLAVE "+container.name)
						
			master_name = [str(container.name) for container in client.containers.list() if is_master(container)]
			mutex = 0
			for zk_name in zk.get_children("/workers/master"):
				if zk_name not in master_name:
					if(zk.exists("/workers/master/"+str(zk_name)) and ("master" in str(zk_name) or "MASTER" in str(zk_name)) and mutex==0):
						mutex = 1
						print("ZOOKEEPER OBSERVED THAT MASTER FAILED")
						zk.delete("/workers/master/"+str(zk_name))
						
						min_pid = 100000
						min_container = None
						for container in client.containers.list():
							if(str(container.image) == "<Image: 'project_slave:latest'>" and not(is_master(container))):
								command = shlex.split("docker inspect -f \'{{ .State.Pid }}\' "+str(container.id))
								try:
									output = subprocess.check_output(command, stderr=subprocess.STDOUT).decode()
									success = True 
								except subprocess.CalledProcessError as e:
									output = e.output.decode()
									success = False
								if int(output) < min_pid:
									min_pid = int(output)
									min_container = container
						
						zk.delete("/workers/slaves/"+str(min_container.name))
						min_container.rename(min_container.name+"-MASTER")
						zk.create("/workers/master/"+str(min_container.name)+"-MASTER", str(min_container.id).encode('utf-8'))
						print("ZOOKEEPER ELECTED NEW MASTER : ",str(min_container.name)+"-MASTER")
						container = client.containers.run("project_slave:latest", network = "project_default", environment =["IS_MASTER=False","IS_FIRST_SLAVE=False"], detach = True)
						zk.create("/workers/slaves/"+str(container.name), str(container.id).encode('utf-8'))
						print("ZOOKEEPER CREATED SLAVE "+container.name)
						mutex = 0

def callback_sync(ch, method, properties, body):
	global sync_messages
	content = body.decode()
	sync_messages.append(content)
	ch.basic_ack(delivery_tag=method.delivery_tag)

def consume_from_syncq():
	connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq',heartbeat=600))
	sync_channel = connection.channel()
	sync_channel.exchange_declare(exchange='SYNCEXCHANGE', exchange_type='fanout')
	sync_channel.queue_declare(queue='SYNCQ', durable=True)
	queue_name = "SYNCQ"
	sync_channel.queue_bind(exchange='SYNCEXCHANGE', queue=queue_name)
	print("Consumption from SYNCQ beginning")
	sync_channel.basic_consume(queue=queue_name, on_message_callback=callback_sync)
	sync_channel.start_consuming()
					
@zk.ChildrenWatch("/workers/slaves")
def watch_slave(children):
	print("Zookeeper Slaves : ",children)

@zk.ChildrenWatch("/workers/master")
def watch_master(children):
	print("Zookeeper Master : ",children)		

class OrchestratorRpcClient:
	def __init__(self):
		global correl_id
		self.corr_id = str(correl_id)
		correl_id += 1

		self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq', heartbeat=600))

		self.channel = self.connection.channel()

		result = self.channel.queue_declare(queue='READQ', durable=True)
		self.callback_queue = result.method.queue
		#self.callback_queue = 'RESPONSEQ'

		self.channel.basic_consume(
			queue=self.callback_queue,
			on_message_callback=self.on_response)
		

	def on_response(self, ch, method, props, body):
		
		if self.corr_id == props.correlation_id:
			self.response = json.loads(body)
			ch.basic_ack(delivery_tag=method.delivery_tag)
			self.connection.close()

	def call(self, query):  
		self.response = None
		self.channel.basic_publish(
			exchange='',
			routing_key='READQ',
			properties=pika.BasicProperties(
				reply_to=self.callback_queue,
				correlation_id=self.corr_id,
			),
			body=query)
		while self.response is None:
			self.connection.process_data_events()
			
			
		return self.response
		



def deploy_slaves():
	global number_of_reads
	global is_scaling
	is_scaling = 1
	curr_num_containers = 0
	for container in client.containers.list():
		print(container.image,type(container.image))
		if(str(container.image) == "<Image: 'project_slave:latest'>" and not(is_master(container))):
			curr_num_containers += 1
	print("Number of slaves : "+str(curr_num_containers), flush=True)
	print("Number of reads : "+str(number_of_reads))
	if(curr_num_containers > ((number_of_reads-1)//20)+1):
		for container in client.containers.list():
			if(str(container.image) == "<Image: 'project_slave:latest'>" and not(is_master(container)) and curr_num_containers>((number_of_reads-1)//20)+1 and not(curr_num_containers==1)):
				print("Stopping Container")
				zk.delete("/workers/slaves/"+str(container.name))
				container.kill()
				curr_num_containers -= 1
			elif(curr_num_containers==((number_of_reads-1)//20)+1):
				break
		number_of_reads = 0
	elif(curr_num_containers < ((number_of_reads-1)//20)+1):
		no_of_reads = number_of_reads
		number_of_reads = 0
		for i in range(curr_num_containers,((no_of_reads-1)//20)+1):
			print("Creating New Slave")
			'''t = threading.Thread(target = create_new_slave)
			t.start()'''
			container = client.containers.run("project_slave:latest", network = "project_default", environment =["IS_MASTER=False","IS_FIRST_SLAVE=False"], detach = True)
			zk.create("/workers/slaves/"+str(container.name), str(container.id).encode('utf-8'))
		print("container.logs")  #
		for container in client.containers.list():
			print(container.image,type(container.image))
		
	is_scaling = 0



def set_interval(func, sec):
    def func_wrapper():
        set_interval(func, sec)
        func()
    t = threading.Timer(sec, func_wrapper)
    t.start()
    return t



@app.route('/api/v1/db/read', methods=['POST'])
def read_data():
	if(correl_id == 0):
		t = threading.Thread(target = update_zookeeper)
		t.start()
		set_interval(deploy_slaves, 120)
	global number_of_reads
	number_of_reads += 1
	print(number_of_reads)
	
	_json = request.get_json()
	
					
				
	if (_json['method']=='GETALL'):				
			
			sql="SELECT count(*) FROM parking_ticket WHERE license_plate = '%s'"%(_json["license_plate"])
			print('SQL Query is: {}'.format(sql))
							
			orpc = OrchestratorRpcClient()
			response = orpc.call(sql)
			print('Query sent to slave. Got response: {}'.format(response))
			print("Response_len:", len(response))
			
			#rows1=json.loads(response)
			
			
			print("response",response)
				
			response2=json.dumps(response)
				
			return response2,200	

	if (_json['method']=='COUNT'):				
			
			sql="SELECT * FROM parking_ticket"
			print('SQL Query is: {}'.format(sql))
							
			orpc = OrchestratorRpcClient()
			response = orpc.call(sql)
			print('Query sent to slave. Got response: {}'.format(response))
			print("Response_len:", len(response))
			
			#rows1=json.loads(response)
			
			
			print("response",response)
				
			response2=json.dumps(response)
				
			return response2,200     		


	if _json['method'] == 'GETTIME':
		sql = "SELECT entry_time from parking_ticket WHERE license_plate = '%s'"%(_json["license_plate"])	
		print('SQL Query is: {}'.format(sql))
		orpc = OrchestratorRpcClient()
		response = orpc.call(sql)
		print('Query sent to slave. Got response: {}'.format(response))
		print("Response_len:", len(response))
		
		#rows1=json.loads(response)
		
		
		print("response",response)
			
		response2=json.dumps(response)
			
		return response2,200

	if _json['method'] == 'GETDETAILS':
		sql = "SELECT name_of_person,bhim_upi from parking_ticket WHERE license_plate = '%s'"%(_json["license_plate"])	
		print('SQL Query is: {}'.format(sql))
		orpc = OrchestratorRpcClient()
		response = orpc.call(sql)
		print('Query sent to slave. Got response: {}'.format(response))
		print("Response_len:", len(response))
		
		#rows1=json.loads(response)
		
		
		print("response",response)
			
		response2=json.dumps(response)
			
		return response2,200

				
			


#writing the sql query
def construct_query(_json):
	
		if(_json['method']=='INSERT'):
			#my_json_obj = {"license_plate": _vehicleNumber, "name_of_person": "Test_username", "bhim_upi": "user@paytm", "aadhar_no": "BLAHBLAH", "mobile_number": "9986815155", "entry_time": _entryTime, "method": "INSERT"}
			_license_plate = _json['license_plate']
			_person_name = _json['name_of_person']
			_bhim_upi = _json['bhim_upi']
			_aadhar_no = _json['aadhar_no']
			_mobile_number = _json['mobile_number']
			_entry_time = _json['entry_time']
			sql = "INSERT INTO parking_ticket(name_of_person,bhim_upi,aadhar_no,mobile_number,license_plate,entry_time) VALUES('%s','%s','%s','%s','%s','%s')"%(_person_name,_bhim_upi,_aadhar_no,_mobile_number,_license_plate,_entry_time)
			return sql



		if(_json['method']=='CLEAR'):
			sql = "DELETE from parking_ticket"
			return sql



		if(_json['method']=='UPDATE'):
			sql = "UPDATE parking_ticket set entry_time='%s' WHERE license_plate='%s'"%(_json['entry_time'],_json['license_plate'])

			# sql = "UPDATE parking_ticket set exit_time='%s' WHERE license_plate='%s'"%(_entryTime,_vehicleNumber)
			return sql

			
		

@app.route('/api/v1/db/write', methods=['POST'])
def write_data():
	global is_first_write
	global correl_id
	if(is_first_write == 1 and correl_id == 0):
		t1 = threading.Thread(target = update_zookeeper)
		t1.start()
	elif(is_first_write == 1):
		t2 = threading.Thread(target = consume_from_syncq)
		t2.start()
	is_first_write = 0
	
	
	data = request.get_json()
	query = construct_query(data)
	print("write API:", query)
	
	
	write_channel.basic_publish(
				exchange='',
				routing_key='WRITEQ',
				body=query,
				properties=pika.BasicProperties(
					delivery_mode=2,  # make message persistent
				))
	print('Data sent on the writeQ to master. SQL Query sent: {}'.format(query))
	return jsonify({}), 200



@app.route('/api/v1/crash/master', methods=['POST'])
def crash_master():
	for container in client.containers.list():
		#if(str(container.image) == "<Image: 'project_master:latest'>"):
			#return str(is_master(container)), 200
		if(is_master(container)):
			command = shlex.split("docker inspect -f \'{{ .State.Pid }}\' "+str(container.id))
			try:
			    output = subprocess.check_output(command, stderr=subprocess.STDOUT).decode()
			    success = True 
			except subprocess.CalledProcessError as e:
			    output = e.output.decode()
			    success = False
			master_pid = int(output)
			print("Killing Master")
			container.kill()
	return jsonify([master_pid]), 200

@app.route('/api/v1/crash/slave', methods=['POST'])
def crash_slave():
	max_pid = 0
	max_container = None
	for container in client.containers.list():
		if(str(container.image) == "<Image: 'project_slave:latest'>" and not(is_master(container))):
			command = shlex.split("docker inspect -f \'{{ .State.Pid }}\' "+str(container.id))
			try:
			    output = subprocess.check_output(command, stderr=subprocess.STDOUT).decode()
			    success = True 
			except subprocess.CalledProcessError as e:
			    output = e.output.decode()
			    success = False
			if int(output) > max_pid:
				max_pid = int(output)
				max_container = container
	print("Killing Slave")
	max_container.kill()
	return jsonify([max_pid]), 200

@app.route('/api/v1/worker/list', methods=['GET'])
def worker_list():
	workers_list = []
	for container in client.containers.list():
		if(str(container.image) == "<Image: 'project_slave:latest'>" or str(container.image) == "<Image: 'project_master:latest'>"):
			command = shlex.split("docker inspect -f \'{{ .State.Pid }}\' "+str(container.id))
			try:
			    output = subprocess.check_output(command, stderr=subprocess.STDOUT).decode()
			    success = True 
			except subprocess.CalledProcessError as e:
			    output = e.output.decode()
			    success = False
			workers_list.append(int(output))
	return jsonify(sorted(workers_list)), 200

@app.route('/api/v1/syncq/content', methods=['GET'])
def get_syncq_content():
	global sync_messages
	return jsonify(sync_messages), 200

if __name__=='__main__':
	app.run(debug=True, host='0.0.0.0', port=80)
