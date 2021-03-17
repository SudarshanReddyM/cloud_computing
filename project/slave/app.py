import pika
import time
import json
import sqlite3
import threading
import os
import docker
import shlex
import subprocess
import requests

client = docker.from_env()

def master_consume():
	conn = sqlite3.connect('cloud.db')
	db = conn.cursor()

	try:
		print(os.environ['IS_FIRST_SLAVE'])
	except:
		sleepTime = 40
		print(' [*] Sleeping for ', sleepTime, ' seconds.')
		time.sleep(sleepTime)

	print(' [*] Connecting to server ...')
	connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq',heartbeat=600))
	write_channel = connection.channel()
	write_channel.queue_declare(queue='WRITEQ', durable=True)
	sync_channel = connection.channel()
	sync_channel.exchange_declare(exchange='SYNCEXCHANGE', exchange_type='fanout')
	sync_channel.queue_declare(queue='SYNCQ', durable=True)
	queue_name = "SYNCQ"
	sync_channel.queue_bind(exchange='SYNCEXCHANGE', queue=queue_name)

	print(' [*] Waiting for messages.')


	def callback_write(ch, method, properties, body):
		print(" [x1] Received %s" % body)   #
		content = body.decode()
		result = db.execute(content)
		conn.commit()
		#conn.close()
		sync_channel.basic_publish(
			exchange='SYNCEXCHANGE',
			routing_key='',
			body= content,
			properties=pika.BasicProperties(
				delivery_mode=2,  # make message persistent
			))
		ch.basic_ack(delivery_tag=method.delivery_tag)

	#write_channel.basic_qos(prefetch_count=1)
	write_channel.basic_consume(queue='WRITEQ', on_message_callback=callback_write)
	write_channel.start_consuming()

def update_status():
	while(True):
		command = "cat /proc/1/cgroup | grep 'docker/' | tail -1 | sed 's/^.*\\///' | cut -c 1-12"
		try:
			output = subprocess.check_output(command, stderr=subprocess.STDOUT, shell=True).decode()
			success = True 
		except subprocess.CalledProcessError as e:
			output = e.output.decode()
			success = False
		curr_cont_id = str(output).replace("\n","")
		#print(curr_cont_id)
		#print(client.containers.list())
		for container in client.containers.list():
			#print([curr_cont_id,str(container.id)[:len(curr_cont_id)],curr_cont_id == str(container.id)[:len(curr_cont_id)]])
			if(curr_cont_id == str(container.id)[:len(curr_cont_id)] and "MASTER" in container.name):
				print("SLAVE ELECTED AS MASTER")
				os.environ['IS_MASTER'] = "True"
				master_consume()

if(str(os.environ['IS_MASTER']) == "False"):
	threads = []

	class Response_Object:
		def __init__(self,response,corr_id):
			self.response = response
			self.corr_id = corr_id

	if(str(os.environ['IS_FIRST_SLAVE']) == "True"):
		sleepTime = 40
		print(' [*] Sleeping for ', sleepTime, ' seconds.')
		time.sleep(sleepTime)

	print(' [*] Connecting to server ...')

	print(' [*] Waiting for messages.')


	def callback_read(ch, method, properties, body):
		if(str(os.environ['IS_MASTER']) == "False"):
			conn = sqlite3.connect('cloud.db')
			db = conn.cursor()
			print(" [x2] Received %s" % body)  #
			
			cmd = body.decode()
			result = db.execute(cmd)
			response = result.fetchall()
			print(response)
			
			ch.basic_publish(exchange='',
		                     routing_key=properties.reply_to,
		                     properties=pika.BasicProperties(correlation_id = properties.correlation_id),
		                     body=json.dumps(response))
			ch.basic_ack(delivery_tag=method.delivery_tag)
			conn.close()
			
		else:
			ch.close()

	def callback_sync(ch, method, properties, body):
		if(str(os.environ['IS_MASTER']) == "False"):
			conn = sqlite3.connect('cloud.db')
			db = conn.cursor()
			print(" [x3] Received %s" % body)  #
			content = body.decode()
			result = db.execute(content)
			conn.commit()
			conn.close()
			ch.basic_ack(delivery_tag=method.delivery_tag)
		else:
			ch.close()

	def configure_new_slave():
		response = requests.get("http://0.0.0.0:80/api/v1/syncq/content")
		li_sync = json.loads(response.text)
		print("configuring new slave")  #
		conn = sqlite3.connect('cloud.db')
		db = conn.cursor()
		for query in li_sync:
			print("li_sync query",query)  #
			db.execute(query)
		conn.commit()
		conn.close()

	def read_consume():
		connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq',heartbeat=600))
		read_channel = connection.channel()
		read_channel.queue_declare(queue='READQ', durable=True)
		read_channel.basic_qos(prefetch_count=1)
		read_channel.basic_consume(queue='READQ', on_message_callback=callback_read)
		read_channel.start_consuming()

	def sync_consume():
		connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq',heartbeat=600))
		sync_channel = connection.channel()
		sync_channel.exchange_declare(exchange='SYNCEXCHANGE', exchange_type='fanout')
		result = sync_channel.queue_declare(queue='', durable=True)
		queue_name = result.method.queue
		sync_channel.queue_bind(exchange='SYNCEXCHANGE', queue=queue_name)
		#sync_channel.basic_qos(prefetch_count=1)
		sync_channel.basic_consume(queue=queue_name, on_message_callback=callback_sync)
		sync_channel.start_consuming()

	def manager():
		t1 = threading.Thread(target=read_consume)
		t1.daemon = True
		threads.append(t1)
		t1.start()  

		t2 = threading.Thread(target=sync_consume)
		t2.daemon = True
		threads.append(t2)
		t2.start()

		t3 = threading.Thread(target=update_status)
		t3.daemon = True
		threads.append(t3)
		t3.start()

		t4 = threading.Thread(target=configure_new_slave)
		t4.daemon = True
		threads.append(t4)
		t4.start()

		for t in threads:
			t.join()

	manager()
else:
	master_consume()
