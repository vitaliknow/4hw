import pickle
import time
import socket
import argparse
from collections import OrderedDict

class Task:
	def __init__(self, data, length):
		self._data = data
		self._length = length
		self._id = 0
		self._is_active = False
		self._request_time = 0
	@property
	def data(self):
		return self._data
	@property
	def length(self):
		return self._length
	@property
	def id(self):
		return self._id
	@property
	def is_active(self):
		return self._is_active
	@property
	def request_time(self):
		return self._request_time
	
	def set_id(self, id):
		self._id = id

	def activate(self):
		self._is_active = True
		self._request_time = time.time()
	
	def is_timeOut(self, timeout):
		new_reqTime = time.time()
		difference = new_reqTime - self.request_time
		if self.is_active and (difference > timeout):
			return True
		return False

class TaskQueue:
	def __init__(self, queueName, task):
		self.queueName = queueName
		self.queue_len = 1
		task.set_id(self.queue_len)
		self.tasks_dict = {1: task}

	def add_task(self, task):
		self.queue_len += 1
		task.set_id(self.queue_len)
		
		self.tasks_dict.update({task.id: task})
		return self

	def printQ(self):
		print('\n', self.queueName, ' | ', self.queue_len, ' | ', self.tasks_dict, '\n')
	
	def ack_task(self, id):
			del self.tasks_dict[id]
	
	def get_task(self, timeout):
		for taskID in self.tasks_dict:
			if not(self.tasks_dict[taskID].is_active) or self.tasks_dict[taskID].is_timeOut(timeout):
				self.tasks_dict[taskID].activate()
				return self.tasks_dict[taskID]
		return 'NONE'

class TaskQueuesDict:
	def __init__(self):
		self.queuesDict = {}
	
	def ADD_task(self, queueName, task):
		if queueName in self.queuesDict:
			self.queuesDict[queueName].add_task(task)
		else:
			self.queuesDict[queueName] = TaskQueue(queueName, task)
		return task.id
	
	def GET_task(self, queueName, timeout):
		to_return = 'NONE'
		if queueName in self.queuesDict:
			task_toGet = self.queuesDict[queueName].get_task(timeout)
			if task_toGet != 'NONE':
				to_return = ' '.join([str(task_toGet.id), task_toGet.length, task_toGet.data])
				

		return to_return

	def ACK_task(self, queueName, id, timeout):
		if queueName in self.queuesDict:
			try:
				if not(self.queuesDict[queueName].tasks_dict[id].is_timeOut(timeout)):
					self.queuesDict[queueName].ack_task(id)
					return 'YES'
			except KeyError:
				return 'NO'
		else:
			return 'wrongQueueName'
		
	def is_taskIN(self, queueName, id):
		if queueName in self.queuesDict:
			try:
				id = int(id)
				self.queuesDict[queueName].tasks_dict[id]
				to_return = 'YES'
			except:
				to_return = 'NO'
		else:
			to_return = 'wrongQueueName'
		return to_return
	
	def save(self, path):
		with open(path, 'wb+') as file:
			list_toWrite = []
			for queueName in self.queuesDict:
				list_toWrite.append((queueName, self.queuesDict[queueName].tasks_dict))
			pickle.dump(list_toWrite, file)

class TaskQueueServer:
	def __init__(self, ip, port, path = 'queues.txt', timeout = 60):
		self.ip = ip
		self.port = port
		self.path = path
		self.timeout = timeout
		self.server_sock = None
	
	def run(self):
		self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.server_sock.bind((self.ip, self.port))
		
		self.server_sock.listen(100)
		
		queuesDict = TaskQueuesDict()
		while True:
			client_sock, addr = self.server_sock.accept()
			print(addr, 'is connected')
			recieved_str = client_sock.recv(2048).decode()
			recieved_strlist = recieved_str.split(' ')
			command, queueName = recieved_strlist[0], recieved_strlist[1]
			
			if command == 'ADD':
				dataLen, data = recieved_strlist[2], recieved_strlist[3]
				
				task_byteSize = (int(dataLen)+7)*2
				if (task_byteSize > 2048):
					addit_recieved_str = (client_sock.recv(task_byteSize - 2048).decode())
					recieved_str += addit_recieved_str
					data += addit_recieved_str
				
				task = Task(data, dataLen)
				taskID = str(queuesDict.ADD_task(queueName, task))
				client_sock.sendall(taskID.encode())
				client_sock.close()
			
			elif command == 'GET':
				to_send = queuesDict.GET_task(queueName, self.timeout)
				client_sock.sendall(to_send.encode())
				client_sock.close()
			
			elif command == 'IN':
				search_taskID = recieved_strlist[2]
				is_taskExist = queuesDict.is_taskIN(queueName, search_taskID)
				client_sock.sendall(is_taskExist.encode())
				client_sock.close()
			
			elif command == 'ACK':
				id = int(recieved_strlist[2])
				isDone_task = queuesDict.ACK_task(queueName, id, self.timeout)
				client_sock.sendall(isDone_task.encode())
				client_sock.close()
			
			elif command == 'SAVE':
				QueuesDict.save(self.path)
				client_sock.sendall(b'OK')
				client_sock.close()
			
			else:
				client_sock.sendall(b'ERROR')				
				client_sock.close()
		
		self.server_sock.close()

def parse_args():
    parser = argparse.ArgumentParser(description='This is a simple task queue server with custom protocol')
    parser.add_argument(
        '-p',
        action="store",
        dest="port",
        type=int,
        default=5555,
        help='Server port')
    parser.add_argument(
        '-i',
        action="store",
        dest="ip",
        type=str,
        default='127.0.0.1',
        help='Server ip adress')
    parser.add_argument(
        '-c',
        action="store",
        dest="path",
        type=str,
        default='queuesDict.txt',
        help='Server checkpoints dir')
    parser.add_argument(
        '-t',
        action="store",
        dest="timeout",
        type=int,
        default=300,
        help='Task maximum GET timeout in seconds')
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_args()
    server = TaskQueueServer(**args.__dict__)
    server.run()