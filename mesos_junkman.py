#!/usr/bin/env python
import urllib3
import json
import time
import sys
import os
import logging
import logging.config
from urllib3 import PoolManager
from apscheduler.schedulers.blocking import BlockingScheduler

# logging configuration
logging.config.fileConfig('/usr/etc/logging.conf')
logger = logging.getLogger('MesosJunkman')

class MesosJunkman(object):

	"""
	MesosJunkman is used for collecting metrics of hosts which are in the mesos cluster
	"""
	def __init__(self, cluster, hostname, port):
		super(MesosJunkman, self).__init__()
		self.cluster = cluster
		self.hostname = hostname
		self.port = port

	pool_manager = urllib3.PoolManager()
	protocol = "http"

	def host_cpu(self, path):
		url = self.protocol + "://" + self.hostname + ":" + str(self.port) + path
		cpu_usage = []
		try:
			cpu_usage_original = MesosJunkman.pool_manager.request("GET",url)
			if cpu_usage_original.status == 200:
				cpu_usage = json.loads(cpu_usage_original.data)
				cpu_idle = float(cpu_usage["data"][0][0][:-1])
				cpu_busy = float(cpu_usage["data"][0][1][:-1])
				cpu_user = float(cpu_usage["data"][0][2][:-1])
				cpu_nice = float(cpu_usage["data"][0][3][:-1])
				cpu_system = float(cpu_usage["data"][0][4][:-1])
				cpu_iowait = float(cpu_usage["data"][0][5][:-1])
				cpu_irq = float(cpu_usage["data"][0][6][:-1])
				cpu_softirq = float(cpu_usage["data"][0][7][:-1])
				cpu_steal = float(cpu_usage["data"][0][8][:-1])
				cpu_guest = float(cpu_usage["data"][0][9][:-1])

				cpu_usage = []
				cpu_tags = "hostname="+self.hostname+",cluster="+self.cluster
				cpu_idle_metric = {
					"metric":"mesos.cpu.idle",
					"value":cpu_idle,
					"tags": cpu_tags
				}
				cpu_busy_metric = {
					"metric":"mesos.cpu.busy",
					"value":cpu_busy,
					"tags":cpu_tags
				}
				cpu_user_metric = {
					"metric":"mesos.cpu.user",
					"value":cpu_user,
					"tags":cpu_tags
				}
				cpu_nice_metric = {
					"metric":"mesos.cpu.nice",
					"value":cpu_nice,
					"tags":cpu_tags
				}
				cpu_system_metric = {
					"metric":"mesos.cpu.system",
					"value":cpu_system,
					"tags":cpu_tags
				}
				cpu_iowait_metric = {
					"metric":"mesos.cpu.iowait",
					"value":cpu_iowait,
					"tags":cpu_tags
				}
				cpu_irq_metric = {
					"metric":"mesos.cpu.irq",
					"value":cpu_irq,
					"tags":cpu_tags
				}
				cpu_softirq_metric = {
					"metric":"mesos.cpu.softirq",
					"value":cpu_softirq,
					"tags":cpu_tags
				}
				cpu_steal_metric = {
					"metric":"mesos.cpu.steal",
					"value":cpu_steal,
					"tags":cpu_tags
				}
				cpu_guest_metric = {
					"metric":"mesos.cpu.guest",
					"value":cpu_guest,
					"tags":cpu_tags
				}

				cpu_usage.append(cpu_idle_metric)
				cpu_usage.append(cpu_busy_metric)
				cpu_usage.append(cpu_user_metric)
				cpu_usage.append(cpu_nice_metric)
				cpu_usage.append(cpu_system_metric)
				cpu_usage.append(cpu_iowait_metric)
				cpu_usage.append(cpu_irq_metric)
				cpu_usage.append(cpu_softirq_metric)
				cpu_usage.append(cpu_steal_metric)
				cpu_usage.append(cpu_guest_metric)
			else:
				logger.warn("Response status of %s is not 200",url)
		except:
			err = sys.exc_info()
			logger.error("%s:%s",err[0],err[1])
		return cpu_usage

	def host_memory(self, path):
		url = self.protocol + "://" + self.hostname + ":" + str(self.port) + path
		memory_usage = []
		try:
			memory_usage_original = MesosJunkman.pool_manager.request("GET", url)
			if memory_usage_original.status == 200:
				memory_usage = json.loads(memory_usage_original.data)
				memory_total = memory_usage["data"][0]
				memory_used = memory_usage["data"][1]
				memory_free = memory_usage["data"][2]

				memory_usage = []
				memory_tags = "hostname="+self.hostname+",cluster="+self.cluster
				memory_total_metric = {
					"metric":"mesos.mem.memtotal",
					"value":memory_total,
					"tags":memory_tags
				}
				memory_used_metric = {
					"metric":"mesos.mem.memused",
					"value":memory_used,
					"tags":memory_tags
				}
				memory_free_metric = {
					"metric":"mesos.mem.memfree",
					"value":memory_free,
					"tags":memory_tags
				}

				memory_usage.append(memory_total_metric)
				memory_usage.append(memory_used_metric)
				memory_usage.append(memory_free_metric)
			else:
				logger.warn("Response status of %s is not 200",url)
		except:
			err = sys.exc_info()
			logger.error("%s:%s",err[0],err[1])
		return memory_usage

	def translate_disk_df(self,disk_df):
		end = disk_df[-1]
		if end == "P":
			return float(disk_df[:-1])*1024*1024*1024
		elif end == "T":
			return float(disk_df[:-1])*1024*1024
		elif end == "G":
			return float(disk_df[:-1])*1024
		elif end == "M":
			return float(disk_df[:-1])
		elif end == "K":
			return float(disk_df[:-1])/1024
		elif end == "B":
			return float(disk_df[:-1])/(1024*1024)
		else:
			return float(disk_df)

	def host_disk_df(self, path):
		url = self.protocol + "://" + self.hostname + ":" + str(self.port) + path
		disk_df_usage = []
		try:
			disk_df_usage_original = MesosJunkman.pool_manager.request("GET", url)
			if disk_df_usage_original.status == 200:
				disk_df_usage_tmp = json.loads(disk_df_usage_original.data)
				for dfu in disk_df_usage_tmp["data"]:
					disk_df_fs = dfu[0]
					disk_df_btotal = self.translate_disk_df(dfu[1])
					disk_df_bused = self.translate_disk_df(dfu[2])
					disk_df_bfree = self.translate_disk_df(dfu[3])
					disk_df_buse = dfu[4]
					disk_df_mouted = dfu[5]
					disk_df_itotal = self.translate_disk_df(dfu[6])
					disk_df_iused = self.translate_disk_df(dfu[7])
					disk_df_ifree = self.translate_disk_df(dfu[8])
					disk_df_iuse = dfu[9]
					disk_df_vfstype = dfu[10]
					
					disk_df_tags = "hostname="+self.hostname+",cluster="+self.cluster+",mount="+disk_df_mouted+",fstype="+disk_df_fs+""
					disk_df_btotal_metric = {
						"metric":"mesos.df.bytes.total",
						"value":disk_df_btotal,
						"tags":disk_df_tags
					}
					disk_df_bused_metric = {
						"metric":"mesos.df.bytes.used",
						"value":disk_df_bused,
						"tags":disk_df_tags
					}
					disk_df_bfree_metric = {
						"metric":"mesos.df.bytes.free",
						"value":disk_df_bfree,
						"tags":disk_df_tags
					}
					disk_df_itotal_metric = {
						"metric":"mesos.df.inodes.total",
						"value":disk_df_itotal,
						"tags":disk_df_tags
					}
					disk_df_iused_metric = {
						"metric":"mesos.df.inodes.used",
						"value":disk_df_iused,
						"tags":disk_df_tags
					}
					disk_df_ifree_metric = {
						"metric":"mesos.df.inodes.free",
						"value":disk_df_ifree,
						"tags":disk_df_tags
					}

					disk_df_usage.append(disk_df_btotal_metric)
					disk_df_usage.append(disk_df_bused_metric)
					disk_df_usage.append(disk_df_bfree_metric)
					disk_df_usage.append(disk_df_itotal_metric)
					disk_df_usage.append(disk_df_iused_metric)
					disk_df_usage.append(disk_df_ifree_metric)
			else:
				logger.warn("Response status of %s is not 200",url)
		except:
			err = sys.exc_info()
			logger.error("%s:%s",err[0],err[1])
		return disk_df_usage

	def host_disk_io(self, path):
		url = self.protocol + "://" + self.hostname + ":" + str(self.port) + path
		disk_io_usage = []
		try:
			disk_io_usage_original = MesosJunkman.pool_manager.request("GET", url)
			if disk_io_usage_original.status == 200:
				disk_io_usage_tmp = json.loads(disk_io_usage_original.data)
				for io in disk_io_usage_tmp["data"]:
					disk_io_device = io[0]
					disk_io_rmqs = io[1]
					disk_io_wrmqs = io[2]
					disk_io_rs = io[3]
					disk_io_ws = io[4]
					disk_io_rkbs = io[5]
					disk_io_wkbs = io[6]
					disk_io_avgrqsz = io[7]
					disk_io_avgqusz = io[8]
					disk_io_await = io[9]
					disk_io_svctm = io[10]
					disk_io_util = io[11][:-1]

					disk_io_tags = "hostname="+self.hostname+",cluster="+self.cluster+",device="+disk_io_device+""
					disk_io_rmqs_metric = {
						"metric":"mesos.disk.io.read_merged",
						"value":disk_io_rmqs,
						"tags":disk_io_tags
					}
					disk_io_wrmqs_metric = {
						"metric":"mesos.disk.io.write_merged",
						"value":disk_io_wrmqs,
						"tags":disk_io_tags
					}
					disk_io_rs_metric = {
						"metric":"mesos.disk.io.read_requests",
						"value":disk_io_rs,
						"tags":disk_io_tags
					}
					disk_io_ws_metric = {
						"metric":"mesos.disk.io.write_requests",
						"value":disk_io_ws,
						"tags":disk_io_tags
					}
					disk_io_rkbs_metric = {
						"metric":"mesos.disk.io.read_bytes",
						"value":disk_io_rkbs,
						"tags":disk_io_tags
					}
					disk_io_wkbs_metric = {
						"metric":"mesos.disk.io.write_bytes",
						"value":disk_io_wkbs,
						"tags":disk_io_tags
					}
					disk_io_avgrqsz_metric = {
						"metric":"mesos.disk.io.avgrq_sz",
						"value":disk_io_avgrqsz,
						"tags":disk_io_tags
					}
					disk_io_avgqusz_metric = {
						"metric":"mesos.disk.io.avgqu_sz",
						"value":disk_io_avgqusz,
						"tags":disk_io_tags
					}
					disk_io_await_metric = {
						"metric":"mesos.disk.io.await",
						"value":disk_io_await,
						"tags":disk_io_tags
					}
					disk_io_svctm_metric = {
						"metric":"mesos.disk.io.svctm",
						"value":disk_io_svctm,
						"tags":disk_io_tags
					}
					disk_io_util_metric = {
						"metric":"mesos.disk.io.util",
						"value":disk_io_util,
						"tags":disk_io_tags
					}
					
					disk_io_usage.append(disk_io_rmqs_metric)
					disk_io_usage.append(disk_io_wrmqs_metric)
					disk_io_usage.append(disk_io_rs_metric)
					disk_io_usage.append(disk_io_ws_metric)
					disk_io_usage.append(disk_io_rkbs_metric)
					disk_io_usage.append(disk_io_wkbs_metric)
					disk_io_usage.append(disk_io_avgrqsz_metric)
					disk_io_usage.append(disk_io_avgqusz_metric)
					disk_io_usage.append(disk_io_await_metric)
					disk_io_usage.append(disk_io_svctm_metric)
					disk_io_usage.append(disk_io_util_metric)
			else:
				logger.warn("Response status of %s is not 200",url)
		except:
			err = sys.exc_info()
			logger.error("%s:%s",err[0],err[1])
		return disk_io_usage

	def host_loadavg(self, path):
		url = self.protocol + "://" + self.hostname + ":" + str(self.port) + path
		loadavg = []
		try:
			loadavg_original = MesosJunkman.pool_manager.request("GET",url)
			if loadavg_original.status == 200:
				loadavg_tmp = json.loads(loadavg_original.data)
				load_one_value = loadavg_tmp["data"][0][0]
				load_one_percent = loadavg_tmp["data"][0][1]
				load_five_value = loadavg_tmp["data"][1][0]
				load_five_percent = loadavg_tmp["data"][1][1]
				load_fifteen_value = loadavg_tmp["data"][2][0]
				load_fifteen_percent = loadavg_tmp["data"][2][1]
				
				load_tags = "hostname="+self.hostname+",cluster="+self.cluster
				load_one_value_metric = {
					"metric":"mesos.load.1min",
					"value":load_one_value,
					"tags":load_tags
				}

				load_five_value_metric = {
					"metric":"mesos.load.5min",
					"value":load_five_value,
					"tags":load_tags
				}

				load_fifteen_value_metric = {
					"metric":"mesos.load.15min",
					"value":load_fifteen_value,
					"tags":load_tags
				}

				loadavg.append(load_one_value_metric)
				loadavg.append(load_five_value_metric)
				loadavg.append(load_fifteen_value_metric)
			else:
				logger.warn("Response status of %s is not 200",url)
		except:
			err = sys.exc_info()
			logger.error("%s:%s",err[0],err[1])
		return loadavg

	def host_net(self, path):
		url = self.protocol + "://" + self.hostname + ":" + str(self.port) + path
		net_usage = []
		pass

	def host_metrics(self,cpu_path,memory_path,disk_df_path,disk_io_path,loadavg_path):
		cpu_usage = self.host_cpu(cpu_path)
		memory_usage = self.host_memory(memory_path)
		disk_df_usage = self.host_disk_df(disk_df_path)
		disk_io_usage = self.host_disk_io(disk_io_path)
		loadavg = self.host_loadavg(loadavg_path)

		metrics = []
		for mt in cpu_usage:
			metrics.append(mt)
		for mt in memory_usage:
			metrics.append(mt)
		for mt in disk_df_usage:
			metrics.append(mt)
		for mt in disk_io_usage:
			metrics.append(mt)
		for mt in loadavg:
			metrics.append(mt)
		print(metrics)
		logger.info(metrics)

def get_hostname():
	return os.popen("hostname -f").read().strip()

def get_port():
	return 21988

def push_opentsdb(cluster):
	pm = PoolManager()
	ts = str(time.time()).split(".")[0]
	port = get_port()
	hostname = get_hostname()
	#hostname = "haoziyu-worker-dev003-bjdxt9.qiyi.virtual"
	ma = MesosJunkman(cluster,hostname, port)
	path = "/page/cpu/usage"
	cpu_idle = ma.host_cpu(path)
	data = []
	for cpu in cpu_idle:
		cpu["timestamp"] = int(ts)
		cpu["tags"] = {
			"hostname":hostname,
			"cluster":cluster
		}
		data.append(cpu)
	path = "/page/memory"
	mem = ma.host_memory(path)
	for m in mem:
		m["timestamp"] = int(ts)
		m["tags"] = {
			"hostname":hostname,
			"cluster":cluster
		}
		data.append(m)
	path = "/page/system/loadavg"
	loadavg = ma.host_loadavg(path)
	for ld in loadavg:
		ld["timestamp"] = int(ts)
		ld["tags"] = {
			"hostname":hostname,
			"cluster":cluster
		}
		data.append(ld)
	# path = "/page/df"
	# df = ma.host_disk_df(path)
	# for d in df:
	# 	d["timestamp"] = int(ts)
	# 	tags = d["tags"].split(",")
	# 	mount = tags[2].split("=")[1]
	# 	fstype = tags[3].split("=")[1]
	# 	d["tags"] = {
	# 		"hostname":hostname,
	# 		"cluster":cluster,
	# 		"mount":mount,
	# 		"fstype":fstype
	# 	}
	# 	data.append(d)

	# path = "/page/diskio"
	# dio = ma.host_disk_io(path)
	# for io in dio:
	# 	io["timestamp"] = int(ts)
	# 	tags = io["tags"].split(",")
	# 	device = tags[2].split("=")[1]
	# 	io["tags"] = {
	# 		"hostname":hostname,
	# 		"cluster":cluster,
	# 		"device":device
	# 	}
	# 	data.append(io)
	# print(data)
	logger.info(data)
	pm.urlopen("POST","http://10.15.230.1:4242/api/put",headers={"Content-Type":"application/json"},body=json.dumps(data))

if __name__ == "__main__":
	try:
		cluster_name = sys.argv[1]
		sche = BlockingScheduler()
		sche.add_job(lambda:push_opentsdb(cluster_name),"interval",seconds=1)
		sche.start()
	except:
		print(sys.exc_info)
