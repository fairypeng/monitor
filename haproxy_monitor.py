#!/usr/bin/env python
# -*- coding:utf-8 -*-
# vim: set noet sw=4 ts=4 sts=4 ff=unix fenc=utf8:

"""
这个脚本是根据小米监控开源的haproxy脚本进行改造的脚本，里面有很多逻辑错误，稍做调整，参考。
"""


import os
import sys
import stat
import socket
import time
import json


class HaproxyStats(object):
    def __init__(self, conf):
        self.StatsFile = conf["stats_file"]
        self.BufferSize = conf["buffer_size"]
        self.MetricPrefix = conf["metric_prefix"]
        self.Metrics = conf["metrics"]
        self._status = True
        self.socket_ = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        if conf["endpoint_type"] == "hostname":
            self.EndpointName = socket.gethostname()
        else:
            self.EndpointName = self.get_local_ip()

    def __del__(self):
        self.socket_.close()

    def connect(self):
        try:
            if os.path.exists(self.StatsFile) and stat.S_ISSOCK(os.stat(self.StatsFile).st_mode):
                self.socket_.connect(self.StatsFile)
            else:
                print >> sys.stderr, "-- SOCK file: " + self.StatsFile + " dont exist"
                self._status = False
        except socket.error, msg:
            self._status = False

    def get_local_ip(self):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(('8.8.8.8', 80))
            (addr, port) = s.getsockname()
            s.close()
            return addr
        except socket.error:
            return socket.gethostbyname(socket.gethostname())

    def get_ha_stats(self):
        NewHS = []
        try:
            COMMAND = 'show stat\n'
            # import pdb;pdb.set_trace()
            if self._status:
                Title = []
                HS = []
                self.socket_.send(COMMAND)
                data = self.socket_.recv(self.BufferSize)
                data = data.split("\n")
                for line in data:
                    Status = line.split(',')
                    # For Compatibility older versions
                    if len(Status) < 2:
                        continue
                    if len(Status) > 32: # 当长度大于32的时候才能取到索引为32的值
                        if Status[32] == 'type':
                            Status[0] = Status[0].replace('#', '').strip()
                            Title = Status[0:-1]
                        else:
                            HS.append(Status)
                    else:
                        HS.append(Status)
                for MS in HS:
                    metric = {}
                    if len(Title) <= len(MS):
                        for header in Title:
                            i = Title.index(header)
                            metric[header] = 0 if len(str(MS[i])) == 0 else MS[i]
                    else:
                        for n in range(len(MS)):
                            header = Title[n]
                            metric[header] = 0 if len(str(MS[n])) == 0 else MS[n]
                    NewHS.append(metric)
                # for MS in HS:
                #     metric = {}
                #     for header in Title:
                #         i = Title.index(header)  # 这里都无法确定MS与Title长度的大小直接取值程序会报错
                #         metric[header] = 0 if len(str(MS[i])) == 0 else MS[i]
                #     NewHS.append(metric)

            return NewHS
        except Exception, msg:
            return NewHS

    def getMetric(self):
        UploadMetric = []
        upload_ts = int(time.time())
        self.connect()
        if self._status:
            MyStats = self.get_ha_stats()
            if MyStats:
                StatusCnt = 0
                for MS in MyStats:
                    Tag = 'pxname=' + MS['pxname'] + ',svname=' + MS['svname']
                    for key, value in MS.iteritems():
                        if key not in self.Metrics:
                            continue
                        MetricName = self.MetricPrefix + key
                        if key == 'status':
                            if value == 'DOWN':
                                MetricValue = 1
                            else:
                                MetricValue = 0
                        else:
                            MetricValue = value
                        UploadMetric.append(
                            {"endpoint": self.EndpointName, "metric": MetricName, "tags": Tag, "timestamp": upload_ts,
                             "value": MetricValue, "step": 60, "counterType": "GAUGE"})
                getStatsFile = 0
            else:
                getStatsFile = 1
        else:
            getStatsFile = 2
        UploadMetric.append({"endpoint": self.EndpointName, "metric": self.MetricPrefix + 'getstats',
                             "tags": 'filename=' + self.StatsFile, "timestamp": upload_ts, "value": getStatsFile,
                             "step": 60, "counterType": "GAUGE"})
        return UploadMetric

    def sendData(self):
        haproxy_metric = self.getMetric()
        print json.dumps(haproxy_metric)
        # r = requests.post(self.FalconCli, data=json.dumps(haproxy_metric))
        # if self.Debug >= 2:
        #     print "-- Metric info:\n", haproxy_metric, "\n"
        # if self._status and self.Debug >= 1:
        #     print "-- falcon return info:\n", r.text, "\n"


if __name__ == "__main__":
    conf = {
        "debug_level": 2,
        "endpoint_type": "ip",
        "metric_prefix": "haproxy_",
        "metrics": ['qcur', 'scur', 'rate', 'status', 'ereq', 'drep', 'act', 'bck', 'qtime', 'ctime', 'rtime', 'ttime'],
        "stats_file": "/var/run/haproxy.sock",
        "buffer_size": 8192,
    }
    hs = HaproxyStats(conf)
    hs.sendData()
