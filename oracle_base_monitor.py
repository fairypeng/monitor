#coding:utf-8

"""大致思路：从进程中获取oracle实例，保存到一个json文件中，每个实例存在自己的当前时间以及过期时间。假如当前时间大于过期时间，删除该实例，假如当前时间小于过期时间，刷新当前时间以及过期时间，过期时间为2周。假设60s执行的脚本时间超过60s，open-falcon会杀死该进程，因此把表空间的获取脚本新建在一个600S执行一次的脚本中。"""


import os  # 标准库
import time  # 标准库
import cx_Oracle
import json  # 标准库
import sys
import commands
import socket

"""
# 检查实例 : select to_char(case when inst_cnt > 0 then 1 else 0 end,'FM99999999999999990') retvalue from (select count(*) inst_cnt from v$instance where status = 'OPEN' and logins = 'ALLOWED' and database_status = 'ACTIVE')        
# 连接的用户数(活跃的连接数) select to_char(count(*)-1, 'FM99999999999999990') retvalue from v$session where username is not null and status='ACTIVE'
# 用户数据大小，无临时(表空间使用率)  脚本
# 所有的数据文件的大小 select b.file_id , b.tablespace_name , b.bytes/1024/1024, (b.bytes-sum(nvl(a.bytes,0)))/1024/1024,sum(nvl(a.bytes,0))/1024/1024,100 - sum(nvl(a.bytes,0))/(b.bytes)*100 from dba_free_space a,dba_data_files b where a.file_id=b.file_id  and b.tablespace_name = 'GMAMP796' group by b.tablespace_name,b.file_id,b.bytes order by b.file_id;
# 上次存档的日志序列 
# 当前登录  select user from dual;
"""

username = "xxxx"
password = "xxxx"

monitor_keys = [
    ("active_user_count", "GAUGE"),
    ("db_file_size", "GAUGE"),
    ("last_arclog", "GAUGE")]


ip = socket.hostname()


def check_active(cur):
    """检查实例"""
    sql = """select to_char(case when inst_cnt > 0 then 1 else 0 end,'FM99999999999999990') retvalue 
             from (select count(*) inst_cnt from v$instance where status = 'OPEN' and logins = 'ALLOWED' 
             and database_status = 'ACTIVE')"""
    cur.execute(sql)
    res = cur.fetchall()
    if res:
        for i in res:
            try:
                ret = int(i[0])
                return ret
            except Exception:
                return -1
    else:
        return 0


def active_user_count(cur):
    """连接的活跃的用户数"""
    sql = """select to_char(count(*)-1, 'FM99999999999999990') retvalue from v$session 
             where username is not null and status='ACTIVE'"""
    cur.execute(sql)
    res = cur.fetchall()
    if res:
        for i in res:
            try:
                ret = int(i[0])
                return ret
            except Exception:
                return -1
    else:
        return -1


def db_file_size(cur):
    """所有的数据文件的大小"""
    sql = "select to_char(sum(bytes), 'FM99999999999999990') retvalue from dba_data_files"
    cur.execute(sql)
    res = cur.fetchall()
    if res:
        for i in res:
            try:
                ret = int(i[0])
                return ret
            except Exception:
                return -1
    else:
        return -1


def last_arclog(cur):
    """上次存档的日志序列"""
    sql = "select to_char(max(SEQUENCE#), 'FM99999999999999990') retvalue from v$log where archived = 'YES'"
    cur.execute(sql)
    res = cur.fetchall()
    if res:
        for i in res:
            try:
                ret = int(i[0])
                return ret
            except Exception:
                return -1
    else:
        return -1


def find_master_slave(cur):
    """查找主从"""
    sql = "select database_role from v$database"  # 假设是主库会返回PRIMARY
    cur.execute(sql)
    res = cur.fetchall()
    if res:
        for i in res:
            return i[0]
    else:
        return None


def find_slave_status(cur):
    """查找从机同步状态，为0异常"""
    sql = "select count(*) from v$managed_standby where process='MRP0'" 
    cur.execute(sql)
    res = cur.fetchall()
    if res:
        for i in res:
            try:
                ret = int(i[0])
                return ret
            except Exception:
                return -1
    else:
        return -1


def get_delay(cur):
    """监控延时，只有从库才需要此操作"""
    sql = "select * from v$dataguard_stats"
    cur.execute(sql)
    ret_list = cur.fetchall()
    final_ret = {}
    if ret_list:
        for ret in ret_list:
            if ret[0] == 'transport lag':
                if ret[1]:
                    transport = ret[1].split(" ")
                    day = transport[0].replace("+", "")
                    time = transport[1].split(":")
                    transport_lag_time = int(day) * 24 * 60 * 60 + int(time[0]) * 60 * 60 + int(time[1]) * 60 + int(
                        time[2])
                    final_ret["transport_lag"] = transport_lag_time
                else:
                    final_ret["transport_lag"] = -1
            elif ret[0] == 'apply lag':
                if ret[1]:
                    apply = ret[1].split(" ")
                    day = apply[0].replace("+", "")
                    time = apply[1].split(":")
                    apply_lag_time = int(day) * 24 * 60 * 60 + int(time[0]) * 60 * 60 + int(time[1]) * 60 + int(
                        time[2])
                    final_ret["apply_lag"] = apply_lag_time
                else:
                    final_ret["apply_lag"] = -1
    return final_ret  # list


def get_oracle_instance():
    #  ps -ef|grep pmon 可以用来判断进程中的实例
    instance_result = os.popen("ps -ef|grep pmon|grep -v grep|awk  '{print $NF}'|awk -F '_' '{print $NF}'")
    instance_res = instance_result.read()  # popen函数返回的是一个file,所以读取这个file生成string

    instances_list = instance_res.split("\n")
    instance_name_list = []
    for instance in instances_list:
        if instance:
            instance_name_list.append(instance)
    return instance_name_list


def get_monitor_data(cur, i, monitor_key):
    ret = []
    try:
        active_status = check_active(cur)
        if active_status != 1:
            i = {'Metric': 'oracle.active', 'Endpoint': ip, 'Timestamp': int(time.time()), 'Step': 60, 'Value': 0,
                 'CounterType': "GAUGE", 'tags': "instance=%s" % i}
            ret.append(i)
        else:
            ret.append(
                {'Metric': 'oracle.active', 'Endpoint': ip, 'Timestamp': int(time.time()), 'Step': 60,
                 'Value': active_status,
                 'CounterType': "GAUGE", 'tags': "instance=%s" % i})
            value = -1
            for key, m_type in monitor_key:
                if key == "active_user_count":
                    value = active_user_count(cur)
                elif key == "db_file_size":
                    value = db_file_size(cur) / 1024 / 1024
                elif key == "last_arclog":
                    value = last_arclog(cur)
                i_ret = {'Metric': 'oracle.%s' % key, 'Endpoint': ip, 'Timestamp': int(time.time()),
                         'Step': 60, 'Value': value, 'CounterType': m_type, 'tags': "instance=%s" % i}
                ret.append(i_ret)
            slave_or_master = find_master_slave(cur)
            if slave_or_master:
                ret.append({'Metric': 'oracle.slave_or_master', 'Endpoint': ip, 'Timestamp': int(time.time()),
                            'Step': 60, 'Value': 1, 'CounterType': "GAUGE", 'tags': "instance=%s" % i})
                if slave_or_master == "PRIMARY":  # 主库
                    pass
                else:  # 不是主库
                    slave_status = find_slave_status(cur)
                    ret.append({'Metric': 'oracle.slave_status', 'Endpoint': ip, 'Timestamp': int(time.time()),
                                'Step': 60, 'Value': slave_status, 'CounterType': "GAUGE", 'tags': "instance=%s" % i})
                    if slave_status != 0:
                        delay_ret = get_delay(cur)
                        for k in delay_ret.keys():
                            ret.append({'Metric': 'oracle.%s' % k, 'Endpoint': ip, 'Timestamp': int(time.time()),
                                        'Step': 60, 'Value': delay_ret[k], 'CounterType': "GAUGE",
                                        'tags': "instance=%s" % i})
            else:
                ret.append({'Metric': 'oracle.slave_or_master', 'Endpoint': ip, 'Timestamp': int(time.time()),
                            'Step': 60, 'Value': 0, 'CounterType': "GAUGE", 'tags': "instance=%s" % i})
    except Exception as e:
        pass
    return ret


def main():
    instance_list = get_oracle_instance()  # 获取当前oracle的实例
    json_filename = "instance.json"
    if os.path.exists(json_filename):  # 判断文件是否存在
        f = open(json_filename, "r")
        strData = f.read()
        json_data = json.loads(strData)
        f.close()
        if instance_list:  # 进程中获取到的实例
            instances = json_data.keys()
            s_ret = []
            same_instance_list = [l for l in instance_list if l in instances]  # 找出两个列表中相同的实例
            new_instance_list = [i for i in instance_list if i not in same_instance_list]
            new_instances = [i for i in instances if i not in same_instance_list]  # 找出配置文件中去除相同实例部分的
            if same_instance_list is not None:  # 如果相同实例组成的list不为空
                for same_instance in same_instance_list:
                    if json_data[same_instance]["expired_time"] < int(time.time()):
                        del json_data[same_instance]
                    else:
                        try:

                            db = cx_Oracle.connect(
                                '{0}/{1}@{2}/{3}'.format(username, password, "127.0.0.1", same_instance))

                            cur = db.cursor()
                            res = get_monitor_data(cur, same_instance, monitor_keys)
                            s_ret.append(res)
                            cur.close()
                            db.close()
                        except Exception as e:

                            s_ret.append(
                                [{'Metric': 'oracle.active', 'Endpoint': ip, 'Timestamp': int(time.time()), 'Step': 60,
                                  'Value': 0, 'CounterType': "GAUGE", 'tags': "instance=%s" % same_instance}])
                        json_data[same_instance] = {"current_time": int(time.time()),
                                                    "expired_time": int(time.time()) + 2 * 7 * 24 * 60 * 60}
                if new_instance_list:
                    for new_instance in new_instance_list:  # 遍历去除相同部分的实例列表
                        try:
                            db = cx_Oracle.connect(
                                '{0}/{1}@{2}/{3}'.format(username, password, "127.0.0.1", new_instance))

                            cur = db.cursor()
                            res = get_monitor_data(cur, new_instance, monitor_keys)
                            s_ret.append(res)
                            cur.close()
                            db.close()
                        except Exception as e:
                            s_ret.append(
                                [{'Metric': 'oracle.active', 'Endpoint': ip, 'Timestamp': int(time.time()), 'Step': 60,
                                  'Value': 0, 'CounterType': "GAUGE", 'tags': "instance=%s" % new_instance}])
                        json_data[new_instance] = {"current_time": int(time.time()),
                                                   "expired_time": int(time.time()) + 2 * 7 * 24 * 60 * 60}
                if new_instances:
                    for ni in new_instances:  # 因为在进程中没有该实例，直接将其的active置位失活，并且不更新过期时间
                        s_ret.append(
                            [{'Metric': 'oracle.active', 'Endpoint': ip, 'Timestamp': int(time.time()), 'Step': 60,
                              'Value': 0, 'CounterType': "GAUGE", 'tags': "instance=%s" % ni}])
            else:  # 如果两个没有相同的实例，分开遍历
                s_ret = []
                for instance in instance_list:  # 遍历进程实例
                    try:
                        db = cx_Oracle.connect('{0}/{1}@{2}/{3}'.format(username, password, "127.0.0.1", instance))

                        cur = db.cursor()
                        res = get_monitor_data(cur, instance, monitor_keys)
                        s_ret.append(res)
                        cur.close()
                        db.close()
                    except Exception as e:
                        s_ret.append(
                            [{'Metric': 'oracle.active', 'Endpoint': ip, 'Timestamp': int(time.time()), 'Step': 60,
                              'Value': 0, 'CounterType': "GAUGE", 'tags': "instance=%s" % instance}])
                    json_data[instance] = {"current_time": int(time.time()),
                                           "expired_time": int(time.time()) + 2 * 7 * 24 * 60 * 60}
                for i in instances:  # 遍历配置文件中的实例，因为配置文件中的实例没有进程无法连接，根据过期时间判断是否失效，失效删除
                    if int(time.time()) > json_data[i]["expired_time"]:
                        del json_data[i]
                    else:
                        s_ret.append(
                            [{'Metric': 'oracle.active', 'Endpoint': ip, 'Timestamp': int(time.time()), 'Step': 60,
                              'Value': 0, 'CounterType': "GAUGE", 'tags': "instance=%s" % i}])
        else:  # 如果进程中没有获取到实例，就只读取配置文件中的实例,未过期就设置为0，过期的删除
            s_ret = []
            for instance in json_data.keys():
                if json_data[instance]["expired_time"] < int(time.time()):  # 该实例已过期
                    del json_data[instance]
                else:
                    s_ret.append([{'Metric': 'oracle.active', 'Endpoint': ip, 'Timestamp': int(time.time()), 'Step': 60,
                                   'Value': 0, 'CounterType': "GAUGE", 'tags': "instance=%s" % instance}])
        final_ret = [{'Metric': 'oracle.dead', 'Endpoint': ip, 'Timestamp': int(time.time()), 'Step': 60,
                      'Value': 0, 'CounterType': "GAUGE", 'tags': ""}]
        for s in s_ret:
            for s_ in s:
                final_ret.append(s_)
        print json.dumps(final_ret, sort_keys=True, indent=4, separators=(',', ':'))
        # 在这里写入新的json文件
        str_data = json.dumps(json_data)
        try:
            os.system("echo '" + str_data + "'> " + json_filename)
        except Exception as e:
            pass
    else:  # 不存在,获取从进程中得到的实例
        if instance_list:
            json_data = {}
            s_ret = []
            for instance in instance_list:  # 创建连接
                try:
                    db = cx_Oracle.connect('{0}/{1}@{2}/{3}'.format(username, password, "127.0.0.1", instance))

                    cur = db.cursor()
                    res = get_monitor_data(cur, instance, monitor_keys)
                    s_ret.append(res)
                    cur.close()
                    db.close()
                except Exception as e:
                    s_ret.append([{'Metric': 'oracle.active', 'Endpoint': ip, 'Timestamp': int(time.time()), 'Step': 60,
                                   'Value': 0, 'CounterType': "GAUGE", 'tags': "instance=%s" % instance}])
                json_data[instance] = {"current_time": int(time.time()),
                                       "expired_time": int(time.time()) + 2 * 7 * 24 * 60 * 60}
            final_ret = [{'Metric': 'oracle.dead', 'Endpoint': ip, 'Timestamp': int(time.time()), 'Step': 60,
                          'Value': 0, 'CounterType': "GAUGE", 'tags': ""}]
            for s in s_ret:
                for s_ in s:
                    final_ret.append(s_)
                    # 在这里写入新的json文件
                str_data = json.dumps(json_data)
                try:
                    os.system("echo '" + str_data + "'> " + json_filename)
                except Exception as e:
                    pass
        else:
            final_ret = [{'Metric': 'oracle.dead', 'Endpoint': ip, 'Timestamp': int(time.time()), 'Step': 60,
                          'Value': 0, 'CounterType': "GAUGE", 'tags': ""}]
        print json.dumps(final_ret, sort_keys=True, indent=4, separators=(',', ':'))


if __name__ == '__main__':
    proc = commands.getoutput(' ps -ef|grep %s|grep -v grep|wc -l ' % os.path.basename(sys.argv[0]))
    sys.stdout.flush()
    if int(proc) < 5:
        main()
