#coding:utf-8
"""假设数据库的内存过大获取内存参数的时候存在速度过慢的可能，所以跟基础监控分离"""
import os
import cx_Oracle
import json
import time
import commands
import sys

username = "xxx"
password = "xxx"


def getHostname():
    agent_path = os.getcwd()
    cfg_name = 'cfg.json'
    cfg = os.path.join(agent_path, cfg_name)
    with open(cfg, 'r') as wf:
        content = wf.read()
        json_data = json.loads(content)
        hostname = json_data['hostname']
        if hostname:
            return hostname
        else:
            return os.uname()[1]


ip = getHostname()


def check_active(cur):
    """检查实例"""
    sql = """select to_char(case when inst_cnt > 0 then 1 else 0 end,'FM99999999999999990') retvalue 
             from (select count(*) inst_cnt from v$instance where status = 'OPEN' and logins = 'ALLOWED' 
             and database_status = 'ACTIVE')"""
    cur.execute(sql)
    res = cur.fetchall()
    for i in res:
        return i[0]


def db_size(cur):
    """表空间使用率"""
    sql = """SELECT D.TABLESPACE_NAME,
               TRUE_SPACE AS "total_mb",
               SPACE - NVL(FREE_SPACE, 0) AS "use_mb",
               FREE_SPACE AS "free_mb",
               ROUND(((SPACE - NVL(FREE_SPACE, 0)) / TRUE_SPACE) * 100, 2) AS "use_pct(%)"
          FROM (SELECT TABLESPACE_NAME,
                       ROUND(SUM(BYTES) / (1024 * 1024), 2) AS "SPACE",
                       ROUND(SUM(CASE AUTOEXTENSIBLE
                                   WHEN 'YES' THEN
                                    MAXBYTES
                                   ELSE
                                    BYTES
                                 END) / (1024 * 1024),
                             2) AS "TRUE_SPACE"
                  FROM DBA_DATA_FILES
                 GROUP BY TABLESPACE_NAME) D,
               (SELECT TABLESPACE_NAME,
                       ROUND(SUM(BYTES) / (1024 * 1024), 2) AS "FREE_SPACE"
                  FROM DBA_FREE_SPACE
                 GROUP BY TABLESPACE_NAME) F
         WHERE D.TABLESPACE_NAME = F.TABLESPACE_NAME(+)
           AND D.TABLESPACE_NAME NOT LIKE 'UNDO%'
        UNION ALL
        -- undo tablespace
        select a.tablespace_name,
               round(b.total_mb) "total_mb",
               round(a.use_mb) "use_mb",
               round(c.free_mb) "free_mb",
               round(a.use_mb / b.total_mb * 100, 2) "use_pct(%)"
          from (select tablespace_name, sum(bytes) / 1024 / 1024 use_mb
                  from dba_undo_extents
                 where status in ('UNEXPIRED', 'ACTIVE')
                 group by tablespace_name) a,
               (select tablespace_name, sum(bytes) / 1024 / 1024 total_mb
                  from dba_data_files
                 where tablespace_name like '%UNDO%'
                 group by tablespace_name) b,
               (select tablespace_name, sum(bytes) / 1024 / 1024 free_mb
                  from dba_undo_extents
                 where status in ('EXPIRED')
                 group by tablespace_name) c
         where a.tablespace_name = b.tablespace_name
        -- temp tablespace
        UNION ALL
        SELECT a.tablespace_name,
               a.total_size AS "total_mb",
               b.used_size AS "use_mb",
               b.free_size AS "free_mb",
               round((b.used_size / a.total_size) * 100, 2) "use_pct(%)"
          FROM (SELECT tablespace_name, SUM(bytes) / 1024 / 1024 total_size
                  FROM dba_temp_files
                 GROUP BY tablespace_name) a,
               (SELECT tablespace_name,
                       used_blocks *
                       (select value from v$parameter where name = 'db_block_size') / 1024 / 1024 used_size,
                       free_blocks *
                       (select value from v$parameter where name = 'db_block_size') / 1024 / 1024 free_size
                  FROM gv$sort_segment) b
         WHERE a.tablespace_name = b.tablespace_name
         ORDER BY 5 DESC"""
    cur.execute(sql)
    res = cur.fetchall()
    final_ret = []
    if res:
        for r in res:
            total_dict = {}
            used_dict = {}
            free_dict = {}
            percent_dict = {}
            total_dict["key"] = "total"
            total_dict["tag"] = r[0]
            total_dict["value"] = r[1]
            final_ret.append(total_dict)

            used_dict["key"] = "used"
            used_dict["tag"] = r[0]
            used_dict["value"] = r[2]
            final_ret.append(used_dict)

            free_dict["key"] = "free"
            free_dict["tag"] = r[0]
            free_dict["value"] = r[3]
            final_ret.append(free_dict)

            percent_dict["key"] = "percent"
            percent_dict["tag"] = r[0]
            percent_dict["value"] = r[4]
            final_ret.append(percent_dict)
    return final_ret  # list


def get_oracle_instance():
    #  ps -ef|grep pmon 可以用来判断实例数
    instance_result = os.popen("ps -ef|grep pmon|grep -v grep|awk  '{print $NF}'|awk -F '_' '{print $NF}'")
    instance_res = instance_result.read()  # popen函数返回的是一个file,所以读取这个file生成string

    instances_list = instance_res.split("\n")
    instance_name_list = []
    for instance in instances_list:
        if instance:
            instance_name_list.append(instance)
    return instance_name_list


def main():
    instance_list = get_oracle_instance()
    ret = []
    if instance_list:  # 进程中的实例
        for instance in instance_list:
            try:
                db = cx_Oracle.connect(
                    '{0}/{1}@{2}/{3}'.format(username, password, "127.0.0.1", instance))
                cur = db.cursor()
                active_status = int(check_active(cur))
                if active_status != 1:  # 失活
                    continue
                else:
                    size_ret = db_size(cur)
                    timestamp = int(time.time())
                    for size in size_ret:
                        r = {'Metric': 'oracle.' + size["key"], 'Endpoint': ip, 'Timestamp': timestamp,
                                 'Step': 600, 'Value': size["value"], 'CounterType': "GAUGE",
                                 'tags': "instance=%s,tbl_name=%s" % (instance,size["tag"])}
                        ret.append(r)
            except Exception as e:
                continue
    print json.dumps(ret, sort_keys=True, indent=4, separators=(',', ':'))


if __name__ == '__main__':
    proc = commands.getoutput(' ps -ef|grep %s|grep -v grep|wc -l ' % os.path.basename(sys.argv[0]))
    sys.stdout.flush()
    if int(proc) < 5:
        main()

