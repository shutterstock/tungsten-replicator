#!/usr/bin/python
# Tungsten node controller sample

import socket, time, sys, marshal, os, psycopg2

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


import ConfigParser
import pprint


response = """HTTP/1.1 200 OK
Content-Type: application/x-python-marshal
Content-Length: %(len)s

%(data)s"""

from subprocess import Popen, PIPE

def runcmd(command):
    p = Popen(command, shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE, close_fds=True)
    out, err = p.communicate('')
    if p.returncode:
        raise RuntimeError, out + err
    return out

def runcmd_err(command):
    p = Popen(command, shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE, close_fds=True)
    out, err = p.communicate('')
    if p.returncode:
        raise RuntimeError, out + err
    return err

# pgq and londiste configuration file templates

provider_ini_template = """\
# Don't edit
# tungsten generated configfile
[pgqadm]
node_name = %(nodename)s
job_name = %(nodename)s_ticker
db = %(connect_string)s

maint_delay = 600

loop_delay = 0.1
logfile = %(base_dir)s/log/%%(job_name)s.log
pidfile = %(base_dir)s/pid/%%(job_name)s.pid

[londiste]
node_name = %(nodename)s
job_name = %(nodename)s_to_any

provider_db = %(connect_string)s
subscriber_db = master-only conf, no subscriber

pgq_queue_name = %(nodename)s_evq

logfile = %(base_dir)s/log/%%(job_name)s.log
pidfile = %(base_dir)s/pid/%%(job_name)s.pid

pgq_lazy_fetch = 500

"""

subscriber_ini_template = """\
# Don't edit
# tungsten generated configfile
[londiste]
node_name = %(nodename)s
job_name = %(provider_nodename)s_to_%(nodename)s

provider_db = %(provider_connect_string)s
subscriber_db = %(connect_string)s

pgq_queue_name = %(provider_nodename)s_evq

logfile = %(base_dir)s/log/%%(job_name)s.log
pidfile = %(base_dir)s/pid/%%(job_name)s.pid

pgq_lazy_fetch = 1000
"""
#/pgq and londiste configuration file templates

def config_as_dict(config):
    return dict(
        [(section, dict([(option, cluster_config.get(section,option))
                         for option
                         in cluster_config.options(section)]))
         for section
         in cluster_config.sections()])

class ClusterNode:
    def __init__(self,  base_dir):
        "loads cluster config file"
        self.base_dir = os.path.abspath(base_dir)
        self.cluster_config_filename = os.path.join(base_dir,'conf','tplugin_cluster.ini')
        self.cluster_config = ConfigParser.ConfigParser()
        # load cluster conf
        self.cluster_config.read(self.cluster_config_filename)
        
    def load_local_config(self):
        "loads local config file if one exists"
        self.nodename = self.cluster_config.get('local_node','nodename')
        self.nodestate = self.cluster_config.get('local_node','nodestate')
        self.noderole = self.cluster_config.get('local_node','noderole')
        if self.nodestate == 'UNCONFIGURED' or self.noderole == 'STANDBY':
            return None
        if self._get_local_config_filename():
            self.local_config = ConfigParser.ConfigParser()
            self.local_config.read(self.local_config_filename)
            return self.local_config_filename, config_as_dict(self.local_config)

    def _get_local_config_filename(self):
        self.nodename = self.cluster_config.get('local_node','nodename')
        self.nodestate = self.cluster_config.get('local_node','nodestate')
        self.noderole = self.cluster_config.get('local_node','noderole')
        if self.noderole == 'STANDBY':
            return None
        if self.noderole == 'MASTER':
            _local_config_filename = 'tplugin_%s_to_any.ini' % self.nodename
        elif self.noderole == 'SLAVE':
            self.provider_node = self.cluster_config.get('londiste_cluster','provider_node')
            _local_config_filename = 'tplugin_%s_to_%s.ini' % (self.provider_node, self.nodename)
        self.local_config_filename = os.path.join(self.base_dir, 'conf', _local_config_filename)
        return 'OK'
    
    def _persist(self):
        self.cluster_config.write(open(self.cluster_config_filename,'w'))
        if self.cluster_config.get('local_node','nodestate') <> 'UNCONFIGURED':
            self.local_config.write(open(self.local_config_filename,'w'))
        
    def prepare_localnode(self, base_dir, nodename, inifile=''):
        """\
        Prepares a node
          * creates neccessary directories
          * saves initial conf file
          * sets [local_node] section to
            - role = STANDBY
            - node_state = UNCONFIGURED
            - nodename = lostest_001
        """
        paths = [os.path.join(base_dir,sdir) for sdir in ('conf','log','pid')]
        for path in paths:
            if not os.path.exists(path):
                os.makedirs(path)
        cluste_ini_path = os.path.join(paths[0], 'tplugin_cluster.ini')
        open(cluste_ini_path,'w').write(inifile)
        self.cluster_config = ConfigParser.ConfigParser()
        self.cluster_config.read(cluste_ini_path)
        if not self.cluster_config.has_section('local_node'):
            self.cluster_config.add_section('local_node')
        self.cluster_config.set('local_node', 'nodename',  nodename)
        self.cluster_config.set('local_node', 'noderole',  'STANDBY')
        self.cluster_config.set('local_node', 'nodestate', 'UNCONFIGURED')
        self.cluster_config.write(open(cluste_ini_path,'w'))
        return 'OK'
    
    def add_node(self, nodename, connect_string, base_dir):
        "adds node to cluster conf"
        if not self.cluster_config.has_section(nodename):
            self.cluster_config.add_section(nodename)
        self.cluster_config.set(nodename, 'connect_string', connect_string)
        self.cluster_config.set(nodename, 'base_dir', base_dir)
        self._persist()
        return 'OK'
        
    def remove_node(self, nodename):
        "remove node from cluster conf"
        self.cluster_config.add_section(nodename)
        self._persist()
        return 'OK'
    
    def set_master_node(self, nodename):
        "sets master node for cluster"
        self.cluster_config.set('londiste_cluster','provider_node', nodename)
        self._persist()
        return 'OK'
    
    def _get_tables(self):
        provider_nodename = self.cluster_config.get('londiste_cluster','provider_node')
        provider_connect_string = self.cluster_config.get(provider_nodename, 'connect_string')
        table_mask = self.cluster_config.get('londiste_cluster','table_mask')
        table_mask_op = self.cluster_config.get('londiste_cluster','table_mask_op')
        try:
            con = psycopg2.connect(provider_connect_string)
        except:
            print 'WARN: unable to get table list from provider DB, using local ones'
            # TODO: shouldn't we always use local DB instead of provider's?
            nodename = self.cluster_config.get('local_node','nodename')                                                             
            connect_string = self.cluster_config.get(self.nodename, 'connect_string')
            con = psycopg2.connect(connect_string)
        query = """
        SELECT schemaname ||'.'|| relname
          FROM pg_stat_user_tables
         WHERE schemaname NOT IN('pgq','londiste')
           AND schemaname ||'.'|| relname %s '%s';
        """ % (table_mask_op, table_mask)
        cur = con.cursor()
        cur.execute(query)
        tables = [row[0] for row in cur.fetchall()]
        return tables
    
    def _psql_args_from_connstring(self, connect_string):
        connect_data = dict([item.split('=') for item in connect_string.split()])
        keymap = {'host':'-h', 'user':'-U', 'port':'-p'}
        dbname = None
        password = None
        connopts = []
        for key in connect_data.keys():
            if key == 'dbname':
                dbname = connect_data[key]
                continue
            if key == 'password':
                password = connect_data[key]
                continue
            connopts.append('%s %s' % (keymap[key], connect_data[key]))
        return dbname, password, connopts
            
    
    def _create_database(self):
        self.nodename = self.cluster_config.get('local_node','nodename')
        self.connect_string = self.cluster_config.get(self.nodename, 'connect_string')
        connect_data = dict([item.split('=') for item in self.connect_string.split()])
        local_dbname = connect_data['dbname']
        connect_data['dbname'] = 'postgres'
        pgdb_connect_string = ' '.join(['%s=%s' % (key, value) for (key, value) in connect_data.items()])
        con = psycopg2.connect(pgdb_connect_string)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()
        try:
            cur.execute('create database %s' % local_dbname)
        except psycopg2.ProgrammingError:
            return 'ERROR', 'database %s already exists' % local_dbname
        # copy tables
        tablist = ''
        if self._get_tables():
            tablist = ' -t '.join(self._get_tables())

        # get dest connection info
        print self.connect_string
        dbname, password, connopts = self._psql_args_from_connstring(self.connect_string)
        if password:
            dstpwd = 'PGPASSWORD=%s; ' % password
        else:
            dstpwd = ''
        dstconn = ' '.join(connopts) + ' ' + dbname
        # get src connection info
        provider_nodename = self.cluster_config.get('londiste_cluster','provider_node')
        provider_connect_string = self.cluster_config.get(provider_nodename, 'connect_string')
        dbname, password, connopts = self._psql_args_from_connstring(provider_connect_string)
        print provider_connect_string
        print (dbname, password, connopts)
        if password:
            srcpwd = 'PGPASSWORD=%s; ' % password
        else:
            srcpwd = ''
        srcconn = ' '.join(connopts) + ' ' + dbname
        # create command
        print 'Copying custom schemas from master to slave...'                                                                                                                                    
        sch = '%(srcpwd)s pg_dump %(srcconn)s -s -N pgq -N londiste -T "*.*" | (%(dstpwd)s psql %(dstconn)s)' % locals()                                                                          
        print sch                                                                                                                                                                                 
        schema_dump_result = runcmd(sch) 
        print 'Copying table schemas from master to slave...'
        cmd = '%(srcpwd)s pg_dump %(srcconn)s -s -t %(tablist)s | (%(dstpwd)s psql %(dstconn)s)' % locals()
        print cmd
        table_dump_result = runcmd(cmd)
        # clean up tables in temp database
#        cur.execute('create database %s_tmp' % local_dbname)
#        cmd = '%(dstpwd)s psql %(dstconn)s_tmp -c 'DROP SCHEMA pgq CASCADE'
#        print cmd
#        cur.execute('drop database %s_tmp' % local_dbname)
        return 'OK'

    def setrole(self, role, uri):
        "sets role of local node"
        current_role = self.cluster_config.get('local_node', 'noderole')
        if role == current_role:
            return 'OK'
        
        # Switch/failover?
        switch = False
        if current_role == 'SLAVE' and role == 'MASTER' or current_role == 'MASTER' and role == 'SLAVE':
    	    switch = True
    	    print 'Performing role switch: ' + current_role + ' -> ' + role
    	    # Must go STANDBY for cleanup before switching roles.
    	    self.setrole('STANDBY', None)
    	    current_role = self.cluster_config.get('local_node', 'noderole')
    	    
    	print 'Londiste role change: ' + current_role + ' -> ' + role
    
        if current_role == 'STANDBY':
            self.cluster_config.set('local_node', 'noderole', role)
            self._get_local_config_filename()
            nodename = self.cluster_config.get('local_node', 'nodename')
            try:
                connect_string = self.cluster_config.get(nodename, 'connect_string')
                base_dir = self.cluster_config.get(nodename, 'base_dir')
            except ConfigParser.NoOptionError:
                return 'ERROR', 'no data for node %s' % nodename
            if role == 'MASTER':
                # Ensure that we don't point to some obsolete master (a must in case of switch/failover).
                self.set_master_node(nodename)
                # Generate a config file for the master.
                filedata = provider_ini_template % locals()
            elif role == 'SLAVE':
                try:
                    provider_nodename = self.cluster_config.get('londiste_cluster','provider_node')
                except ConfigParser.NoOptionError:
                    return 'ERROR', 'provider node is not known'
                try:
                    if uri:
                        provider_connect_string = uri
                        # Parse host out of URI if needed. 
                        if uri.startswith('wal://'):
                    	    # TODO: move out to a separate method.
                    	    host_start_i = 6
                    	    host_end_i = uri.find('/', host_start_i)
                    	    if host_end_i == -1:
                    		host_end_i = len(uri)
                    		print host_end_i
                    	    master_host = uri[host_start_i:host_end_i]
                    	    provider_connect_string = 'host='+master_host+' '+connect_string
                    	    
                    	    # Master node has changed, remember that.
                    	    self.add_node(master_host, provider_connect_string, base_dir)
                    	    self.set_master_node(master_host)
                    	    provider_nodename = self.cluster_config.get('londiste_cluster','provider_node')
                    	    print 'Set provider node:', provider_nodename
                    	    self._get_local_config_filename()
                    else:
                        provider_connect_string = self.cluster_config.get(provider_nodename, 'connect_string')
                except ConfigParser.NoOptionError:
                    return 'ERROR', 'provider connect_string is not known'
                filedata = subscriber_ini_template % locals()
            open(self.local_config_filename,'w').write(filedata)
    	    print 'Use config file:', os.path.basename(self.local_config_filename)
            self._get_local_config_filename()
            if role == 'MASTER':
                # install pgq, start ticker
                print 'Installing pgq and starting ticker deamon'
                pgq_install_result = runcmd('pgqadm.py %s install' % self.local_config_filename)
                pgq_ticker_result = runcmd('pgqadm.py %s ticker -d' % self.local_config_filename)
                # install londiste, subscribe tables on provider
                print 'Installing provider and adding triggers to tables'
                londiste_install_result = runcmd('londiste.py %s provider install' % self.local_config_filename)
                tables = ' '.join(self._get_tables())
                londiste_subscribe_result = runcmd('londiste.py %s provider add %s' % (self.local_config_filename, tables))
                results = (pgq_install_result,pgq_ticker_result,londiste_install_result,londiste_subscribe_result)
            elif role == 'SLAVE':
                # create slave database, fail if already exists (should tolerate this with more checking on switch/fail-over)
                if not switch:
            	    create_result = self._create_database()
            	    if not create_result == 'OK':
                	print "failed to create slave database: ", create_result
                	return create_result
                # install londiste, but don't start replay, nor subscribe tables on subscriber (responsibilities of online)
                londiste_install_result = runcmd('londiste.py %s subscriber install' % self.local_config_filename)
                #londiste_replay_result = runcmd('londiste.py %s replay -d' % self.local_config_filename)
                #tables = ' '.join(self._get_tables())
                #londiste_subscribe_result = runcmd('londiste.py %s subscriber add %s' % (self.local_config_filename, tables))
                results = (londiste_install_result) #,londiste_replay_result, londiste_subscribe_result)
            self.cluster_config.set('local_node', 'noderole', role)
            self._persist()
            return 'OK'
        if role == 'STANDBY':
            self._get_local_config_filename()
            if current_role == 'MASTER':
                runcmd('pgqadm.py %s --stop' % self.local_config_filename)
            elif current_role == 'SLAVE':
                runcmd('londiste.py %s --stop' % self.local_config_filename)
            # Remove triggers or subscribtions.
            tables = ' '.join(self._get_tables())
            if current_role == 'MASTER':
                try:
                    print 'Removing triggers'
                    londiste_unsubscribe_result = runcmd('londiste.py %s provider remove %s' % (self.local_config_filename, tables))
                except:
                    print 'FAILED'
            elif current_role == 'SLAVE':
                try:
                    print 'Removing table subscribtions'
                    londiste_unsubscribe_result = runcmd('londiste.py %s subscriber remove %s' % (self.local_config_filename, tables))
                except:
                    print 'FAILED'
            
            self.cluster_config.set('local_node', 'noderole', role)
            self._persist()
            return 'OK'

    def flush(self, out_file_path):
        return self.status(out_file_path)

    def offline(self):
        self.nodename = self.cluster_config.get('local_node','nodename')
        self.nodestate = self.cluster_config.get('local_node','nodestate')
        self.noderole = self.cluster_config.get('local_node','noderole')
        self._get_local_config_filename()
        if self.noderole == 'STANDBY':
            self.cluster_config.set('local_node','nodestate','OFFLINE')
            return 'OK'
        elif self.noderole == 'MASTER':
            # should we FORCE ordinary users out
            # REVOKE CONNECT ON %s FROM PUBLIC % 
            # for pif in pids_of_backends_of_non_superusers():
            #   kill %(pid)s
            self.cluster_config.set('local_node','nodestate','OFFLINE')
            return 'OK'
        elif self.noderole == 'SLAVE':
            # stop replay
            runcmd('londiste.py %s --stop' % self.local_config_filename)
            self.cluster_config.set('local_node','nodestate','OFFLINE')
            return 'OK'

    def online(self):
        self.nodename = self.cluster_config.get('local_node','nodename')
        self.nodestate = self.cluster_config.get('local_node','nodestate')
        self.noderole = self.cluster_config.get('local_node','noderole')
        self._get_local_config_filename()
        if self.noderole == 'STANDBY':
            self.cluster_config.set('local_node','nodestate','ONLINE')
            return 'OK'
        elif self.noderole == 'MASTER':
            self.cluster_config.set('local_node','nodestate','ONLINE')
            return 'OK'
        elif self.noderole == 'SLAVE':
            # resume replay
            londiste_replay_result = runcmd('londiste.py %s replay -d' % self.local_config_filename)
            # actually should also do subscribe and wait here
            tables = ' '.join(self._get_tables())
            print 'Subscribing to tables'
            londiste_subscribe_result = runcmd_err('londiste.py %s subscriber add %s' % (self.local_config_filename, tables))
            #print londiste_subscribe_result
            # wait for subscription to complete
            # TODO!
            self.cluster_config.set('local_node','nodestate','ONLINE')
            return 'OK'

    def capabilities(self, out_file_path):
        file = open(out_file_path, "w")
        file.write("""\
roles=master,slave,standby
model=pull
provision=joiner
consistency=false
heartbeat=true
flush=true
""")
        file.close()
        return 'OK'

    def status(self, out_file_path):
        # subscriber info
        nodename = self.cluster_config.get('local_node', 'nodename')
        connect_string = self.cluster_config.get(nodename, 'connect_string')
        
        self.noderole = self.cluster_config.get('local_node','noderole')
        self._get_local_config_filename()
        if self.noderole == 'MASTER':
    	    con = None
    	    try:
	            con = psycopg2.connect(connect_string)
    	    except:
	            print 'ERROR: unable to connect to DB for status'
	            (lag_sec,last_saved_tick_id) = (-2,-2)    	    
    	    if con:
	            cur = con.cursor()
	            cur.execute("select extract('epoch' from lag) as lag_sec,last_tick from pgq.get_consumer_info() limit 1")
	            res = cur.fetchone()
	            if res:
	        	    (lag_sec,last_saved_tick_id) = res
	            else:
	        	    print "WARN: no queues on master (are there any slaves?)"
	        	    return 'OK' # No slaves = no queues, that's fine.
    	                		    
    	    file = open(out_file_path, "w")
    	    errmsg = 'OK'
    	    nodename = self.cluster_config.get('local_node','nodename')
    	    noderole = self.cluster_config.get('local_node','noderole')
    	    status = self.cluster_config.get('local_node','nodestate').lower()
    	    file.write("""
status=%(status)s
string=%(status)s
role=%(noderole)s
errmsg=%(errmsg)s
last-sent=%(last_saved_tick_id)d
applied-latency=%(lag_sec)s
""" % locals())
            file.close()
            if con:
	            return 'OK'
    	    else:
	            return 'OK' # TODO: return a code so replicator would go to OFFLINE:ERROR

        # provider info
        provider_nodename = self.cluster_config.get('londiste_cluster','provider_node')
        provider_queue_name = "%(provider_nodename)s_evq" % locals()
        provider_connect_string = self.cluster_config.get(provider_nodename, 'connect_string')
        # job info        
        jobname = "%(provider_nodename)s_to_%(nodename)s" % locals()

        # queries
        try:
    	    prov_con = psycopg2.connect(provider_connect_string)
    	    #print 'provider_connect_string', provider_connect_string
    	    prov_cur = prov_con.cursor()
    	    prov_cur.execute("select extract('epoch' from lag) as lag_sec,last_tick from pgq.get_consumer_info() where consumer_name=%s", (jobname,))
    	    res = prov_cur.fetchone()
    	    if res:
        	    (lag_sec,last_saved_tick_id) = res
            else:
        	    (lag_sec,last_saved_tick_id) = -1, -1
        except:
            print 'ERROR: connecting to provider DB for status failed'
            (lag_sec,last_saved_tick_id) = -2, -2
        
        subs_con = psycopg2.connect(connect_string)
        #print 'connect_string', connect_string
        subs_cur = subs_con.cursor()
        subs_cur.execute("select last_tick_id from londiste.completed where consumer_id=%s", (jobname,))
        res = subs_cur.fetchone()
        if res:
            (last_processed_tick_id,) = res
        else:
            last_processed_tick_id = -1
        
        #
        file = open(out_file_path, "w")
        nodename = self.cluster_config.get('local_node','nodename')
        noderole = self.cluster_config.get('local_node','noderole')
        status = self.cluster_config.get('local_node','nodestate').lower()
        errmsg = 'OK'
        file.write("""
status=%(status)s
string=%(status)s
role=%(noderole)s
errmsg=%(errmsg)s
last-sent=%(last_saved_tick_id)d
last-applied=%(last_processed_tick_id)d
last-received=%(last_processed_tick_id)d
applied-latency=%(lag_sec)s
""" % locals())
        file.close()
        return 'OK'
    
    def waitevent(self, out_file_path, eventID, timeout):
        # provider info
        provider_nodename = self.cluster_config.get('londiste_cluster','provider_node')
        provider_queue_name = "%(provider_nodename)s_evq" % locals()
        provider_connect_string = self.cluster_config.get(provider_nodename, 'connect_string')
        # subscriber info
        nodename = self.cluster_config.get('local_node', 'nodename')
        connect_string = self.cluster_config.get(nodename, 'connect_string')
        # job info        
        jobname = "%(provider_nodename)s_to_%(nodename)s" % locals()
               
        subs_con = psycopg2.connect(connect_string)
        print 'connect_string', connect_string
        subs_cur = subs_con.cursor()
        t1 = time.time()
        while 1:
            subs_cur.execute("select last_tick_id from londiste.completed where consumer_id=%s", (jobname,))
            res = subs_cur.fetchone()
            if res:
                (last_processed_tick_id,) = res
            else:
                print "No latest processed tick id found on slave", self.noderole
                return 'ERROR'
            if last_processed_tick_id >= eventID:
                break
            t2 = time.time()
            if timeout and ((t2 - t1) > timeout):
                print "Timeout exceeded"
                return "TIMEOUT"
            time.sleep(0.5)
        #
        file = open(out_file_path, "w")
        nodename = self.cluster_config.get('local_node','nodename')
        noderole = self.cluster_config.get('local_node','noderole')
        status = self.cluster_config.get('local_node','nodestate').lower()
        errmsg = 'OK'
        file.write("""
last-applied=%(last_processed_tick_id)d
last-received=%(last_processed_tick_id)d
""" % locals())
        file.close()
        return 'OK'

###############################

cluster_ini = """
[londiste_cluster]
table_mask = %.%
table_mask_op = LIKE
clustername = lostest
"""

import time
logfile = open('/tmp/pg-londiste-plugin.log','a',0)

if __name__ == '__main__':
    if sys.argv[1] == 'test_setup':
        # setup node 1 as master
        node1 = ClusterNode('nodes/lostest_001')
        node1.prepare_localnode('nodes/lostest_001','lostest_001', cluster_ini)
        node1.add_node('lostest_001','dbname=lostest_001','nodes/lostest_001')
        node1.add_node('lostest_002','dbname=lostest_002','nodes/lostest_002')
        node1.add_node('lostest_003','dbname=lostest_003','nodes/lostest_003')
        node1.set_master_node('lostest_001')
        print node1.setrole('MASTER',None)
        # setup node 2 as slave
        node2 = ClusterNode('nodes/lostest_002')
        node2.prepare_localnode('nodes/lostest_002','lostest_002', cluster_ini)
        node2.add_node('lostest_001','dbname=lostest_001','nodes/lostest_001')
        node2.add_node('lostest_002','dbname=lostest_002','nodes/lostest_002')
        node2.add_node('lostest_003','dbname=lostest_003','nodes/lostest_003')
        node2.set_master_node('lostest_001')
        print node2.setrole('SLAVE',None)
        # setup node 3 as slave
        node3 = ClusterNode('nodes/lostest_003')
        node3.prepare_localnode('nodes/lostest_003','lostest_003', cluster_ini)
        node3.add_node('lostest_001','dbname=lostest_001','nodes/lostest_001')
        node3.add_node('lostest_002','dbname=lostest_002','nodes/lostest_002')
        node3.add_node('lostest_003','dbname=lostest_003','nodes/lostest_003')
        node3.set_master_node('lostest_001')
        print node3.setrole('SLAVE',None)
    elif sys.argv[1] == 'server':
        "usage: <prog> server portnr basedir"
        host = ''
        try:
            port = int(sys.argv[2])
        except:
            port = 10080
        server = Server(host, port)
        node = ClusterNode(sys.argv[3])
        server.register('prepare', node.prepare_localnode)
        server.register('add_node', node.add_node)
        server.register('set_master_node', node.set_master_node)
        server.register('setrole', node.setrole)
        server.run()
    else:
        from optparse import OptionParser
        parser = OptionParser()
        parser.add_option("-b", "--basedir", action="store", type="string", dest="base_dir")
        parser.add_option("-c", "--config", action="store", type="string", dest="tungsten_config")
        parser.add_option("-o", "--operation", action="store", type="string", dest="command")
        parser.add_option("-I", "--in-params", action="store", type="string", dest="in_params")
        parser.add_option("-O", "--out-params", action="store", type="string", dest="out_params")
        (options, args) = parser.parse_args()
        
        try:
            logfile.write(repr({'time':time.time(), 'options':options,'args':args, 'config_file':open(options.tungsten_config).read()})+'\n\n')
        except:
            pass
        
        # read base directory from tungsten configuration file
        if options.tungsten_config:
            tungsten_config = ConfigParser.ConfigParser()
            tungsten_config.read(options.tungsten_config)
            base_dir = tungsten_config.get('tungsten','base_directory')
            options.base_dir = base_dir

        node = ClusterNode(options.base_dir)
        result = 'OK'

        if options.command == 'prepare':
            base_dir, nodename = options.in_params.split(":")
            result = node.prepare_localnode(base_dir, nodename, cluster_ini)
        elif options.command == 'add_node':
            nodename, connstring, basedir = options.in_params.split(":")
            connstring = connstring.replace(';',' ')
            result = node.add_node(nodename, connstring, basedir)
        elif options.command == 'flush':
            result = node.flush(options.out_params)
        elif options.command == 'waitevent':
            # Split out arguments from event=NNN:timeout=TTT param string.
            for param in options.in_params.split(";"):
                name, value = param.split("=")
                if name == 'event':
                    eventID = int(value)
                elif name == 'timeout':
                    timeout = int(value)

            result = node.waitevent(options.out_params, eventID, timeout)
        elif options.command == 'offline':
            result = node.offline()
        elif options.command == 'online':
            result = node.online()
        elif options.command == 'set_master_node':
            master_node_name = options.in_params
            result = node.set_master_node(master_node_name)
        elif options.command == 'setrole':
            # Split out arguments from role=<value>;uri=<value> param string.
            role = None
            uri  = None
            for param in options.in_params.split(";"):
                name, value = param.split("=")
                if name == 'role':
                    role = value.upper()
                elif name == 'uri':
                    uri = value
            if role:
                result = node.setrole(role, uri)
            else:
                result = 'ERROR: no role for setrole command'
        elif options.command == 'capabilities':
            result = node.capabilities(options.out_params)
        elif options.command == 'status':
            result = node.status(options.out_params)
        else:
            print "unknown command:", options.command

        if not result == 'OK':
            sys.exit(1)
        
        sys.exit(0)

sample_shellscripts = """
#!/bin/bash

# sample shell script to set up replication using base directory: nodes/lostest_001

## setup node 1
# prepare node ini file, all data, inifiles and logs are stored in subdirectories of base_dir
./pg-londiste-plugin.py -b nodes/node_001 -o prepare -I "nodes/node_001 node_001"
# add node to ini file
./pg-londiste-plugin.py -b nodes/node_001 -o add_node -I "node_001 dbname=lostest_001 nodes/node_001"
# add another node to ini file
./pg-londiste-plugin.py -b nodes/node_001 -o add_node -I "node_002 dbname=lostest_002 nodes/node_002"
# add 3rd node to ini file, demonstrating connect string syntax (';' instead of ' ')
./pg-londiste-plugin.py -b nodes/node_001 -o add_node -I "node_003 dbname=lostest_003;port=5432 nodes/node_003"
# declare node 1 to be master
./pg-londiste-plugin.py -b nodes/node_001 -o set_master_node -I node_001
# set this node to be master
./pg-londiste-plugin.py -b nodes/node_001 -o setrole -I "role=master"

## setup node 2
# prepare node ini file
./pg-londiste-plugin.py -b nodes/node_002 -o prepare -I "nodes/node_002 node_002"
# add node to ini file
./pg-londiste-plugin.py -b nodes/node_002 -o add_node -I "node_001 dbname=lostest_001 nodes/node_001"
# add another node to ini file
./pg-londiste-plugin.py -b nodes/node_002 -o add_node -I "node_002 dbname=lostest_002 nodes/node_002"
# add 3rd node to ini file
./pg-londiste-plugin.py -b nodes/node_002 -o add_node -I "node_003 dbname=lostest_003 nodes/node_003"
# declare node 1 to be master
./pg-londiste-plugin.py -b nodes/node_002 -o set_master_node -I node_001
# set this node to be slave
./pg-londiste-plugin.py -b nodes/node_002 -o setrole -I "role=slave"

# at this point both master and slave node are set up and initial copy has been started

"""
