#!/usr/bin/python

import os;
import sys;
from xml.etree import ElementTree as et;

DN_DIR_PREFIX="/app/hadoop/additionalData/";

if ( not os.path.exists(DN_DIR_PREFIX) ):
   print "$0: DN_DIR_PREFIX is not set. set it to something like /hadooptmp/dn ";
   exit(1);

def create_hdfs_site_conf(dn):
   hdfs_conf_file ="datanode_conf/hdfs-site.xml" 
   tree = et.parse(hdfs_conf_file);
   for prop  in tree.findall("property"):
      propName = prop.find("name").text;
      if propName == 'dfs.datanode.address':
         prop.find("value").text = "0.0.0.0:5001"+dn;
      elif propName == 'dfs.datanode.http.address':
         prop.find("value").text = "0.0.0.0:5008"+dn;
      elif propName == 'dfs.datanode.ipc.address':
         prop.find("value").text = "0.0.0.0:5002"+dn;
         
   tree.write(hdfs_conf_file);

def create_core_site_conf(dn):
   core_conf_file="datanode_conf/core-site.xml"
   tree = et.parse(core_conf_file);
   for prop  in tree.findall("property"):
      propName = prop.find("name").text;
      if propName == 'hadoop.tmp.dir':
         prop.find("value").text = DN_DIR_PREFIX+dn;
         break;
         
   tree.write(core_conf_file);
   
   
def run_datanode(cmd, dn):
    os.environ["HADOOP_LOG_DIR"]=DN_DIR_PREFIX+dn+"/logs";
    os.environ["HADOOP_PID_DIR"]=os.environ["HADOOP_LOG_DIR"];
    #DN_CONF_OPTS="\ 
    #            -Dhadoop.tmp.dir=$DN_DIR_PREFIX$dn\ 
    #            -Ddfs.datanode.address=0.0.0.0:5001$dn \ 
    #            -Ddfs.datanode.http.address=0.0.0.0:5008$dn \ 
    #            -Ddfs.datanode.ipc.address=0.0.0.0:5002$dn";
    #bin/hadoop-daemon.sh --script bin/hdfs $1 datanode $DN_CONF_OPTS 
    create_core_site_conf(dn);
    create_hdfs_site_conf(dn);
    comm = "bin/hadoop-daemon.sh --config datanode_conf "+cmd+" datanode"
    os.system(comm);

cmd=sys.argv[1];
i=2;
for i in range(2,len(sys.argv)):
   dn=int(sys.argv[i]);
   if(dn>0 and dn<10):
      print dn;
      run_datanode(cmd,str(dn));
   else:
      print dn,">9";
#    run_datanode  $cmd $i



