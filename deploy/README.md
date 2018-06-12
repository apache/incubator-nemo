# Nemo-Hadoop(YARN/HDFS) Cluster Deployment Guide

## Install the operating system
* Ubuntu 14.04.4 LTS
* Other OS/versions not tested

## Initialize the fresh OS
* Run `initialize_fresh_ubuntu.sh` on each node

## Set hostnames
* Assign a name to each node (v-m, v-w1, v-w2, ...)
* Set `/etc/hosts` accordingly on v-m and `pscp` the file to all v-w
* Set `hostname` of each node using `set_hostname.sh`

## Set up YARN/HDFS cluster
* Set up the conf files(core-site.xml, hdfs-site.xml, yarn-site.xml, slaves, ...) on v-m, and `pscp` them to all v-w
* hdfs namenode -format (does not always cleanly format... may need to delete the hdfs directory)
* start-yarn.sh && start-dfs.sh
* For more information, refer to the official Hadoop website

## Viewing the YARN/HDFS Web UI (when the nodes can't be directly accessed from the internet)
* This is the case for our cmslab cluster
* Use ssh tunneling: `ssh -D 8123 -f -C -q -N johnyangk@cmscluster.snu.ac.kr`
* Turn on SOCKS proxy in chrome(web browser) advanced settings
* Set your mac's `/etc/hosts`
* Go to `v-m:8088` and `v-m:50070`

## Run Nemo in the cluster
* git clone Nemo on v-m and install
* Upload a local input file to HDFS with `hdfs -put`
* Launch a Nemo job with `-deploy_mode yarn`, and hdfs paths as the input/output
* Example: `./bin/run.sh -deploy_mode yarn -job_id mr -user_main edu.snu.nemo.examples.beam.WordCount -user_args "hdfs://v-m:9000/sample_input_mr hdfs://v-m:9000/sample_output_mr"`

## And you're all set.....?
* I hope so
* But the chances are that you'll run into problems that are not covered in this guide
* When you resolve the problems(with your friend Google), please share what you've learned by updating this README
* Final tip: Learn how to use `pssh`, and you'll be able to maintain a n-node cluster as if you're maintaining a single server
* A great tutorial on pssh: https://www.tecmint.com/execute-commands-on-multiple-linux-servers-using-pssh/

## Some Example Commands for copying files
```bash
# miss any of these and you'll have a very intersting(?) YARN/HDFS cluster
pscp -h ~/parallel/hostfile /etc/hosts /etc/hosts
pscp -h ~/parallel/hostfile /home/ubuntu/hadoop/etc/hadoop/core-site.xml /home/ubuntu/hadoop/etc/hadoop/core-site.xml
pscp -h ~/parallel/hostfile /home/ubuntu/hadoop/etc/hadoop/yarn-site.xml /home/ubuntu/hadoop/etc/hadoop/yarn-site.xml
```

## Some Example Commands for querying cluster status
```bash
# sanity check
pssh -i -h ~/parallel/hostfile 'echo $HADOOP_HOME'
pssh -i -h ~/parallel/hostfile 'echo $JAVA_HOME'
pssh -i -h ~/parallel/hostfile 'yarn classpath'

# any zombies? things running alright?
pssh -i -h ~/parallel/hostfile 'jps'
pssh -i -h ~/parallel/hostfile 'ps'

# resource usage
htop
```

## Misc
```bash
# When HDFS is being weird... run the following commands in order to hard-format
pssh -i -h ~/parallel/hostfile 'rm -rf /home/ubuntu/hadoop/dfs'
hdfs namenode -format
```

