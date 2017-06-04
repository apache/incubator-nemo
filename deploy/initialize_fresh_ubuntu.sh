#!/bin/bash
# My hostname
sudo sh -c 'echo 127.0.0.1 `hostname` >> /etc/hosts'

# SSH, Rsync, PSSH
sudo apt-get update
sudo apt-get install -y ssh rsync python-pip
sudo pip install pssh

# SSH key
ssh-keygen -t dsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys

# Install Git
sudo apt-get install -y git

# Install Java
sudo apt-get install -y python-software-properties debconf-utils
sudo add-apt-repository -y ppa:webupd8team/java
sudo apt-get update
echo "oracle-java8-installer shared/accepted-oracle-license-v1-1 select true" | sudo debconf-set-selections
sudo apt-get install -y oracle-java8-installer
export JAVA_HOME=/usr/lib/jvm/java-8-oracle/
echo "export JAVA_HOME=$JAVA_HOME" >> ~/.profile

# Install Hadoop
cd /tmp
wget http://apache.mesi.com.ar/hadoop/common/hadoop-2.7.2/hadoop-2.7.2.tar.gz
tar -xzvf hadoop-2.7.2.tar.gz
mv hadoop-2.7.2 ~/hadoop

export HADOOP_HOME=/home/ubuntu/hadoop
export YARN_HOME=$HADOOP_HOME
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
echo "export HADOOP_HOME=$HADOOP_HOME" >> ~/.profile
echo "export YARN_HOME=\$HADOOP_HOME" >> ~/.profile
echo "export YARN_CONF_DIR=\$HADOOP_HOME/etc/hadoop" >> ~/.profile
echo "export PATH=\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$PATH" >> ~/.profile
echo "export LC_ALL=en_US.UTF-8" >> ~/.profile
echo "export LANG=en_US.UTF-8" >> ~/.profile

# Install Maven
sudo apt-get install -y maven

# Wrap Up
source ~/.profile
