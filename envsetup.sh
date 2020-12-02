#!/bin/bash
##########################################
#Created on Wed Dec 02 01:17:08 2020
#
#@author: Jagadeesh
###########################################

# Immediately exit if any command exists with a non-zero status
set -o errexit

echo "Hadoop and Spark configuration"
echo "Installing dependencies for Spark"
echo "Installing Maven for buliding dependencies"
echo "Configuring /opt/spark/conf/core-site.xml for connecting to IBM COS"

echo "(access_key_id,secret_access_key)IBM Programmatic access for accessing IBM resources"

ACCESS_KEY_ID='<yourIBMaccessKey>'
SECRET_ACCESS_KEY='<yourIBMSecretKey>'

echo "Installing java,git,maven"

yum update -y 
yum install java-1.8.0-openjdk -y
yum install git -y
yum install wget -y
yum install maven -y


wget https://archive.apache.org/dist/maven/maven-3/3.2.5/binaries/apache-maven-3.2.5-bin.tar.gz -P ./mvn
sleep 10
tar xvzf ./mvn/apache-maven-3.2.5-bin.tar.gz -C /opt
sleep 10
echo "Creating Soft link for Maven"
ln -s /opt/apache-maven-3.2.5 /opt/maven

echo "Configuring Maven profile file"
sleep 10
echo '''export JAVA_HOME=/usr/lib/jvm/jre-openjdk
export M2_HOME=/opt/maven
export MAVEN_HOME=/opt/maven
export PATH=${M2_HOME}/bin:${PATH}''' > /etc/profile.d/maven.sh

wget https://downloads.apache.org/spark/spark-3.0.1/spark-3.0.1-bin-hadoop2.7.tgz -P ./spark
sleep 10
tar xvf ./spark/spark-3.0.1-bin-hadoop2.7.tgz -C ./spark
sleep 10
sudo mv ./spark/spark-3.0.1-bin-hadoop2.7 /opt/spark
echo 'export SPARK_HOME=/opt/spark' >> ~/.bashrc
echo 'export PATH=$SPARK_HOME/bin:$PATH' >> ~/.bashrc
echo "Updating Env file"
source ~/.bashrc

echo "Running master spark service"

$SPARK_HOME/sbin/start-master.sh

echo "
<configuration>

<property>
  <name>fs.stocator.scheme.list</name>
  <value>cos</value>
</property>
<property>
  <name>fs.cos.impl</name>
  <value>com.ibm.stocator.fs.ObjectStoreFileSystem</value>
</property>
<property>
  <name>fs.stocator.cos.impl</name>
  <value>com.ibm.stocator.fs.cos.COSAPIClient</value>
</property>
<property>
  <name>fs.stocator.cos.scheme</name>
  <value>cos</value>
</property>

<property>
  <name>fs.cos.myCos.access.key</name>
  <value>${ACCESS_KEY_ID}</value>
</property>
<property>
  <name>fs.cos.myCos.secret.key</name>
  <value>${SECRET_ACCESS_KEY}</value>
</property>pww

<!-- Choose the relevant endpoint from https://cos-service.bluemix.net/endpoints -->
<property>
  <name>fs.cos.myCos.endpoint</name>
  <value>http://s3-api.us-geo.objectstorage.softlayer.net</value>
</property>

<property>
  <name>fs.cos.myCos.v2.signer.type</name>
  <value>false</value>
</property>

<!-- This property auto creates bucket if its not existing -->
<property>
  <name>fs.cos.myCos.create.bucket</name>
  <value>true</value>
</property>

</configuration>
" >> /opt/spark/conf/core-site.xml

chmod 777 /opt/spark/conf/core-site.xml

sleep 5

echo "Installing pyspark"

pip3 install pyspark

sudo ln -s /usr/bin/python3 /usr/bin/python

git clone https://github.com/CODAIT/stocator
sleep 5
cd stocator
echo "Building necessary JAR's"
mvn clean package -P all-in-one
sleep 5

echo "Script Executed successfully"