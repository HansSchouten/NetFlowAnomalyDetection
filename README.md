# NetFlow Anomaly Detection
Scalable, real-time anomaly detection on netflow data

## Setup
The following section describes the setup of the system on Windows.

### Dependencies
The system has the following dependencies:
1. Apache ZooKeeper
2. Apache Kafka
3. Apache Flink
4. VerizonDigital vFlow
5. Plixer Flowalyzer
6. IntelliJ IDEA

### Installation
In the installation steps, specific file system paths are specified. It is adviced to follow these, since the provided bat files for running the various systems assume these paths are used.

#### Apache ZooKeeper
To install ZooKeeper, download the [latest version](http://zookeeper.apache.org/releases.html) and unzip the archive in c:\zookeeper.  
Perform the following steps:
1. Rename c:\zookeeper\conf\zoo_sample.cfg to zoo.cfg
2. Open zoo.cfg and replace the line starting with *dataDir* by: *dataDir=c:/zookeeper/zookeeperdata*
3. Update the Windows Environment Variables. Add in System Variables *ZOOKEEPER_HOME = C:\zookeeper*. Edit the System Variable named *Path* and append with *;%ZOOKEEPER_HOME%\bin*;

#### Apache Kafka
To install Kafka, go to the [Kafka Releases](https://kafka.apache.org/downloads) and choose the binary download of Kafka version 0.10.2.1 with Scala version 2.10. It is important to match these settings, as other Kafka or Scala versions are incompatible with the Kafka Connector used in the code provided by this repository.  
Perform the following steps:
1. unzip the downloaded archive into c:\kafka
2. edit c:\kafka\config\server.properties and replace the line starting with *log.dirs* by: *log.dirs=c:/kafka/kafka-logs*

#### Apache Flink
To install Flink, go to the [Flink Releases](https://flink.apache.org/downloads.html) and select Binaries of Flink 1.2.0. Next, download the archive of Flink 1.2.0 with Hadoop 27 and Scala 2.10. If you use another Flink or Scala version, the used Flink Connector used in this repository will not work.  
Perform the following steps:
1. unzip the archive in c:\flink
2. edit c:\flink\conf\zoo.cfg and replace the line starting with *dataDir* by: *dataDir=c:/flink/zookeeper*

#### vFlow
vFlow is used to parse incoming NetFlows, convert them into JSON objects and insert them in a Kafka Topic.  
To instal vflow, go the [vFlow Releases](https://github.com/VerizonDigital/vflow/releases) and download the latest windows binary version.  
Perform the following steps:
1. Place the binary in c:\vflow
2. Create c:\vflow\vflow.conf and add the line: *stats-http-port: 80*

#### Flowalyzer
Flowalyzer is used for generating NetFlows.  
To install Flowalyzer, go to the [Flowalyzer download page](https://www.plixer.com/products/flowalyzer/), fill in your details and hit download now.

#### IntelliJ
IntelliJ is the recommended editor for compiling and executing Flink Jobs. Eclipse is known to have issues with mixed Scala and Java projects.  
Download and install IntelliJ from [IntelliJ Releases](https://www.jetbrains.com/idea/download/#section=windows).
Check the option to also install Maven, since this is used for running the code.  
Import the project by following these steps:
1. Start IntelliJ IDEA and choose "Import Project"
2. Select the root folder of the Flink repository
3. Choose "Import project from external model" and select "Maven"
4. Leave the default options and click on "Next" until you hit the SDK section.
5. If there is no SDK, create a one with the "+" sign top left, then click "JDK", select your JDK home directory and click "OK". Otherwise simply select your SDK.
6. Continue by clicking "Next" again and finish the import.

Create a IntelliJ Build Artifact by:
1. Go to *Build* and then to *Build Artifacts* and then to *Edit*
2. Add a new Build Artifact
3. Output the jar in: c:\flink\jars
4. Choose as main class: *org.tudelft.flink.streaming.heavyhitters.KafkaHeavyHitters*

Create a Maven Run configuration by:
1. Go to *Run* and then to *Edit Configurations*
2. Add a new Maven configuration
3. Specify the following command line argument: *exec:java -Dexec.mainClass=org.tudelft.flink.streaming.heavyhitters.KafkaHeavyHitters "-Dexec.args=--topic vflow.netflow9 --bootstrap.servers localhost:9092 --zookeeper.connect localhost:2181 --group.id group1"*

### Running the System
To run the system you need to download all .bat files from the bat-files folder in this repository.  
Next, perform the following steps:
1. Run zookeeper.bat
2. Run kafka-server.bat
3. Run flow-to-kafka.bat
4. Run Flowalyzer, switch to the Generator tab and switch to NetFlow v9. Also replace the UDP Port by: 4729, which is the port vflow is listening on
5. Switch to IntelliJ and build the project [Ctrl+F9]
6. Run the Maven run configuration that you prepared in IntelliJ
7. Once the Flink Job is running, you can start generating NetFlows with Flowalyzer
