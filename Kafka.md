**What is Kafka :** 

* Apache Kafka is an open-source distributed event streaming platform developed by LinkedIn and later donated to the Apache Software Foundation. It is used to handle large-scale real-time data streams efficiently and reliably.   
* Traditional systems often struggle to process such large-scale data efficiently. Kafka solves these problems by providing:   
  - **Real-Time Processing:** Processes live data streams instantly and helps systems respond quickly to events.  
  - **Fault Tolerance:** Replicates data across brokers to prevent data loss during failures.  
  - **Scalability:** Supports horizontal scaling and efficiently handles growing workloads.  
  - **Event-Driven Architecture:** Enables systems to automatically react to events while reducing continuous polling.  
  - **High Throughput:** Processes millions of messages per second with low latency.  
  - **Offset Management:** Consumers can continue reading from saved positions  
* event streaming is the practice of capturing data in real-time from event sources like databases, sensors, mobile devices, cloud services, and software applications in the form of streams of events.   
* storing these event streams durably for later retrieval; manipulating, processing, and reacting to the event streams in real-time as well as retrospectively; and routing the event streams to different destination technologies as needed. Event streaming thus ensures a continuous flow and interpretation of data so that the right information is at the right place.  
* **Core Components :** 

  - ### Kafka Broker : 

    * A Kafka broker is a server responsible for storing and managing data.  
    * Store topic partitions  
    * Handle producer and consumer requests  
    * Support scalability and fault tolerance  
    * Work together in a Kafka cluster

  - ### Producers

    * Producers are applications or services that send data to Kafka topics.

  - ### Kafka Topic

    * A topic in Kafka is a category or feed where messages are stored.  
    * Producers send messages to specific topics.  
    * Consumers subscribe to topics to read messages.  
    * Every Kafka message is associated with a topic.  
    * Topics are divided into partitions for better scalability.  
    * Partitions help Kafka process large volumes of data efficiently.  
      

  - ### Consumers and Consumer Groups

    * Consumers are applications that read messages from Kafka topics.  
    * Distribute workload.  
    * Process messages in parallel.  
    * Ensure each message is processed only once within a group. 

  - ### Zookeeper

    * Apache ZooKeeper helps manage Kafka clusters.   
    * Broker coordination  
    * Metadata management  
    * Leader election  
    * Cluster synchronization  
    * Failure recovery.  
* **Topic partition**: Kafka topics are divided into a number of partitions, which allows you to split data across multiple brokers.  
* **Consumer Group**: A consumer group is a collection of consumers reading from the same topic.  
* **Node**: A node refers to an individual server or machine inside a Kafka cluster.  
* **Replicas:** A replica of a partition is a "backup" of a partition. Replicas never read or write data. They are used to prevent data loss.

* ## **Workflow of Apache Kafka**

  - ### Step 1: Producers Send Data

    * Producers create and send data to Kafka topics.  
    * Kafka divides data into partitions for efficient processing.

  - ### Step 2: Kafka Stores the Data

    * Kafka stores messages inside topics for a configured time period.  
    * Messages are not deleted immediately after being read.

  - ###  Step 3 : Consumers Read the Data

  - ### Step 4: Kafka Balances the Load

    * ZooKeeper manages broker coordination and failure handling.  
    * Kafka distributes partitions across brokers for scalability.  
    * If a broker fails, Kafka switches to replica brokers automatically.

  - ### Step 5: Data is Processed and Used

* ## **Use event streaming for?**

  - To process payments and financial transactions in real-time, such as in stock exchanges, banks, and insurances.  
  - To track and monitor cars, trucks, fleets, and shipments in real-time, such as in logistics and the automotive industry.  
  - To monitor patients in hospital care and predict changes in condition to ensure timely treatment in emergencies.

    

