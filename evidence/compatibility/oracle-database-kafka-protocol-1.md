<!-- fetched 2026-10-13 from https://docs.oracle.com/en/database/oracle/oracle-database/23/adque/Kafka_cient_interface_TEQ.html by sweep-shape-compat.py; text extracted from the provider's own page -->

Kafka APIs for Oracle Transactional Event Queues 
 Previous 
 Next 
 JavaScript must be enabled to correctly display this content
 5 Kafka APIs for Oracle
 Transactional Event Queues
 Oracle Transactional Event Queue (TxEventQ) makes it easy to implement event-based applications. It is also highly integrated with Apache Kafka, an open-source stream-processing software platform developed by LinkedIn and donated to the Apache Software Foundation, written in Scala and Java. Apart from enabling applications that use Kafka APIs to transparently operate on Oracle TxEventQ, Oracle TxEventQ also supports bi-directional information flow between TxEventQ and Kafka, so that changes are available in TxEventQ or Kafka as soon as possible in near-real-time. 
 Apache Kafka Connect is a framework included in Apache Kafka that integrates Kafka with other systems. Oracle TxEventQ will provide standard JMS package and related JDBC, Transaction packages to establish the connection and complete the transactional data flow. Oracle TxEventQ configures standard Kafka JMS connectors to establish interoperability and complete the data flow between the two messaging systems.
 This chapter includes the following topics:
 Apache Kafka Overview 
 Kafka Java Client for Transactional Event Queues 
 Configuring Kafka Java Client for Transactional Event Queues 
 Overview of Kafka Producer Implementation for TxEventQ 
 Overview of Kafka Consumer implementation for TxEventQ 
 Overview of Kafka Admin Implementation for TxEventQ 
 Kafka REST APIs for TxEventQ 
 Kafka Connectors for TxEventQ 
 Monitoring Message Transfer 
 Apache Kafka Overview
 Apache Kafka is a community distributed event streaming platform that is horizontally-scalable and fault-tolerant.
 Kafka is deployed on a cluster of one or more servers. Each Kafka cluster stores streams of records in categories called topics. Each record consists of a key, a value, and a timestamp. The Kafka APIs allow an application to connect to a Kafka cluster and use the Kafka messaging platform.
 Kafka Java Client for Transactional Event Queues
 Oracle Database 21c introduced Kafka application compatibility with the Oracle Database. Oracle Database 23ai provides more refined compatibility for a Kafka application with the Oracle Database. This provides easy migration for Kafka Java applications to Transactional Event Queues (TxEventQ). The Kafka Java APIs can now connect to an Oracle database server and use TxEventQ as a messaging platform.
 Figure 5-1 Kafka Application Integration with Transactional Event Queue
 Description of "Figure 5-1 Kafka Application Integration with Transactional Event Queue" 
 This figure shows the Kafka API library, which contains Oracle specific implementation of Kafka's Java APIs which depends on the kafka-clients-2.8.0.jar file. This implementation internally invokes AQ-JMS APIs which in turn use the JDBC driver to communicate with the Oracle Database.
 Developers can now migrate an existing Java application that uses Kafka to the Oracle database using okafka.jar . This client side library allows Kafka applications to connect to the Oracle Database instead of a Kafka cluster and use TxEventQ's messaging platform transparently.
 Configuring Kafka Java Client for Transactional Event Queues
 Prerequisites
 The following are the prerequisites for configuring and running the Kafka Java client for TxEventQ in an Oracle Database.
 Create a database user.
 Grant the following privileges to the user.
 Note:
 It is preferred in general to assign or grant a specific quota on a tablespace to a database user instead of granting unlimited quota in default tablespace. One can create a table space and use the following command to grant quota on a specific tablespace to a database user.
 ALTER USER user QUOTA UNLIMITED /* or size-clause */ on tablespace_name 
 GRANT EXECUTE on DBMS_AQ to user .
 GRANT EXECUTE on DBMS_AQADM to user .
 GRANT SELECT on GV_$SESSION to user ; 
 GRANT SELECT on V_$SESSION to user ; 
 GRANT SELECT on GV_$INSTANCE to user ; 
 GRANT SELECT on GV_$LISTENER_NETWORK to user ; 
 GRANT SELECT on GV_$PDBS to user ; 
 GRANT SELECT on USER_QUEUE_PARTITION_ASSIGNMENT_TABLE to
 user ; 
 exec DBMS_AQADM.GRANT_PRIV_FOR_RM_PLAN(' user '); 
 Set the correct database configuration parameter to use TxEventQ.
 SET STREAMS_POOL_SIZE=400M 
 Note:
 Set the size appropriately based on your workload. STREAMS_POOL_SIZE cannot be set for Autonomous Database Shared. It is automatically configured.
 Set LOCAL_LISTENER database parameter
 SET LOCAL_LISTENER= (ADDRESS=(PROTOCOL=TCP)(HOST=<HOST_NAME.DOMAIN_NAME/ IP> )(PORT=<PORT NUMBER>)) 
 Note:
 LOCAL_LISTENER is not required to be set for:
 Autonomous Database, and
 Oracle database with only one database instance (that is, for non culster deployments).
 Connection Configuration
 OKafka uses JDBC(thin driver) connection to connect to Oracle Database instance using any one of two security protocols.
 PLAINTEXT
 SSL
 PLAINTEXT : In this protocol a JDBC connection is setup by providing username and password in plain text in ojdbc.prperties file. To use PLAINTEXT protocol user must provide following properties through application.
 security.protocol = "PLAINTEXT"
 bootstrap.servers = "host:port"
 oracle.service.name = "name of the service running on the instance"
 oracle.net.tns_admin = "location of ojdbc.properties file" ojdbc.properties file must have the following properties.
 user(in lowercase)=DatabaseUserName
 password(in lowercase)=Password SSL : With this protocol JDBC driver uses Oracle Wallet to connect to Oracle database. This protocol is typically used to o connect to Oracle Database 23ai instance in Oracle Autonomous cloud. To use this protocol Okafka application must specify following properties.
 security.protocol = "SSL"
 oracle.net.tns_admin = "location containing Oracle Wallet, tnsname.ora and ojdbc.properties file"
 tns.alias = "alias of connection string in tnsnames.ora" Directory location provided in oracle.net.tns_admin property has:
 Oracle Wallet
 tnsnames.ora file
 ojdbc.properties file (optional)
 This depends on how the Oracle Wallet is configured.
 See Also:
 JDBC Thin Connections with a Wallet (mTLS) for more information about how to establish secured JDBC connections
 Kafka Client Interfaces
 Kafka applications mainly use Producer, Consumer, and Admin APIs to communicate with a Kafka cluster. This version of Kafka client for TxEventQ supports only a subset of Apache Kafka 2.8.0's Producer, Consumer and Admin APIs and properties. With the okafka.jar client library, Kafka applications will be able to use the Oracle TxEventQ platform. The okafka.jar library requires JRE 9 or above.
 We first illustrate the use of the Kafka Client APIs by way of simple examples, and later describe more details on the same.
 Kafka API Examples 
 Overview of Kafka Producer Implementation for TxEventQ 
 Overview of Kafka Consumer implementation for TxEventQ 
 Overview of Kafka Admin Implementation for TxEventQ 
 Kafka API Examples
 Example: Creating an Oracle Kafka Topic
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.oracle.okafka.clients.admin.AdminClient;
public class SimpleAdminOKafka {
 public static void main(String[] args) {
 Properties props = new Properties();
 //IP or Host name where Oracle Database 23ai is running and Database Listener's Port
 props.put("bootstrap.servers", "localhost:1521");
 //name of the service running on the database instance
 props.put("oracle.service.name", "freepdb1");
 props.put("security.protocol","PLAINTEXT");
 // location for ojdbc.properties file where user and password properties are saved
 props.put("oracle.net.tns_admin","."); 
 try (Admin admin = AdminClient.create(props)) {
 //Create Topic named TEQ with 10 Partitions.
 CreateTopicsResult result = admin.createTopics(
 Arrays.asList(new NewTopic("TEQ", 10, (short)0)));
 try {
 KafkaFuture<Void> ftr = result.all();
 ftr.get();
 } catch ( InterruptedException | ExecutionException e ) {
 throw new IllegalStateException(e);
 }
 System.out.println("Closing OKafka admin now");
 }
 catch(Exception e)
 {
 System.out.println("Exception while creating topic " + e);
 e.printStackTrace();
 }
 }
}
 Example: Creating a Simple OKafka Consumer
import java.util.Properties;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.oracle.okafka.clients.consumer.KafkaConsumer;
public class SimpleConsumerOKafka {
 // Dummy implementation of ConsumerRebalanceListener interface
 // It only maintains the list of assigned partitions in assignedPartitions list
 static class ConsumerRebalance implements ConsumerRebalanceListener {
 public List<TopicPartition> assignedPartitions = new ArrayList<>();
 @Override
 public synchronized void onPartitionsAssigned(Collection<TopicPartition> partitions) { 
 System.out.println("Newly Assigned Partitions:");
 for (TopicPartition tp :partitions ) {
 System.out.println(tp);
 assignedPartitions.add(tp);
 }
 } 
 @Override
 public synchronized void onPartitionsRevoked(Collection<TopicPartition> partitions) {
 System.out.println("Revoked previously assigned partitions. ");
 for (TopicPartition tp :assignedPartitions ) {
 System.out.println(tp);
 }
 assignedPartitions.clear();
 }
 }
 public static void main(String[] args) {
 //System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "TRACE");
 Properties props = new Properties();
 //IP or Host name where Oracle Database 23ai is running and Database Listener's Port
 props.put("bootstrap.servers", "localhost:1521");
 //name of the service running on the database instance
 props.put("oracle.service.name", "freepdb1");
 props.put("security.protocol","PLAINTEXT");
 // location for ojdbc.properties file where user and password properties are saved
 props.put("oracle.net.tns_admin","."); 
 //Consumer Group Name
 props.put("group.id" , "CG1");
 props.put("enable.auto.commit","false");
 // Maximum number of records fetched in single poll call
 props.put("max.poll.records", 2000);
 props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
 props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
 Consumer<String , String> consumer = new KafkaConsumer<String, String>(props); 
 ConsumerRebalanceListener rebalanceListener = new ConsumerRebalance();
 //Subscribe to a single topic named 'TEQ'.
 consumer.subscribe(Arrays.asList("TEQ"), rebalanceListener);
 int expectedMsgCnt = 40000;
 int msgCnt = 0;
 Instant startTime = Instant.now();
 try {
 while(true) {
 try {
 //Consumes records from the assigned partitions of 'TEQ' topic
 ConsumerRecords <String, String> records = consumer.poll(Duration.ofMillis(10000));
 //Print consumed records
 for (ConsumerRecord<String, String> record : records) 
 {
 System.out.printf("partition = %d, offset = %d, key = %s, value =%s\n ", record.partition(), record.offset(), record.key(), record.value());
 for(Header h: record.headers())
 {
 System.out.println("Header: " +h.toString());
 } 
 }
 //Commit all the consumed records
 if(records != null && records.count() > 0) {
 msgCnt += records.count();
 System.out.println("Committing records " + records.count());
 try {
 consumer.commitSync();
 }catch(Exception e)
 {
 System.out.println("Exception in commit " + e.getMessage());
 continue;
 }
 if(msgCnt >= expectedMsgCnt )
 {
 System.out.println("Received " + msgCnt + " Expected " + expectedMsgCnt +". Exiting Now.");
 break;
 }
 }
 else {
 System.out.println("No Record Fetched. Retrying in 1 second");
 Thread.sleep(1000);
 }
 }catch(Exception e)
 {
 System.out.println("Inner Exception " + e.getMessage());
 throw e;
 } 
 }
 }catch(Exception e)
 {
 System.out.println("Exception from OKafka consumer " + e);
 e.printStackTrace();
 }finally {
 long runDuration = Duration.between(startTime, Instant.now()).toMillis();
 System.out.println("Closing OKafka Consumer. Received "+ msgCnt +" records. Run Duration " + runDuration);
 consumer.close();
 }
 }
}
 Example: Creating a Simple OKafka Producer
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.oracle.okafka.clients.producer.KafkaProducer;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.Future;
public class SimpleProducerOKafka {
 public static void main(String[] args) {
 try {
 Properties props = new Properties();
 //IP or Host name where Oracle Database 23ai is running and Database Listener's Port
 props.put("bootstrap.servers", "localhost:1521");
 //name of the service running on the database instance
 props.put("oracle.service.name", "freepdb1");
 props.put("security.protocol","PLAINTEXT");
 // location for ojdbc.properties file where user and password properties are saved
 props.put("oracle.net.tns_admin","."); 
 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); 
 String baseMsg = "This is a test message ";
 // Creates OKafka Producer
 Producer<String, String> producer = new KafkaProducer<String, String>(props);
 Future<RecordMetadata> lastFuture = null;
 int msgCnt = 40000;
 Instant startTime = Instant.now();
 //Headers, common for all records
 RecordHeader rH1 = new RecordHeader("CLIENT_ID", "FIRST_CLIENT".getBytes());
 RecordHeader rH2 = new RecordHeader("REPLY_TO", "REPLY_TOPIC_NAME".getBytes());
 //Produce 40000 messages into topic named "TEQ".
 for(int i=0;i<msgCnt;i++) {
 ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("TEQ", ""+i, baseMsg + i);
 producerRecord.headers().add(rH1).add(rH2);
 lastFuture =producer.send(producerRecord);
 }
 //Waits until the last message is acknowledged
 lastFuture.get();
 long runTime = Duration.between( startTime, Instant.now()).toMillis();
 System.out.println("Produced "+ msgCnt +" messages. Run Duration " + runTime);
 //Closes the OKafka producer
 producer.close();
 } 
 catch(Exception e)
 {
 System.out.println("Exception in Main " + e );
 e.printStackTrace();
 }
 }
}
 Example: Deleting an Oracle Kafka Topic
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.oracle.okafka.clients.admin.AdminClient;
public class SimpleAdminDeleteTopic {
 public static void main(String[] args) {
 Properties props = new Properties();
 //IP or Host name where Oracle Database 23ai is running and Database Listener's Port
 props.put("bootstrap.servers", "localhost:1521");
 //name of the service running on the database instance
 props.put("oracle.service.name", "freepdb1");
 props.put("security.protocol","PLAINTEXT");
 // location for ojdbc.properties file where user and password properties are saved
 props.put("oracle.net.tns_admin","."); 
 try (Admin admin = AdminClient.create(props)) {
 //Throws Exception if failed to delete the topic. Returns null on successful deletion.
 org.apache.kafka.clients.admin.DeleteTopicsResult delResult =
 admin.deleteTopics(Collections.singletonList("TEQ"));
 Thread.sleep(1000);
 System.out.println("Closing admin now");
 }
 catch(Exception e)
 {
 System.out.println("Exception while creating topic " + e);
 e.printStackTrace();
 }
 }
}
 Example: Transactional OKafka Producer
import org.oracle.okafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.header.internals.RecordHeader;
import java.sql.Connection;
import java.util.Properties;
public class TransactionalProducerOKafka {
 public static void main(String[] args) {
 Producer<String, String> producer = null;
 try {
 Properties props = new Properties();
 // Option 1: Connect to Oracle Database with database username and password
 props.put("security.protocol","PLAINTEXT");
 //IP or Host name where Oracle Database 23ai is running and Database Listener's Port
 props.put("bootstrap.servers", "localhost:1521");
 props.put("oracle.service.name", "freepdb1"); //name of the service running on the database instance
 // location for ojdbc.properties file where user and password properties are saved
 props.put("oracle.net.tns_admin","."); 
 /*
 //Option 2: Connect to Oracle Autonomous Database using Oracle Wallet
 //This option to be used when connecting to Oracle autonomous database instance on OracleCloud
 props.put("security.protocol","SSL");
 // location for Oracle Wallet, tnsnames.ora file and ojdbc.properties file
 props.put("oracle.net.tns_admin","."); 
 props.put("tns.alias","Oracle23ai_high"); 
 */
 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 //Property to create a Transactional Producer
 props.put("oracle.transactional.producer", "true");
 producer = new KafkaProducer<String, String>(props);
 int msgCnt = 100;
 String jsonPayload = "{\"name\":\"Programmer"+msgCnt+"\",\"status\":\"classy\",\"catagory\":\"general\",\"region\":\"north\",\"title\":\"programmer\"}";
 System.out.println(jsonPayload);
 producer.initTransactions();
 Connection conn = ((KafkaProducer<String, String> )producer).getDBConnection();
 String topicName = "TXEQ";
 // Produce 100 records in a transaction and commit.
 try {
 producer.beginTransaction();
 boolean fail = false;
 for( int i=0;i<msgCnt;i++) {
 //Optionally set RecordHeaders
 RecordHeader rH1 = new RecordHeader("CLIENT_ID", "FIRST_CLIENT".getBytes());
 RecordHeader rH2 = new RecordHeader("REPLY_TO", "TXEQ_2".getBytes());
 ProducerRecord<String, String> producerRecord = 
 new ProducerRecord<String, String>(topicName, i+"", jsonPayload);
 producerRecord.headers().add(rH1).add(rH2);
 try {
 processRecord(conn, producerRecord);
 } catch(Exception e) {
 //Retry processRecord or abort the Okafka transaction and close the producer 
 fail = true;
 break;
 }
 producer.send(producerRecord);
 }
 if(fail) // Failed to process the records. Abort Okafka transaction 
 producer.abortTransaction();
 else // Successfully process all the records. Commit OKafka transaction
 producer.commitTransaction();
 System.out.println("Produced 100 messages.");
 }catch( DisconnectException dcE) {
 producer.close();
 }catch (KafkaException e) {
 producer.abortTransaction();
 }
 } 
 catch(Exception e)
 {
 System.out.println("Exception in Main " + e );
 e.printStackTrace();
 }
 finally {
 try {
 if(producer != null)
 producer.close();
 }catch(Exception e) 
 {
 System.out.println("Exception while closing producer " + e);
 e.printStackTrace();
 }
 System.out.println("Producer Closed");
 }
 }
 private static void processRecord(Connection conn, ProducerRecord<String, String> record) throws Exception
 {
 //Application specific logic
 }
} 
 Example: Transactional OKafka Consumer
import java.util.Properties;
import java.sql.Connection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.oracle.okafka.clients.consumer.KafkaConsumer;
public class TransactionalConsumerOKafka {
 // Dummy implementation of ConsumerRebalanceListener interface
 // It only maintains the list of assigned partitions in assignedPartitions list
 static class ConsumerRebalance implements ConsumerRebalanceListener {
 public List<TopicPartition> assignedPartitions = new ArrayList();
 @Override
 public synchronized void onPartitionsAssigned(Collection<TopicPartition> partitions) { 
 System.out.println("Newly Assigned Partitions:");
 for (TopicPartition tp :partitions ) {
 System.out.println(tp);
 assignedPartitions.add(tp);
 }
 } 
 @Override
 public synchronized void onPartitionsRevoked(Collection<TopicPartition> partitions) {
 System.out.println("Revoked previously assigned partitions. ");
 for (TopicPartition tp :assignedPartitions ) {
 System.out.println(tp);
 }
 assignedPartitions.clear();
 }
 }
 public static void main(String[] args) {
 Properties props = new Properties();
 // Option 1: Connect to Oracle Database with database username and password
 props.put("security.protocol","PLAINTEXT");
 //IP or Host name where Oracle Database 23ai is running and Database Listener's Port
 props.put("bootstrap.servers", "localhost:1521");
 props.put("oracle.service.name", "freepdb1"); //name of the service running on the database instance
 // location for ojdbc.properties file where user and password properties are saved
 props.put("oracle.net.tns_admin","."); 
 /*
 //Option 2: Connect to Oracle Autonomous Database using Oracle Wallet
 //This option to be used when connecting to Oracle autonomous database instance on OracleCloud
 props.put("security.protocol","SSL");
 // location for Oracle Wallet, tnsnames.ora file and ojdbc.properties file
 props.put("oracle.net.tns_admin","."); 
 props.put("tns.alias","Oracle23ai_high"); 
 */
 //Consumer Group Name
 props.put("group.id" , "CG1");
 props.put("enable.auto.commit","false");
 // Maximum number of records fetched in single poll call
 props.put("max.poll.records", 10);
 props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
 props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
 Consumer<String , String> consumer = new KafkaConsumer<String, String>(props); 
 ConsumerRebalanceListener rebalanceListener = new ConsumerRebalance();
 consumer.subscribe(Arrays.asList("TXEQ"), rebalanceListener);
 int expectedMsgCnt = 100;
 int msgCnt = 0;
 Connection conn = null;
 boolean fail = false;
 try {
 while(true) {
 try {
 //Consumes records from the assigned partitions of 'TXEQ' topic
 ConsumerRecords <String, String> records = consumer.poll(Duration.ofMillis(10000));
 if (records.count() > 0 )
 {
 conn = ((KafkaConsumer<String, String>)consumer).getDBConnection();
 fail = false;
 for (ConsumerRecord<String, String> record : records) 
 {
 System.out.printf("partition = %d, offset = %d, key = %s, value =%s\n ", record.partition(), record.offset(), record.key(), record.value());
 for(Header h: record.headers())
 {
 System.out.println("Header: " +h.toString());
 } 
 try {
 processRecord(conn, record);
 } catch(Exception e) {
 fail = true;
 break;
 }
 } 
 if(fail){
 conn.rollback();
 }
 else { 
 msgCnt += records.count();
 consumer.commitSync();
 }
 if(msgCnt >= (expectedMsgCnt )) {
 System.out.println("Received " + msgCnt + " Expected " + expectedMsgCnt +". Exiting Now.");
 break;
 }
 }
 else {
 System.out.println("No Record Fetched. Retrying in 1 second");
 Thread.sleep(1000);
 }
 }catch(Exception e)
 {
 System.out.println("Exception while consuming messages: " + e.getMessage());
 throw e;
 } 
 }
 }catch(Exception e)
 {
 System.out.println("Exception from OKafka consumer " + e);
 e.printStackTrace();
 }finally {
 System.out.println("Closing OKafka Consumer. Received "+ msgCnt +" records.");
 consumer.close();
 }
 }
 private static void processRecord(Connection conn, ConsumerRecord<String, String> record) 
 {
 //Application specific logic to process the message
 }
}
 Example: OKafka consume-transform-produce
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.oracle.okafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.header.Header;
import org.oracle.okafka.clients.consumer.KafkaConsumer;
public class TransactionalConsumerProducer {
 static int msgNo =0;
 static PreparedStatement instCStmt = null;
 static PreparedStatement instPStmt = null;
 public static void main(String[] args) {
 Properties commonProps = new Properties();
 Properties cProps = new Properties();
 Properties pProps =new Properties();
 // Option 1: Connect to Oracle Database with database username and password
 commonProps.put("security.protocol","PLAINTEXT");
 //IP or Host name where Oracle Database 23ai is running and Database Listener's Port
 commonProps.put("bootstrap.servers", "localhost:1521");
 commonProps.put("oracle.service.name", "freepdb1"); //name of the service running on the database instance
 // directory location where ojdbc.properties file is stored which contains user and password properties
 commonProps.put("oracle.net.tns_admin","."); 
 /*
 //Option 2: Connect to Oracle Autonomous Database using Oracle Wallet
 //This option to be used when connecting to Oracle autonomous database instance on OracleCloud
 commonProps.put("security.protocol","SSL");
 // location for Oracle Wallet, tnsnames.ora file and ojdbc.properties file
 commonProps.put("oracle.net.tns_admin","."); 
 commonProps.put("tns.alias","Oracle23ai_high"); 
 */
 cProps.putAll(commonProps);
 pProps.putAll(commonProps);
 //Consumer Group Name
 cProps.put("group.id" , "CG1");
 cProps.put("enable.auto.commit","false");
 // Maximum number of records fetched in single poll call
 cProps.put("max.poll.records", 10);
 cProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
 cProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
 pProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 pProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 pProps.put("oracle.transactional.producer", "true");
 Consumer<String , String> consumer = new KafkaConsumer<String, String>(cProps);
 ConsumerRebalanceListener rebalanceListener = new ConsumerRebalance();
 consumer.subscribe(Arrays.asList("TXEQ"), rebalanceListener);
 int expectedMsgCnt = 100;
 int msgCnt = 0;
 Connection conn = null;
 Producer<String, String> producer = null;
 try {
 conn = ((KafkaConsumer<String, String>)consumer).getDBConnection();
 producer = new KafkaProducer<String,String>(pProps, conn);
 producer.initTransactions();
 while(true) {
 try {
 //Consumes records from the assigned partitions of 'TXEQ' topic
 ConsumerRecords <String, String> records = consumer.poll(Duration.ofMillis(10000));
 if(records != null && records.count() > 0) {
 msgCnt += records.count();
 producer.beginTransaction();
 boolean fail =false;
 for (ConsumerRecord<String, String> record : records) {
 ProducerRecord<String,String> pr = null;
 try {
 String outRecord = processConsumerRecord(conn, record);
 pr = new ProducerRecord<String,String>("TXEQ_2", record.key(), outRecord);
 processProducerRecord(conn, pr);
 }catch(Exception e)
 {
 // Stop processing of this batch 
 fail =true;
 break;
 }
 producer.send(pr);
 }
 if(fail) {
 //Abort consumed and produced records along with any DML operations done using connection object.
 //Next consumer.poll will fetch the same records again.
 producer.abortTransaction();
 }
 else {
 //Commit consumed and produced records along with any DML operations done using connection object
 producer.commitTransaction();
 }
 }
 else {
 System.out.println("No Record Fetched. Retrying in 1 second");
 Thread.sleep(1000);
 }
 if(msgCnt >= expectedMsgCnt )
 {
 System.out.println("Received " + msgCnt + " Expected " + expectedMsgCnt +". Exiting Now.");
 break;
 }
 }catch(DisconnectException dcE) {
 System.out.println("Disconnect Exception while committing or aborting records "+ dcE);
 throw dcE;
 }
 catch(KafkaException e)
 {
 System.out.println("Re-triable Exception while committing records "+ e);
 producer.abortTransaction();
 }
 catch(Exception e)
 {
 System.out.println("Exception while processing records " + e.getMessage());
 throw e;
 } 
 }
 }catch(Exception e)
 {
 System.out.println("Exception from OKafka consumer " + e);
 e.printStackTrace();
 }finally {
 System.out.println("Closing OKafka Consumer. Received "+ msgCnt);
 producer.close();
 consumer.close();
 }
 }
 static String processConsumerRecord(Connection conn, ConsumerRecord <String, String> record) throws Exception 
 {
 //Application specific logic to process the record
 System.out.println("Received: " + record.partition() +"," + record.offset() +":" + record.value());
 return record.value();
 }
 static void processProducerRecord(Connection conn, ProducerRecord <String, String> records) throws Exception 
 {
 //Application specific logic to process the record
 }
 static void processRecords(Producer<String,String> porducer, Consumer<String,String> consumer, ConsumerRecords <String, String> records) throws Exception
 {
 Connection conn = ((KafkaProducer<String,String>)porducer).getDBConnection();
 String jsonPayload = null;
 ProducerRecord<String,String> pr = null;
 Future<RecordMetadata> lastFuture = null;
 for (ConsumerRecord<String, String> record : records) 
 {
 msgNo++;
 System.out.println("Processing " + msgNo + " record.value() " + record.value());
 System.out.printf("partition = %d, offset = %d, key = %s, value =%s\n ", record.partition(), record.offset(), record.key(), record.value());
 for(Header h: record.headers())
 {
 System.out.println("Header: " +h.toString());
 } 
 jsonPayload = "{\"name\":\"Programmer"+msgNo+"\",\"status\":\"classy\",\"catagory\":\"general\",\"region\":\"north\",\"title\":\"programmer\"}";
 pr = new ProducerRecord<String,String>("KTOPIC1", record.key(), jsonPayload);
 lastFuture = porducer.send(pr);
 RecordMetadata metadata = lastFuture.get();
 }
 }
 // Dummy implementation of ConsumerRebalanceListener interface
 // It only maintains the list of assigned partitions in assignedPartitions list
 static class ConsumerRebalance implements ConsumerRebalanceListener {
 public List<TopicPartition> assignedPartitions = new ArrayList<TopicPartition>();
 @Override
 public synchronized void onPartitionsAssigned(Collection<TopicPartition> partitions) { 
 System.out.println("Newly Assigned Partitions:");
 for (TopicPartition tp :partitions ) {
 System.out.println(tp);
 assignedPartitions.add(tp);
 }
 } 
 @Override
 public synchronized void onPartitionsRevoked(Collection<TopicPartition> partitions) {
 System.out.println("Revoked previously assigned partitions. ");
 for (TopicPartition tp :assignedPartitions ) {
 System.out.println(tp);
 }
 assignedPartitions.clear();
 }
 }
}
 Note:
 Topics created using the KafkaAdmin interface can be accessed only by KafkaProducer or KafkaConsumer interfaces.
 KafkaProducer can send records to a regular JMS transactional event queue topic/queue.However, KafkaConsumer can only consume records from the topics which are created using the KafkaAdmin interface or using DBMS_AQADM.CREATE_DATABASE_KAFKA_TOPIC procedue.
 Kafka REST APIs for TxEventQ
 The TxEventQ REST APIs allow common operations to produce and consume from topics
 and partitions, and are implemented using Oracle REST Data Services (ORDS) in the Oracle
 Database. Common operations include creating and deleting topics, producing and
 consuming messages, and operational APIs for getting consumer lag on a topic, seeking to
 an offset, among many others. 
 The following three APIs for Kafka allow TxEventQ to co-exist with Kafka deployments, and
 provide the advantages of transactional outbox, JMS messaging and pub/sub in the
 database and high throughput streaming of events to the event queue in the Oracle
 Database. 
 See Also:
 Oracle Transactional Event Queues REST Endpoints for the Oracle REST Data Services API documentation
 Overview of Kafka Producer Implementation for TxEventQ
 Producer APIs allow a Kafka application to publish messages into Oracle Transactional Event Queues (TxEventQ). A Kafka application needs to provide Oracle specific properties: bootstrap.servers , oracle.servicename , and oracle.net.tns_admin . More details about these properties are mentioned in the configuration section. These properties are used to set up the database connection and produce the message into TxEventQ. In the current release, Oracle's implementation of KafkaProducer supports only a subset of the Producer APIs.
 Internally, an Oracle Kafka Producer object encapsulates an AQ JMS producer object
 which is used to publish messages into Oracle TxEventQ. Similar to Apache Kafka
 Producer, each Producer send() call will append a Kafka Record into
 a batch based on its topic and partition. Based on Apache Kafka's internal
 algorithm, a background thread will publish the entire batch to an Oracle
 TxEventQ.
 The following KafkaProducer APIs are supported in Oracle Database 23ai.
 Constructor:
 KafkaProducer : Creates an OKafka producer and internal support objects. Application can use any of the four available constructors to create OKafka producer. Each of the constructor has an overloaded version which takes an Oracle Connection object as argument. When application passes pre-created connection object, OKafka producer will use it to send records to Oracle Transaction Event Queue. Application must set oracle.transactional.producer property to true to be able to use external database connection with OKafka producer.
 Methods:
 send(ProducerRecord) , send(ProducerRecord, Callback) :
 The send method asynchronously publishes a message into TxEventQ. This method returns immediately once a Kafka Record has been stored in the buffer of records waiting to be sent. If the buffer is full, then the send call blocks for a maximum time of max.block.ms . Records will be published into the topic using AQ JMS.
 The method returns a Future<RecordMetadata> , which contains the partition, offset, and publish timestamp of the record. Both the send(ProducerRecord) and send(ProducerRecord, Callback) versions are supported.
 getDBConnection : This method returns the database connection used by this KafkaProducer.OKafka producer property oracle.transactional.producer must be set to true to fetch the database connection using this method.
 close : Closes the producer and frees the memory. It will also close the internal connection to the Oracle Database.
 Classes
 ProducerRecord : A class that represents a message in the Kafka platform. The Kafka API library translates a ProducerRecord into a JMS BytesMessage for the TxEventQ platform.
 RecordMetadata : This contains metadata of the record like topic, partition, offset, timestamp etc. of the Record in the Kafka platform. This is assigned values relevant for TxEventQs. A message id of TxEventQ is converted into an offset of RecordMetadata .
 Callback Interface: A callback function which is executed once a Record is successfully published into a Kafka topic.
 Partitioner Interface: Defines methods which map a Key of the message to a partition number of the topic. A partition number is analogous to a stream id of TxEventQs. 
 Properties
 key.serializer and value.serializer : Converts Key and payload into byte array respectively.
 acks : For Kafka APIs, the only value relevant for the acks property is all . Any other field set by the user is ignored.
 linger.ms : Time in miliseconds for which the sender thread waits before publishing the records in TxEventQ.
 batch.size : Total size of records to be batched in bytes for which the sender thread waits before publishing records in TxEventQ. 
 buffer.memory : Total memory in bytes the accumulator can hold. 
 max.block.ms : If buffer.memory size is full in the accumulator, then wait for max.block.ms amount of time before the send() method can receive an out of memory error.
 retries : This property enables a producer to resend a record in case of transient errors. This value limits the number of retries per batch.
 retry.backoff.ms : The amount of time to wait before attempting to retry a failed request to a given topic partition. This avoids repeatedly sending requests in a tight loop under some failure scenarios.
 bootstrap.servers : IP address and port of a machine where an instance of the database is running.
 enable.idempotence : True of False.The idempotent producer strengthens OKafka's delivery semantics from at least once to exactly once delivery. In particular producer retries will no longer introduce duplicates.
 oracle.transactional.producer : True or False. This property creates a transactional OKafka producer. The transactional producer allows an application to send messages to multiple partitions and topics atomically. It also allows application to perform any dml operation within the same transaction.
 Transactional producer can use getDBConnection() method to fetch the database connection which is being used to send the records to the Oracle's Transactional Event Queue broker. Application can use this connection to perform and DML operation.
 commitTransaction() method will atomically commit the DML operation(s) and send operation(s) performed within the current transaction. Similarly, abortTransaction() will atomically roll-back the DML operation(s) and abort the produced record(s) sent within the current transaction. 
 The transactional producer is not thread safe. Application should manage the concurrent access of the transactional producer. Transactional producer does not get benefit of batching. Each message is sent to Oracle Transactional Event Queue broker in a separate request.
 Overview of Kafka Consumer implementation for TxEventQ
 The Consumer API allows applications to read streams of data from a Transactional
 Event Queue (TxEventQ). Kafka consumer for TxEventQ uses AQ JMS APIs and a JDBC
 driver to consume messages in batches from Oracle TxEventQ. For Oracle Kafka,
 consuming messages from a topic is achieved by dequeuing messages from a
 Transactional Event Queue. 
 Similar to Apache Kafka, in TxEventQ's implementation, a consumer group (subscriber) may contains many consumer instances (unique database sessions that are consuming for the subscriber). Each consumer group has a unique group-id (subscriber name). Each consumer instance internally maintains a single connection/session to an Oracle Database instance provided by the bootstrap.servers property. Oracle Database 23ai introduces Kafka API support for Consumer Group Rebalancing. Partitions of a topic will be distributed among the active consumers of a consumer group such that no two consumers from the same consumer group are assigned the same partition of the topic simultaneously. Whenever new consumers join a consumer group or an existing consumer leaves the group, the partitions will be redistributed among the active consumers.
 For the 23ai release of Kafka APIs, a consumer can subscribe to only one topic. 
 The following KafkaConsumer APIs are supported in Oracle Database 23ai.
 Constructor: KafkaConsumer : Creates a consumer that allows the application to consume messages from a key based TxEventQ. Internal client side TxEventQ objects created are not visible to a client application. All variations of the KafkaConsumer constructor are supported in Oracle Database 23ai.
 Methods:
 Subscribe : This method takes a list of topics to subscribe to. In Oracle Database 23ai, only the first topic of the list will be subscribed to. An exception is thrown if the size of the list is greater than 1. This method creates a durable subscriber on TxEventQ server side with Group-Id as subscriber name. An application can also implement the ConsumerRebalanceListener interface and pass an object of the implemented class to the subscribe method. This allows a consumer to execute callbacks when a partition is revoked or assigned.
 Poll : The poll method returns a batch of messages from assigned partitions from TxEventQ. It attempts to dequeue a message from the key based TxEventQ for the subscriber. TxEventQ uses the array dequeue API of AQ JMS to receive a batch of messages from the queue. The size of the batch depends on the parameter max.poll.records set by the Kafka client application. Poll takes time in milliseconds as an argument. The AQ JMS API for array dequeue can pass this timeout as a dequeue option to the TxEventQ server and make the dequeue call, which will wait for messages till the timeout if the full array batch is not complete.
 When poll is invoked for the first time by a consumer, it triggers consumer rebalancing for all the alive consumers of the consumer group. At the end of the consumer rebalancing, all alive consumers are assigned topic partitions, and subsequent poll requests will fetch messages from the assigned partitions only. 
 An application can participate and influence rebalancing using the ConsumerRebalanceListener interface and partition.assignment.strategy configuration.
 The partition.assignment.strategy configuration allows an application to select a strategy for assigning partitions to consumer streams. OKafka supports all values for this configuration parameter which are documented in the Apache Kafka 2.8.0 documentation. 
 The default value for this configuration is org.oracle.okafka.clients.consumer.TXEQAssignor which is aware of Oracle RAC and implements a strategy that is best for achieving higher throughput from Oracle TxEventQ.
 This strategy prioritizes fair distribution of partitions and local consumption of messages while distributing partitions among alive sessions.
 ConsumerRebalanceListener allows an application to invoke callbacks when partitions are revoked or assigned to a consumer. 
 The database view USER_QUEUE_PARTITION_ASSIGNMENT_TABLE allows a developer to view the current distribution of partitions among the alive consumers.
 commitSync : Commits all consumed messages. Commit to an offset is not supported in Oracle Database 23ai. This call directly calls commit on the database which commits all consumed messages from TxEventQ.
 commitAsync : This call is translated into commitSync . A callback function passed as an argument gets executed once the commit is successful.
 Unsubscribe : Unsubscribes the topic that a consumer has subscribed to. A consumer can no longer consume messages from unsubscribed topics. This call does not remove a subscriber group from the TxEventQ metadata. Other consumer applications can still continue to consume for the same consumer group. 
 getDBConnection : Get the Oracle database connection used to consume records from Oracle Transactional Event Queue.
 close : Closes the consumer and unsubscribes the topic it has subscribed to. 
 Class: ConsumerRecord : A class that represents a consumed record in the Kafka platform. Kafka APIs receive AQ JMS messages from TxEventQ and convert each of them into a ConsumerRecord and deliver it to the application.
 Properties:
 auto.offset.reset : When there is no initial offset found for this consumer group, then the value of this property controls whether to consume the messages from the beginning of the topic partition or to only consume new messages. Values for this property and its usage are as follows:
 earliest : Consume from the beginning of the topic partition.
 latest : Consume from the end of the topic partition (default).
 none : Throw an exception if no offset present for the consumer group.
 key.deserializer and value.deserializer : For Oracle Kafka messaging platform, key and value are stored as byte arrays in a JMS Message in Oracle's TxEventQ. On consuming, these byte arrays are deserialized into a key and a value using key.deserializer and value.deserializer respectively. These properties allow an application to convert Key and Value , which are stored in byte array format, into application specified data types.
 group.id : This is a consumer group name for which messages are consumed from the Kafka topic. This property is used as a durable subscriber name for key based TxEventQs.
 max.poll.records : Maximum number of records to fetch in a single array dequeue call from an Oracle TxEventQ server.
 enable.auto.commit : Enables auto commit of consumed messages for every specified interval.
 auto.commit.interval.ms : Interval in milliseconds for auto commit of messages.
 bootstrap.servers : IP address and port of a machine where a database instance is running.
 Overview of Kafka Admin Implementation for TxEventQ
 The Kafka admininistrative API allows applications to perform administrative tasks like creating a topic, deleting a topic, adding a partition to a topic and so on. Oracle Database 23ai release supports only the following administrative APIs.
 Methods
 create(props) and create(config) : Creates an
 object of KafkaAdmin class that uses passed parameters.
 The method creates a database session which is used for further
 operations. An application has to provide connection configuration
 parameters as explained in the Connection Configuration section.
 createTopics() : Allows an application to create a Kafka Topic. This creates a TxEventQ in the user's schema. 
 close() : Closes a database session and Admin client.
 deleteTopic : Deletes a Kafka Topic. This returns null when a topic is deleted successfully. Otherwise, the method throws an exception. The method does not return until the topic is successfully deleted or any error is encountered. 
 Classes: NewTopic : Class used for creating a new topic. This class contains parameters with which a transactional event queue is created. 
 Kafka REST APIs for TxEventQ
 The TxEventQ REST APIs allow common operations to produce and consume from topics
 and partitions, and are implemented using Oracle REST Data Services (ORDS) in the Oracle
 Database. Common operations include creating and deleting topics, producing and
 consuming messages, and operational APIs for getting consumer lag on a topic, seeking to
 an offset, among many others. 
 The following three APIs for Kafka allow TxEventQ to co-exist with Kafka deployments, and
 provide the advantages of transactional outbox, JMS messaging and pub/sub in the
 database and high throughput streaming of events to the event queue in the Oracle
 Database. 
 See Also:
 Oracle Transactional Event Queues REST Endpoints for the Oracle REST Data Services API documentation
 Kafka Connectors for TxEventQ
 The Kafka Sink and Source Connector requires a minimum Oracle Database version of 21c in order to create a Transactional Event Queue. To use the application, Kafka with a minimum version number of 3.1.0 will need to be downloaded and installed on a server.
 See Also:
 https://github.com/oracle/okafka/tree/master/connectors for more information.
 Monitoring Message Transfer
 The Sink/Source connector messages transfer can be monitored from Oracle TxEventQ.
 See Also:
 Monitoring Transactional Event Queues to startup TxEventQ Monitor System to check enqueue/dequeue rate, TxEventQ depth, and more DB/System Level statistics.
