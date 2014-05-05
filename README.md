amazon-sqs-throughput-test
==========================

Little test harness to test throughput of Amazon SQS. Optionally uses ZooKeeper to coordinate test start/stop between nodes.

## Building and Running

    mvn clean package;
    java -classpath target/amazon-sqs-test-1.0-SNAPSHOT.jar com.mmiladinovic.sqs.main.ConsumerMain
    java -classpath target/amazon-sqs-test-1.0-SNAPSHOT.jar com.mmiladinovic.sqs.main.ProducerMain


