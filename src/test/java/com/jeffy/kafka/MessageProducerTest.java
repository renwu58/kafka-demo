package com.jeffy.kafka;

import org.junit.Test;

public class MessageProducerTest {
/**
 * 
[kafka@node2 bin]$ ./kafka-topics.sh --zookeeper master:2181 --create --topic topic01 --partitions 2 --replication-factor 3
Created topic "topic01".
[kafka@node2 bin]$ ./kafka-topics.sh --zookeeper master:2181 --create --topic topic02 --partitions 3 --replication-factor 3
Created topic "topic02".
[kafka@node2 bin]$ ./kafka-topics.sh --zookeeper master:2181 --create --topic topic03 --partitions 4 --replication-factor 2
Created topic "topic03".
[kafka@node2 bin]$ ./kafka-topics.sh --zookeeper master:2181 --create --topic topic04 --partitions 4 --replication-factor 2
Created topic "topic04".
 * 
 */
	@Test
	public void testAsynchronousWrite() {
		MessageProducer  message = new MessageProducer();
    	message.init();
    	long beginTs = System.currentTimeMillis();
    	message.asynchronousWrite("topic01");
    	message.asynchronousWrite("topic02");
    	System.out.println("Synchronous send cost " + (System.currentTimeMillis() - beginTs) + " milliseconds.");
    	message.shutdown();
	}
	@Test
	public void testSynchronousWrite() {
		MessageProducer  message = new MessageProducer();
    	message.init();
    	long beginTs = System.currentTimeMillis();
    	message.synchronousWrite("topic03");
    	message.synchronousWrite("topic04");
    	System.out.println("Synchronous send cost " + (System.currentTimeMillis() - beginTs) + " milliseconds.");
    	message.shutdown();
	}

}
