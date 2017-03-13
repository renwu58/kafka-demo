package com.jeffy.kafka;

import org.junit.Test;

public class MessageConsumerTest {

	@Test(timeout=6000)
	public void testAutoOffsetCommitConsumer() {
		MessageConsumer consumer = new MessageConsumer();
		consumer.init();
		consumer.autoOffsetCommitConsumer("topic01");
		System.out.println("Job 1 finished.");
	}

	@Test(timeout=6000)
	public void testManualOffsetConsumer() {
		MessageConsumer consumer = new MessageConsumer();
		consumer.init();
		consumer.manualOffsetConsumer("topic02");
		System.out.println("Job 2 finished.");
	}
	@Test(timeout=6000)
	public void testManualOffsetPartitionConsumer() {
		MessageConsumer consumer = new MessageConsumer();
		consumer.init();
		consumer.manualOffsetPartitionConsumer("topic03");
		System.out.println("Job 3 finished.");
	}
	@Test
	public void testMultiThreadConsumer() {
		MessageConsumer consumer = new MessageConsumer();
		consumer.init();
		new Thread(consumer.multiThreadConsumer("topic04")).start();
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		consumer.shutdown();
		System.out.println("Job 4 finished.");
	}

}
