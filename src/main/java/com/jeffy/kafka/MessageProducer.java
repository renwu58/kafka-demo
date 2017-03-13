package com.jeffy.kafka;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

/**
 * 完整API参考： http://kafka.apache.org/0101/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html
 * 
 * 本实例演示同步发送与异步发送到Kafka Cluster
 * 
 */
public class MessageProducer
{
	private final static Logger log = Logger.getLogger(MessageProducer.class);
//	private final static String ASY_TOPIC_NAME = "asy_topic";
//	private final static String SYN_TOPIC_NAME = "syn_topic";
    private Properties prons = null;
    private KafkaProducer<String, String> producer = null;
    public void init(){
    	//http://kafka.apache.org/documentation#configuration
    	prons = new Properties();
    	//An id string to pass to the server when making requests. 
    	prons.put("client.id", "Jeffy");
    	prons.put("bootstrap.servers", "node1:6667,node2:6667,node3:6667");
    	prons.put("key.serializer", StringSerializer.class);
    	prons.put("value.serializer", StringSerializer.class);
    	//The number of acknowledgments the producer requires the leader to have received before considering a request complete. [0,1,all]
    	prons.put("acks", "1");
    	//Setting a value greater than zero will cause the client to resend any record whose send fails with a potentially transient error.
    	prons.put("retries", 1);
    	//The producer will attempt to batch records together into fewer requests whenever multiple records are being sent to the same partition. (Unit bytes)
    	prons.put("batch.size", 4096);
    	//The producer groups together any records that arrive in between request transmissions into a single batched request
    	prons.put("linger.ms", 500);
    	//Partitioner class that implements the Partitioner interface.
    	//prons.put("partitioner.class", DefaultPartitioner.class);
    	//The configuration controls the maximum amount of time the server will wait for acknowledgments from followers to meet the acknowledgment requirements the producer has specified with the acks configuration.
    	prons.put("timeout.ms", 3000);
    	producer = new KafkaProducer<String, String>(prons);
    }
    /**
     * 异步发送消息示例
     */
    public void asynchronousWrite(String topicName){
    	for(int i=0; i<200; i++){
    		producer.send(new ProducerRecord<String, String>(topicName,"asy-key-" + i,"asy-value-" + i), new Callback(){
    			@Override
    			public void onCompletion(RecordMetadata metadata, Exception e) {
    				if(e != null)
    					log.warn("Send failed for record " + metadata.topic(),e);
    				else
    					log.info("Send message " + metadata + " sucess.");
    			}
        	});
    	}
    }
    /**
     * 同步发送消息示例
     */
    public void synchronousWrite(String topicName){
    	log.info("Synchronous producer message to broker.");
    	for(int i=0; i<200; i++){
    		Future<RecordMetadata> future = producer.send(new ProducerRecord<String, String>(topicName,"topic-key-"+i,"topic-value-"+i));
    		try {
    			RecordMetadata metadata = future.get();
    			log.info("Message " + metadata +" send sucess!");
    		} catch (InterruptedException | ExecutionException e){
    			log.error("Message  send to " + topicName + " failure!", e);
			}
    	}
    }
    public void shutdown(){
    	producer.close();
    	log.info("Shutdown message system.");
    }
}
