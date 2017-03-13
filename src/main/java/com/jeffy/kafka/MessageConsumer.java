/**
 * 
 */
package com.jeffy.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;

/**
 * @author Jeffy <renwu58@gmail.com>
 * 
 * Kafka消费API示例，具体API参考：http://kafka.apache.org/0101/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html
 *
 */
public class MessageConsumer {

	private static final Logger log = Logger.getLogger(MessageConsumer.class);
	private final Properties props = new Properties();
//	private static final String TOPIC_NAME="asy_topic";
	private final AtomicBoolean closed = new AtomicBoolean(false);

	/**
	 * 初始化系统配置项
	 */
	public void init(){
		//A list of host/port pairs to use for establishing the initial connection to the Kafka cluster. 
		props.put("bootstrap.servers", "node1:6667,node2:6667,node3:6667");
		props.put("key.deserializer", StringDeserializer.class);
		props.put("value.deserializer", StringDeserializer.class);
		props.put("group.id", "JeffyTest");
		props.put("client.id", "Jeffy-PC");
		//What to do when there is no initial offset in Kafka or if the current offset does not exist any more on the server [latest, earliest, none]
		props.put("auto.offset.reset", "earliest");
		
	}

	/**
	 * kafka自动提交消费位置
	 */
	public void autoOffsetCommitConsumer(final String topicName){
		//If true the consumer's offset will be periodically committed in the background.
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", 2000);
		try(KafkaConsumer<String, String>  consumer= new KafkaConsumer<String, String>(props)){
			consumer.subscribe(Arrays.asList(topicName));
			long begin = System.currentTimeMillis();
			while((System.currentTimeMillis()-begin)<5000){
				consumer.poll(100).forEach((record)->{
					log.info("offset = "+  record.offset() +", key = " + record.key() + ", value = "+ record.value());
				});
			}
		}catch(Exception e){
			log.error("Something wrong", e);
		}
	}
	/**
	 * 手工控制消费的位置提交
	 */
	public void manualOffsetConsumer(final String topicName){
		props.put("enable.auto.commit", "false");
		try(KafkaConsumer<String, String>  consumer= new KafkaConsumer<String, String>(props)){
			consumer.subscribe(Arrays.asList(topicName));
			//定义批量大小
			final int minBatchSize = 100;
			//内测缓存，用于保存指定大小的批大小数据
			List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
			long begin = System.currentTimeMillis();
			while((System.currentTimeMillis()-begin) < 5000){
				consumer.poll(100).forEach((record)->{
					//log.info("offset = "+  record.offset() +", key = " + record.key() + ", value = "+ record.value());
					buffer.add(record);
					if(buffer.size() > minBatchSize){
						log.info("Batch insert into database or write to file.");
						consumer.commitAsync();
						buffer.clear();
					}
				});
			}	
		}catch(Exception e){
			log.error("Something wrong", e);
		}
	}
	/**
	 * 手工控制分區的提交位置，沒處理完一個分區的數據，將該分區位置進行提交，控制更細的粒度
	 */
	public void manualOffsetPartitionConsumer(final String topicName){
		props.put("enable.auto.commit", "false");
		long begin = System.currentTimeMillis();
		try(KafkaConsumer<String, String>  consumer= new KafkaConsumer<String, String>(props)) {
			 consumer.subscribe(Arrays.asList(topicName));
	         while((System.currentTimeMillis()-begin) < 5000) {
	           ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
	           records.partitions().forEach((partition)->{
	            	records.records(partition).forEach((record)->{
	            		log.info(record.offset() + ": " + record.value());
	            	});
	            	long lastOffset = records.records(partition).get(records.records(partition).size() - 1).offset();
	            	consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
	            });
	         }
	     } catch(Exception e){
			log.error("Something wrong", e);
		 }
	}
	
	/**
	 * 多线程下面使用示例
	 */
	public Runnable multiThreadConsumer(final String topicName){
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", 2000);
		return ()->{
			try(KafkaConsumer<String, String>  consumer= new KafkaConsumer<String, String>(props)) {
				consumer.subscribe(Arrays.asList(topicName));
				while(!closed.get()){
					consumer.poll(1000).forEach((record)->{
						log.info("offset = "+  record.offset() +", key = " + record.key() + ", value = "+ record.value());
					});
				}
			}catch(WakeupException  e){
				if(!closed.get()) throw e;
				log.error("Something wrong", e);
			}
		};
	}
	public AtomicBoolean getClosed() {
		return closed;
	}
	public void shutdown() {
		this.closed.set(true);
	}
	
}
