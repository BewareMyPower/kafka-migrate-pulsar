/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.bewaremypower;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/**
 * Copy a topic's messages from an old cluster tp a new cluster.
 */
@AllArgsConstructor
@Slf4j
public class CopyTopicDataTask implements Runnable {

    private static final String KAFKA_MIGRATE_PULSAR_GROUP = "__kafka_migrate_pulsar_group_12";

    private final String topic;
    private final String oldBrokerList;
    private final String newBrokerList;

    @Override
    public void run() {
        final KafkaConsumer<byte[], byte[]> consumer = newConsumer();
        final KafkaProducer<byte[], byte[]> producer = newProducer();

        final List<TopicPartition> topicPartitions = consumer.partitionsFor(topic).stream()
                .map(partitionInfo -> new TopicPartition(topic, partitionInfo.partition()))
                .collect(Collectors.toList());

        final Map<TopicPartition, Long> nextOffsets = new HashMap<>();
        topicPartitions.forEach(topicPartition -> nextOffsets.put(topicPartition, -1L));

        while (true) {
            final ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(1));
            if (records.isEmpty()) {
                // check EOF
                final Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);
                if (reachedEnd(nextOffsets, endOffsets)) {
                    log.info("Stop copying data of topic {}, because EOF of all partitions are reached: {}",
                            topic, endOffsets);
                    break;
                }
                log.info("end offsets: {}", endOffsets);
                log.info("consumed offsets: {}", nextOffsets);
                continue;
            }

            records.forEach(record -> {
                final TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
                nextOffsets.put(topicPartition, record.offset() + 1);
                producer.send(new ProducerRecord<>(topic, topicPartition.partition(), record.key(), record.value()));
            });

            consumer.commitSync(Duration.ofSeconds(1));
        }

        producer.flush();
        producer.close();
        consumer.close();
    }

    private boolean reachedEnd(final Map<TopicPartition, Long> offsets, final Map<TopicPartition, Long> endOffsets) {
        for (Map.Entry<TopicPartition, Long> partitionOffset : offsets.entrySet()) {
            final TopicPartition topicPartition = partitionOffset.getKey();
            final long offset = partitionOffset.getValue();

            if (offset != -1L && offset != endOffsets.get(topicPartition)) {
                return false;
            }
        }
        return true;
    }

    private KafkaProducer<byte[], byte[]> newProducer() {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, newBrokerList);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return new KafkaProducer<>(props);
    }

    private KafkaConsumer<byte[], byte[]> newConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, oldBrokerList);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, KAFKA_MIGRATE_PULSAR_GROUP);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // commit offsets manually

        final KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton(topic));
        return consumer;
    }
}
