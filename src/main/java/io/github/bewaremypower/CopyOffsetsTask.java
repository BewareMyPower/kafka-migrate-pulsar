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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/**
 * Copy a topic's offsets from an old cluster tp a new cluster.
 */
@AllArgsConstructor
@Slf4j
public class CopyOffsetsTask implements Runnable {

    private final String topic;
    private final String oldBrokerList;
    private final String newBrokerList;

    @Override
    public void run() {
        try (AdminClient admin = Main.newAdmin(oldBrokerList)) {
            // NOTE: Kafka doesn't provide an API to list groups of a specific topic directly
            final List<String> allGroups;
            try {
                allGroups = admin.listConsumerGroups().all().get().stream()
                        .map(ConsumerGroupListing::groupId).collect(Collectors.toList());
            } catch (InterruptedException | ExecutionException e) {
                log.error("Failed to get all groups from {}", oldBrokerList, e);
                return;
            }

            for (String group : allGroups) {
                // filter the offsets that belong to this topic
                try {
                    final Map<TopicPartition, OffsetAndMetadata> partitionsToOffsetAndMetadata =
                            admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get()
                            .entrySet().stream().filter(entry -> entry.getKey().topic().equals(topic))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                    commitOffsets(group, partitionsToOffsetAndMetadata);
                } catch (InterruptedException | ExecutionException e) {
                    log.error("Failed to list offsets of group {}", group, e);
                }
            }
        }
    }

    private void commitOffsets(final String group, final Map<TopicPartition, OffsetAndMetadata> offsets) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, newBrokerList);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // commit offsets manually

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            final AtomicBoolean readyToCommit = new AtomicBoolean(false);
            consumer.subscribe(Collections.singleton(topic), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                    readyToCommit.set(true);
                }
            });

            // wait at most 5 seconds until partitions are assigned
            for (int i = 0; i < 5; i++) {
                consumer.poll(Duration.ofSeconds(1));
                if (readyToCommit.get()) {
                    consumer.commitSync(offsets);
                    log.info("Commit offsets to {}, group: {}, offsets: {}", newBrokerList, group, offsets);
                    return;
                }
            }
            log.error("Failed to commit offsets to {}: no partitions assigned in given timeout"
                    + "(group: {}, offsets: {})", newBrokerList, group, offsets);
        } catch (Exception e) {
            log.error("Failed to commit offsets to {}, group: {}, offsets: {}", newBrokerList, group, offsets, e);
        }
    }
}
