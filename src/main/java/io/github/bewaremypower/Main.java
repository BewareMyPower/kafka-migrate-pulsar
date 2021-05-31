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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;

/**
 * The entrypoint of the migration tool.
 */
@Slf4j
public class Main {

    private static class Arguments {

        @Parameter(names = { "--topic" }, description = "Topic name", required = true)
        private String topic;

        @Parameter(names = { "--kafka-broker-list" },
                description = "Kafka cluster's broker list, e.g. localhost:9092", required = true)
        private String kafkaBrokerList;

        @Parameter(names = { "--kop-broker-list" },
                description = "KoP cluster's broker list, e.g. localhost:9092", required = true)
        private String kopBrokerList;

        @Parameter(names = { "-h", "--help" }, description = "Show this help message")
        private boolean help = false;
    }

    public static void main(String[] args) {
        final Arguments arguments = new Arguments();
        final JCommander jCommander = new JCommander();
        try {
            jCommander.addObject(arguments);
            jCommander.parse(args);
            if (arguments.help) {
                jCommander.usage();
                return;
            }
        } catch (Exception e) {
            jCommander.usage();
            throw e;
        }

        final String kafkaBrokerList = arguments.kafkaBrokerList;
        final String kopBrokerList = arguments.kopBrokerList;
        final String topic = arguments.topic;
        log.info("Migrate topic {} from {} to {}...", topic, kafkaBrokerList, kopBrokerList);

        if (!createTopicIfNotExists(topic, kafkaBrokerList, kopBrokerList)) {
            return;
        }

        final CopyTopicDataTask copyTopicDataTask = new CopyTopicDataTask(topic, kafkaBrokerList, kopBrokerList);
        copyTopicDataTask.run();

        // TODO: Use an ExecutorService to schedule copyTopicDataTask periodically
        final CopyOffsetsTask copyOffsetsTask = new CopyOffsetsTask(topic, kafkaBrokerList, kopBrokerList);
        copyOffsetsTask.run();
    }

    private static boolean createTopicIfNotExists(final String topic,
                                                  final String oldBrokerList,
                                                  final String newBrokerList) {
        int partitions = -1;
        try (AdminClient admin = newAdmin(oldBrokerList)) {
            final Map<String, TopicDescription> topicDescriptionMap =
                    admin.describeTopics(Collections.singleton(topic)).all().get();
            final TopicDescription topicDescription = topicDescriptionMap.get(topic);
            if (topicDescription == null) {
                log.error("Failed to find topic {} in cluster {} but no exception was thrown", topic, oldBrokerList);
                return false;
            }
            partitions = topicDescription.partitions().size();
            System.out.println(topicDescription.partitions());
        } catch (InterruptedException | ExecutionException e) {
            log.error("Failed to find topic {} in cluster {}", topic, oldBrokerList, e);
            return false;
        }
        try (AdminClient admin = newAdmin(newBrokerList)) {
            admin.createTopics(Collections.singleton(new NewTopic(topic, partitions, (short) 1))).all().get();
        } catch (InterruptedException | ExecutionException e) {
            if (!e.getMessage().contains("already exists")) {
                log.error("Failed to create topic {} in cluster {}", topic, newBrokerList, e);
                return false;
            }
        }
        return true;
    }

    public static AdminClient newAdmin(final String brokerList) {
        final Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        return AdminClient.create(props);
    }
}
