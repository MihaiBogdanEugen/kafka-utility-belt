package de.mbe1224.utils;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

public class KafkaAdminClient implements AutoCloseable {

    private final AdminClient adminClient;

    public KafkaAdminClient(Properties props) {
        this.adminClient = AdminClient.create(props);
    }

    public boolean createTopic(KafkaTopic topic, int timeOut) throws ExecutionException, InterruptedException {

        NewTopic newTopic = new NewTopic(topic.getName(), topic.getPartitions(), (short)topic.getReplicationFactor()).configs(topic.getConfig());
        CreateTopicsResult result = this.adminClient.createTopics(Collections.singletonList(newTopic), new CreateTopicsOptions().timeoutMs(timeOut));

        result.all().get();
        return true;
    }

    public boolean validateTopic(KafkaTopic topic, int timeOut) throws ExecutionException, InterruptedException {

        String topicName = topic.getName();
        ConfigResource configResource = new ConfigResource(Type.TOPIC, topicName);

        DescribeTopicsResult describeTopicsResult = this.adminClient.describeTopics(Collections.singletonList(topicName), new DescribeTopicsOptions().timeoutMs(timeOut));

        Map<String, TopicDescription> topicsDescriptions = describeTopicsResult.all().get();
        TopicDescription topicDescription = topicsDescriptions.get(topicName);

        DescribeConfigsResult describeConfigsResult = this.adminClient.describeConfigs(Collections.singletonList(configResource), new DescribeConfigsOptions().timeoutMs(timeOut));

        Map<ConfigResource, Config> configs = describeConfigsResult.all().get();
        Config config = configs.get(configResource);

        Map<String, String> actualConfig = topic.getConfig().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> config.get(e.getKey()).value()));
        KafkaTopic actualTopic = new KafkaTopic(topicDescription.name(), topicDescription.partitions().size(), (topicDescription.partitions().get(0)).replicas().size(), actualConfig);

        return actualTopic.equals(topic);
    }

    public boolean topicExists(KafkaTopic topic, Integer timeOut) throws ExecutionException, InterruptedException {

        String topicName = topic.getName();

        try {
            DescribeTopicsResult describeTopicsResult = this.adminClient.describeTopics(Collections.singletonList(topicName), new DescribeTopicsOptions().timeoutMs(timeOut));
            Map<String, TopicDescription> topicsDescriptions = describeTopicsResult.all().get();
            topicsDescriptions.get(topicName);
            return true;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                return false;
            } else {
                throw e;
            }
        }
    }

    @Override
    public void close() {
        this.adminClient.close();
    }
}
