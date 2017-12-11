package de.mbe1224.utils;

import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class KafkaTopic {

    private String name;
    private int partitions;
    private int replicationFactor;
    private Map<String, String> config;

    public KafkaTopic() {
    }

    public KafkaTopic(String name, int partitions, int replicationFactor, Map<String, String> config) {
        this.name = name;
        this.partitions = partitions;
        this.replicationFactor = replicationFactor;
        this.config = config;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getPartitions() {
        return partitions;
    }

    public void setPartitions(int partitions) {
        this.partitions = partitions;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public Map<String, String> getConfig() {
        return config;
    }

    public void setConfig(Map<String, String> config) {
        this.config = config;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        KafkaTopic that = (KafkaTopic) o;
        return getPartitions() == that.getPartitions()
                && getReplicationFactor() == that.getReplicationFactor()
                && Objects.equals(getName(), that.getName())
                && Objects.equals(getConfig(), that.getConfig());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName(), getPartitions(), getReplicationFactor(), getConfig());
    }

    @Override
    public String toString() {

        String configsAsString = String.join(", ", config.entrySet()
                .stream()
                .map(entry -> String.format("%s=%s", entry.getKey(), entry.getValue()))
                .collect(Collectors.toList()));

        return String.format("KafkaTopic{name='%s', partitions=%d, replicationFactor=%d, config='[%s]' }",
                this.name, this.partitions, this.replicationFactor, configsAsString);
    }
}
