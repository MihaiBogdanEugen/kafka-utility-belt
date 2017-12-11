package de.mbe1224.utils;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class KafkaTopics {

    private List<KafkaTopic> topics;

    public KafkaTopics() {
        this.topics = Collections.emptyList();
    }

    public KafkaTopics(List<KafkaTopic> topics) {
        this.topics = topics;
    }

    public List<KafkaTopic> getTopics() {
        return topics;
    }

    public void setTopics(List<KafkaTopic> topics) {
        this.topics = topics;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        KafkaTopics that = (KafkaTopics) o;
        return Objects.equals(getTopics(), that.getTopics());
    }

    @Override
    public int hashCode() {

        return Objects.hash(getTopics());
    }

    @Override
    public String toString() {
        return String.format("KafkaTopics{topics=[%s]}", String.join(", ", this.topics.stream().map(KafkaTopic::toString).collect(Collectors.toList())));
    }
}
