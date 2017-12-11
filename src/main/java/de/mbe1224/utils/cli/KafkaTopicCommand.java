package de.mbe1224.utils.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;

import de.mbe1224.utils.KafkaTopic;
import de.mbe1224.utils.KafkaTopics;
import de.mbe1224.utils.KafkaAdminClient;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static net.sourceforge.argparse4j.impl.Arguments.store;

public class KafkaTopicCommand {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTopicCommand.class);
    private static final String KAFKA_TOPIC = "kafka-topic";
    private static final String TIMEOUT = "timeout";
    private static final String CONFIG = "config";
    private static final String FILE = "timeout";
    private static final String CREATE_IF_NOT_EXISTS = "create-if-not-exists";

    private static ArgumentParser createArgsParser() {

        ArgumentParser parser = ArgumentParsers
                .newFor(KAFKA_TOPIC)
                .build()
                .defaultHelp(true)
                .description("Check if topic exists and is valid.");
        parser.addArgument(TIMEOUT)
                .action(store())
                .required(true)
                .type(Integer.class)
                .metavar(TIMEOUT.toUpperCase())
                .help("Time (in ms) to wait for service to be ready.");
        parser.addArgument(CONFIG)
                .action(store())
                .required(true)
                .type(String.class)
                .metavar(CONFIG.toUpperCase())
                .help("Client config.");
        parser.addArgument(FILE)
                .action(Arguments.store())
                .type(String.class)
                .metavar(FILE.toUpperCase())
                .required(true)
                .help("Topic config file.");
        parser.addArgument(CREATE_IF_NOT_EXISTS)
                .action(Arguments.store())
                .type(Boolean.class)
                .metavar(CREATE_IF_NOT_EXISTS.toUpperCase())
                .setDefault(false)
                .help("Create topic if it does not exist.");
        return parser;
    }

    public static void main(String[] args) {

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        ArgumentParser parser = createArgsParser();
        boolean success = false;
        try {
            Namespace arguments = parser.parseArgs(args);
            LOGGER.debug("Arguments {}. ", arguments);

            boolean createIfNotExists = arguments.getBoolean(CREATE_IF_NOT_EXISTS);
            int timeout = arguments.getInt(TIMEOUT);
            String config = arguments.getString(CONFIG);
            String file = arguments.getString(FILE);
            KafkaTopics kafkaTopics = mapper.readValue(new File(file), KafkaTopics.class);

            try(KafkaAdminClient adminClient = new KafkaAdminClient(Utils.loadProps(config))) {
                for (KafkaTopic  kafkaTopic : kafkaTopics.getTopics()) {
                    String kafkaTopicName = kafkaTopic.getName();
                    if (adminClient.topicExists(kafkaTopic, timeout)) {
                        LOGGER.info("Topic [{}] already exists", kafkaTopicName);
                        success = adminClient.validateTopic(kafkaTopic, timeout);
                    } else if (createIfNotExists) {
                        LOGGER.info("Topic [{}] does not exist", kafkaTopicName);
                        success = adminClient.createTopic(kafkaTopic, timeout);
                    }
                }
            }
        } catch (ArgumentParserException e) {
            success = BaseCliCommand.handleParserExcepiton(args, parser, e);
        } catch (Exception e) {
            LOGGER.error("Error while running {}: {}", KAFKA_TOPIC, e.getMessage());
            success = false;
        }

        System.exit(success ? 0 : 1);
    }
}
