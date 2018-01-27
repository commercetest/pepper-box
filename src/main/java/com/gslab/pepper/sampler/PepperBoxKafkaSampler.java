
package com.gslab.pepper.sampler;

import com.eclipsesource.json.*;
import com.gslab.pepper.util.ProducerKeys;
import com.gslab.pepper.util.PropsKeys;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jmeter.threads.JMeterVariables;
import org.apache.jmeter.util.JMeterUtils;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.log.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * The PepperBoxKafkaSampler class custom java sampler for jmeter.
 *
 * @Author Satish Bhor<satish.bhor@gslab.com>, Nachiket Kate <nachiket.kate@gslab.com>
 * @Version 1.0
 * @since 01/03/2017
 */
public class PepperBoxKafkaSampler extends AbstractJavaSamplerClient {

    public static final String GENERATE_PER_THREAD_TOPICS = "generate.per-thread.topics";
    //kafka producer
    private KafkaProducer<String, Object> producer;

    // topic on which messages will be sent
    private String topic;

    //Message placeholder key
    private String placeHolder;

    private static final Logger log = LoggingManager.getLoggerForClass();

    /**
     * Set default parameters and their values
     *
     * @return
     */
    @Override
    public Arguments getDefaultParameters() {

        Arguments defaultParameters = new Arguments();

        log.info(String.format("Threads num %d, total %d",
                JMeterContextService.getNumberOfThreads(), JMeterContextService.getTotalThreads()));

        defaultParameters.addArgument(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ProducerKeys.BOOTSTRAP_SERVERS_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerKeys.ZOOKEEPER_SERVERS, ProducerKeys.ZOOKEEPER_SERVERS_DEFAULT);
        defaultParameters.addArgument(ProducerKeys.KAFKA_TOPIC_CONFIG, ProducerKeys.KAFKA_TOPIC_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ProducerKeys.KEY_SERIALIZER_CLASS_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ProducerKeys.VALUE_SERIALIZER_CLASS_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerConfig.COMPRESSION_TYPE_CONFIG, ProducerKeys.COMPRESSION_TYPE_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerConfig.BATCH_SIZE_CONFIG, ProducerKeys.BATCH_SIZE_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerConfig.LINGER_MS_CONFIG, ProducerKeys.LINGER_MS_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerConfig.BUFFER_MEMORY_CONFIG, ProducerKeys.BUFFER_MEMORY_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerConfig.ACKS_CONFIG, ProducerKeys.ACKS_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");
        defaultParameters.addArgument(ProducerConfig.RETRIES_CONFIG, "0");
        defaultParameters.addArgument(ProducerConfig.SEND_BUFFER_CONFIG, ProducerKeys.SEND_BUFFER_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerConfig.RECEIVE_BUFFER_CONFIG, ProducerKeys.RECEIVE_BUFFER_CONFIG_DEFAULT);
        defaultParameters.addArgument(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.PLAINTEXT.name);
        defaultParameters.addArgument(PropsKeys.MESSAGE_PLACEHOLDER_KEY, PropsKeys.MSG_PLACEHOLDER);
        defaultParameters.addArgument(ProducerKeys.KERBEROS_ENABLED, ProducerKeys.KERBEROS_ENABLED_DEFAULT);
        defaultParameters.addArgument(ProducerKeys.JAVA_SEC_AUTH_LOGIN_CONFIG, ProducerKeys.JAVA_SEC_AUTH_LOGIN_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerKeys.JAVA_SEC_KRB5_CONFIG, ProducerKeys.JAVA_SEC_KRB5_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerKeys.SASL_KERBEROS_SERVICE_NAME, ProducerKeys.SASL_KERBEROS_SERVICE_NAME_DEFAULT);
        defaultParameters.addArgument(ProducerKeys.SASL_MECHANISM, ProducerKeys.SASL_MECHANISM_DEFAULT);
        defaultParameters.addArgument(ProducerKeys.SASL_JAAS_CONFIG, ProducerKeys.SASL_JAAS_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerKeys.SSL_ENABLED_PROTOCOLS, ProducerKeys.SSL_ENABLED_PROTOCOLS_DEFAULT);
        defaultParameters.addArgument(ProducerKeys.SSL_TRUSTSTORE_LOCATION, ProducerKeys.SSL_TRUSTSTORE_LOCATION_DEFAULT);
        defaultParameters.addArgument(ProducerKeys.SSL_TRUSTSTORE_PASSWORD, ProducerKeys.SSL_TRUSTSTORE_PASSWORD_DEFAULT);
        defaultParameters.addArgument(ProducerKeys.SSL_TRUSTSTORE_TYPE, ProducerKeys.SSL_TRUSTSTORE_TYPE_DEFAULT);
        defaultParameters.addArgument(GENERATE_PER_THREAD_TOPICS, "NO");
        return defaultParameters;
    }

    /**
     * Gets invoked exactly once  before thread starts
     *
     * @param context
     */
    @Override
    public void setupTest(JavaSamplerContext context) {

        try {
            JMeterVariables jMeterVariables = new JMeterVariables();
            log.info("zk = " + JMeterContextService.getContext().getVariables().get("zookeepers"));
            log.info("vars(\"zookeepers\":" +jMeterVariables.get("zookeepers"));
        } catch (Exception e) {
            log.error("Can't get the jmeter vars", e);
        }
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBrokerServers(context));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, context.getParameter(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, context.getParameter(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
        props.put(ProducerConfig.ACKS_CONFIG, context.getParameter(ProducerConfig.ACKS_CONFIG));
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
                context.getParameter(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION));
        props.put(ProducerConfig.RETRIES_CONFIG, context.getParameter(ProducerConfig.RETRIES_CONFIG));
        props.put(ProducerConfig.SEND_BUFFER_CONFIG, context.getParameter(ProducerConfig.SEND_BUFFER_CONFIG));
        props.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, context.getParameter(ProducerConfig.RECEIVE_BUFFER_CONFIG));
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, context.getParameter(ProducerConfig.BATCH_SIZE_CONFIG));
        props.put(ProducerConfig.LINGER_MS_CONFIG, context.getParameter(ProducerConfig.LINGER_MS_CONFIG));
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, context.getParameter(ProducerConfig.BUFFER_MEMORY_CONFIG));
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, context.getParameter(ProducerConfig.COMPRESSION_TYPE_CONFIG));
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, context.getParameter(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        props.put(ProducerKeys.SASL_MECHANISM, context.getParameter(ProducerKeys.SASL_MECHANISM));

        final String security_protocol = context.getParameter(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG);
        log.info("security_protocol set to[" + security_protocol + "], comparing to [" + SecurityProtocol.SASL_SSL.name +"].");
        if (security_protocol.equals(SecurityProtocol.SASL_SSL.name)) {
            log.info("Adding SASL_SSL parameters for Kafka to use.");
            props.put(ProducerKeys.SASL_JAAS_CONFIG, context.getParameter(ProducerKeys.SASL_JAAS_CONFIG));
            props.put(ProducerKeys.SASL_MECHANISM, context.getParameter(ProducerKeys.SASL_MECHANISM));

            props.put(ProducerKeys.SSL_ENABLED_PROTOCOLS, context.getParameter(ProducerKeys.SSL_ENABLED_PROTOCOLS));
            props.put(ProducerKeys.SSL_TRUSTSTORE_LOCATION, context.getParameter(ProducerKeys.SSL_TRUSTSTORE_LOCATION));
            props.put(ProducerKeys.SSL_TRUSTSTORE_PASSWORD, context.getParameter(ProducerKeys.SSL_TRUSTSTORE_PASSWORD));
            props.put(ProducerKeys.SSL_TRUSTSTORE_TYPE, context.getParameter(ProducerKeys.SSL_TRUSTSTORE_TYPE));
        }

        Iterator<String> parameters = context.getParameterNamesIterator();
        parameters.forEachRemaining(parameter -> {
            if (parameter.startsWith("_")) {
                log.info("Adding: " + parameter);
                props.put(parameter.substring(1), context.getParameter(parameter));
            }
        });
        String kerberosEnabled = context.getParameter(ProducerKeys.KERBEROS_ENABLED);
        if (kerberosEnabled != null && kerberosEnabled.equals(ProducerKeys.FLAG_YES)) {
            log.info("Adding the Kerberos related parameters.");
            System.setProperty(ProducerKeys.JAVA_SEC_AUTH_LOGIN_CONFIG, context.getParameter(ProducerKeys.JAVA_SEC_AUTH_LOGIN_CONFIG));
            System.setProperty(ProducerKeys.JAVA_SEC_KRB5_CONFIG, context.getParameter(ProducerKeys.JAVA_SEC_KRB5_CONFIG));
            props.put(ProducerKeys.SASL_KERBEROS_SERVICE_NAME, context.getParameter(ProducerKeys.SASL_KERBEROS_SERVICE_NAME));
        }

        placeHolder = context.getParameter(PropsKeys.MESSAGE_PLACEHOLDER_KEY);
        topic = context.getParameter(ProducerKeys.KAFKA_TOPIC_CONFIG);

        System.setProperty("A", topic);  // Seems to work but I can't find how to use it elsewhere.
        try {
            JMeterUtils.setProperty("B", topic);
            log.info("Set Properties System.setProperty A and B to topic: " + topic);
        } catch (NullPointerException npe) {
            // This catch is needed otherwise the tests fail with the NPE
            log.error("couldn't find JMeterUtils.");
        }

        if (context.getParameter(GENERATE_PER_THREAD_TOPICS).equalsIgnoreCase("YES")) {
            topic = topic + "." + JMeterContextService.getContext().getThreadNum();
            log.info("Using custom Topic: " + topic);
        }
        producer = new KafkaProducer<String, Object>(props);

    }


    /**
     * For each sample request this method is invoked and will return success/failure result
     *
     * @param context
     * @return
     */
    @Override
    public SampleResult runTest(JavaSamplerContext context) {

        SampleResult sampleResult = new SampleResult();
        sampleResult.sampleStart();
        Object message = JMeterContextService.getContext().getVariables().getObject(placeHolder);

        try {
            ProducerRecord<String, Object> producerRecord = new ProducerRecord<String, Object>(topic, message);
            producer.send(producerRecord);
            sampleResult.setResponseData(message.toString(), StandardCharsets.UTF_8.name());
            sampleResult.setSuccessful(true);
            sampleResult.sampleEnd();

        } catch (Exception e) {
            log.error("Failed to send message", e);
            sampleResult.setResponseData(e.getMessage(), StandardCharsets.UTF_8.name());
            sampleResult.setSuccessful(false);
            sampleResult.sampleEnd();

        }

        return sampleResult;
    }

    @Override
    public void teardownTest(JavaSamplerContext context) {
        producer.close();
    }

    private String getBrokerServers(JavaSamplerContext context) {

        StringBuilder kafkaBrokers = new StringBuilder();

        String zookeeperServers = context.getParameter(ProducerKeys.ZOOKEEPER_SERVERS);

        if (zookeeperServers != null && !zookeeperServers.equalsIgnoreCase(ProducerKeys.ZOOKEEPER_SERVERS_DEFAULT)) {

            try {

                ZooKeeper zk = new ZooKeeper(zookeeperServers, 10000, null);
                List<String> ids = zk.getChildren(PropsKeys.BROKER_IDS_ZK_PATH, false);

                for (String id : ids) {

                    String brokerInfo = new String(zk.getData(PropsKeys.BROKER_IDS_ZK_PATH + "/" + id, false, null));
                    log.info("Broker Info from zk: " + brokerInfo);

                    JsonObject jsonObject = Json.parse(brokerInfo).asObject();

                    String brokerHost = jsonObject.getString(PropsKeys.HOST, "");
                    int brokerPort = jsonObject.getInt(PropsKeys.PORT, -1);

                    if (!brokerHost.isEmpty() && brokerPort != -1) {
                        kafkaBrokers.append(brokerHost);
                        kafkaBrokers.append(":");
                        kafkaBrokers.append(brokerPort);
                        kafkaBrokers.append(",");
                    }
                }
            } catch (IOException | KeeperException | InterruptedException | UnsupportedOperationException e) {
                log.error("Failed to get broker information", e);
            }
        }

        if (kafkaBrokers.length() > 0) {
            kafkaBrokers.setLength(kafkaBrokers.length() - 1);
            return kafkaBrokers.toString();
        } else {
            return  context.getParameter(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
        }
    }
}
