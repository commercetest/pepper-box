/**
 * A Kafka consumer which adds a timestamp to each received-message.
 *
 * The processing uses a micro-batch polling system so the timestamp
 * includes a subset of the polling interval as well as the main processing/
 * propagation time intrinsic to the Kafka subsystem.
 *
 * Basic support for SASL_SSL is included as that's essential for some of the
 * testing I need to do.
 *
 * Known limitations:
 * The script relies on pre-defined variables, which I define in the Test Plan
 * as User Defined Variables. It doesn't really check their validity (yet) so
 * please consider this a proof-of-concept that's prone to problems if it's
 * not provided with valid settings.
 *
 * The support for SASL_SSL is based on the immediate needs of the project I'm
 * working on. YMMV. Improvements are welcome.
 */

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.protocol.SecurityProtocol

String bootstrap_servers = vars.get("bootstrap.servers") // get or die
String topic = vars.get("topic.name")  // get or die
String generate_per_thread_topics = vars.get("per.thread.topicnames") // default to false
String threadz = props.get("threadz")
Integer counter = Integer.valueOf(args[0]) % Integer.valueOf(threadz)

String sasl_jaas_username = vars.get("sasl.jaas.username")
String sasl_jaas_password = vars.get("sasl.jaas.password")
String security_protocol = vars.get("security.protocol")
log.info("security.protocol:" + security_protocol)

String ssl_truststore_location = vars.get("ssl.truststore.location")
String ssl_truststore_password = vars.get("ssl.truststore.password")

Long WAITING_PERIOD = 30000  // 30 seconds to wait for additional messages.

// Add validation of the input parameters around here. These are only examples
// See https://github.com/commercetest/pepper-box/issues/8 for context

if (bootstrap_servers.length() < 8) {
    log.error("bootstrap.servers too short to be trusted: " + bootstrap_servers)
    ctx.getEngine().stopTest()
} else {
    log.info("bootstrap.servers:" + bootstrap_servers)
}

if (sasl_jaas_username.length() < 3) {
   log.error("sasl.jaas.username too short, aborting test")
   ctx.getEngine().stopTest()
}

String sasl_jaas_config = "org.apache.kafka.common.security.plain.PlainLoginModule required" +
        " username=\"${sasl_jaas_username}\"" +
        " password=\"${sasl_jaas_password}\";"
log.info("sasl_jaas_config: " + sasl_jaas_config)

String group = "jmeter-consumer"

// TODO find a way to obtain the hostname,
// log.info("host:" + vars.get("HOST")) isn't correct.
// See https://github.com/commercetest/pepper-box/issues/7 for the context

Properties props = new Properties()
props.put("bootstrap.servers", bootstrap_servers)
props.put("group.id", group)
props.put("enable.auto.commit", "true")
props.put("auto.commit.interval.ms", "1000")
props.put("session.timeout.ms", "10000")
props.put("key.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer")
props.put("value.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer")

/*
 * The following is essentially a replica of what I've coded in the Java code
 * PepperBoxKafkaSampler.java
 * I'd prefer to share the configuration settings and will aim to do so if and
 * when I find a practical way to so do.
 */
if (security_protocol == SecurityProtocol.SASL_SSL.name) {
    props.put("security.protocol", security_protocol)
    props.put("sasl.jaas.config", sasl_jaas_config)
    props.put("sasl.mechanism", "PLAIN") // hard-coded for now
    props.put("ssl.enabled.protocols", "TLSv1.2")
    props.put("ssl.truststore.location", ssl_truststore_location)
    props.put("ssl.truststore.password", ssl_truststore_password)
    props.put("ssl.truststore.type", "JKS")
}

/*
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
 */

KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props)

if (generate_per_thread_topics?.trim() && generate_per_thread_topics.equalsIgnoreCase("yes")) {
    thread_topic_name = topic + "." + counter
    consumer.subscribe(Arrays.asList(thread_topic_name))
    log.info("Subscribed to per thread topic:" + thread_topic_name)
} else {
    consumer.subscribe(Arrays.asList(topic))
    log.info("Subscribed to common topic:" + topic)
}

long t = System.currentTimeMillis();
long end = t + WAITING_PERIOD;
String results_filename = "results-" + counter + ".json"
f = new FileOutputStream(results_filename, true);
p = new PrintStream(f);
while (System.currentTimeMillis()<end)
{
   ConsumerRecords<String, String> records = consumer.poll(100);
   for (ConsumerRecord<String, String> record : records)
   {
      p.println( "{\n\"received\":{\n\t\"batchReceivedAt\":" + System.currentTimeMillis() + ",\n\t\"offset\":" + record.offset() +"\n} \n\"generated\":" + record.value() + "\n}");
      end = System.currentTimeMillis() + WAITING_PERIOD  // increment the how long to wait for more data time
   }
   consumer.commitSync()
}
consumer.close()
p.close()
f.close()
