package com.gslab.pepper.util;

import org.apache.kafka.common.serialization.StringSerializer;
/**
 * The ProducerKeys is property constant class.
 *
 * @Author Satish Bhor<satish.bhor@gslab.com>, Nachiket Kate <nachiket.kate@gslab.com>
 * @Version 1.0
 * @since 01/03/2017
 */
public class ProducerKeys {
    public static final String KAFKA_TOPIC_CONFIG = "kafka.topic.name";
    public static final String KAFKA_TOPIC_CONFIG_DEFAULT = "<Topic>";

    public static final String ZOOKEEPER_SERVERS = "zookeeper.servers";
    public static final String ZOOKEEPER_SERVERS_DEFAULT = "<Zookeeper List>";

    public static final String ACKS_CONFIG_DEFAULT = "1";
    public static final String BATCH_SIZE_CONFIG_DEFAULT = "16384";
    public static final String BOOTSTRAP_SERVERS_CONFIG_DEFAULT = "<Broker List>";
    public static final String BUFFER_MEMORY_CONFIG_DEFAULT = "33554432";
    public static final String COMPRESSION_TYPE_CONFIG_DEFAULT = "none";

    public static final String JAVA_SEC_AUTH_LOGIN_CONFIG = "java.security.auth.login.config";
    public static final String JAVA_SEC_AUTH_LOGIN_CONFIG_DEFAULT = "<JAAS File Location>";

    public static final String JAVA_SEC_KRB5_CONFIG = "java.security.krb5.conf";
    public static final String JAVA_SEC_KRB5_CONFIG_DEFAULT = "<krb5.conf location>";

    public static final String KERBEROS_ENABLED = "kerberos.auth.enabled";
    public static final String KERBEROS_ENABLED_DEFAULT = "NO";

    public static final String LINGER_MS_CONFIG_DEFAULT = "0";
    public static final String RECEIVE_BUFFER_CONFIG_DEFAULT = "32768";
    public static final String SEND_BUFFER_CONFIG_DEFAULT = "131072";

    public static final String SASL_JAAS_CONFIG = "sasl.jaas.config";
    public static final String SASL_JAAS_CONFIG_DEFAULT = "org.apache.kafka.security.plain.PlainLoginModule " +
                                                            "required username=\"alice\" password=\"secret\";";

    public static final String SASL_KERBEROS_SERVICE_NAME = "sasl.kerberos.service.name";
    public static final String SASL_KERBEROS_SERVICE_NAME_DEFAULT = "kafka";

    public static final String SASL_MECHANISM = "sasl.mechanism";
    public static final String SASL_MECHANISM_DEFAULT = "PLAIN"; // this suits SASL_SSL, it was GSSAPI

    public static final String SSL_ENABLED_PROTOCOLS = "ssl.enabled.protocols";
    public static final String SSL_ENABLED_PROTOCOLS_DEFAULT = "TLSv1.2";

    public static final String SSL_TRUSTSTORE_LOCATION = "ssl.truststore.location";
    public static final String SSL_TRUSTSTORE_LOCATION_DEFAULT = "/replace/me/client.truststore.jks";

    public static final String SSL_TRUSTSTORE_PASSWORD = "ssl.truststore.password";
    public static final String SSL_TRUSTSTORE_PASSWORD_DEFAULT = "<replace with the correct password>";

    public static final String SSL_TRUSTSTORE_TYPE = "ssl.truststore.type";
    public static final String SSL_TRUSTSTORE_TYPE_DEFAULT = "JKS";



    public static final String KEY_SERIALIZER_CLASS_CONFIG_DEFAULT = StringSerializer.class.getName();
    public static final String VALUE_SERIALIZER_CLASS_CONFIG_DEFAULT = StringSerializer.class.getName();

    public static final String FLAG_YES = "YES";
}
