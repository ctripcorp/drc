package com.ctrip.framework.drc.service.config;

import com.ctrip.xpipe.config.AbstractConfigBean;

/**
 * Created by shiruixin
 * 2024/10/24 16:43
 */
public class TripServiceDynamicConfig extends AbstractConfigBean {
    private static final String SUBENV_SWITCH = "subenv.switch";
    private static final String KAFKA_APPID_TOKEN = "kafka.appid.token";

    private static final String LINGER_MS_CONFIG = "kafka.linger.ms";
    private static final String DEFAULT_LINGER_MS_CONFIG = "500";

    private static final String BATCH_SIZE_CONFIG = "kafka.batch.size";
    private static final String DEFAULT_BATCH_SIZE_CONFIG = "163840";

    private static final String BUFFER_MEMORY_CONFIG = "kafka.buffer.memory";
    private static final String DEFAULT_BUFFER_MEMORY_CONFIG = "33554432";

    private static final String MAX_REQUEST_SIZE_CONFIG = "kafka.max.request.size";
    private static final String DEFAULT_MAX_REQUEST_SIZE_CONFIG = "1048576";

    private static final String COMPRESSION_TYPE_CONFIG = "kafka.compression.type";
    private static final String DEFAULT_COMPRESSION_TYPE_CONFIG = "gzip";

    private static final String ACKS_CONFIG = "kafka.acks";
    private static final String DEFAULT_ACKS_CONFIG = "1";

    private TripServiceDynamicConfig() {}

    private static class TripServiceDynamicConfigHolder {
        public static final TripServiceDynamicConfig INSTANCE = new TripServiceDynamicConfig();
    }

    public static TripServiceDynamicConfig getInstance() {
        return TripServiceDynamicConfigHolder.INSTANCE;
    }

    public boolean isSubenvEnable() {
        String subenvSwitch = getProperty(SUBENV_SWITCH, "off");
        return subenvSwitch.equalsIgnoreCase("on");
    }

    public String getKafkaAppidToken(){
        return getProperty(KAFKA_APPID_TOKEN, "");
    }

    public String getKafkaLingerMs(String topic) {
        return getDefaultStringValue(LINGER_MS_CONFIG, topic, DEFAULT_LINGER_MS_CONFIG);
    }


    public String getKafkaBatchSize(String topic) {
        return getDefaultStringValue(BATCH_SIZE_CONFIG, topic, DEFAULT_BATCH_SIZE_CONFIG);
    }

    public String getKafkaBufferMemory(String topic) {
        return getDefaultStringValue(BUFFER_MEMORY_CONFIG, topic, DEFAULT_BUFFER_MEMORY_CONFIG);
    }

    public String getKafkaMaxRequestSize(String topic) {
        return getDefaultStringValue(MAX_REQUEST_SIZE_CONFIG, topic, DEFAULT_MAX_REQUEST_SIZE_CONFIG);
    }

    public String getCompressionType(String topic) {
        return getDefaultStringValue(COMPRESSION_TYPE_CONFIG, topic, DEFAULT_COMPRESSION_TYPE_CONFIG);
    }

    public String getAcks(String topic) {
        return getDefaultStringValue(ACKS_CONFIG, topic, DEFAULT_ACKS_CONFIG);
    }

    public String getDefaultStringValue(String prefix, String key, String defaultValue) {
        String defaultResult = getProperty(prefix, defaultValue);
        String compositeKey = String.format(prefix + ".%s", key);
        return getProperty(compositeKey, defaultResult);
    }

    public boolean getAddKafkaCodecTypeSwitch(String topic) {
        String condecTypeSwitch = getDefaultStringValue("kafka.codectype.switch", topic, "on");
        return condecTypeSwitch.equalsIgnoreCase("on");
    }

}