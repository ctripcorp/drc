package com.ctrip.framework.drc.core.mq;

import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.meta.MqConfig;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.ctrip.xpipe.codec.JsonCodec;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * @ClassName MessengerProperties
 * @Author haodongPan
 * @Date 2022/10/9 11:08
 * @Version: $
 */
public class MessengerProperties {

    private String nameFilter;

    private DataMediaConfig dataMediaConfig;

    private List<MqConfig> mqConfigs;

    @JsonIgnore
    private static final String DELAY_MONITOR_TABLE = "drcmonitordb.delaymonitor";

    @JsonIgnore
    private static final String DELAY_MONITOR_TOPIC = "bbz.drc.delaymonitor";

    @JsonIgnore
    private Map<String, MqConfig> table2Config = Maps.newConcurrentMap();  // regex : mqConfig

    @JsonIgnore
    private Map<String, AviatorRegexFilter> table2Filter = Maps.newConcurrentMap();  // regex : AviatorRegexFilter

    @JsonIgnore
    private Map<String, Producer> table2Producer = Maps.newConcurrentMap();  // regex : Producer

    @JsonIgnore
    private Map<String, List<Producer>> tableName2Producers = Maps.newConcurrentMap(); // tableName :  Producers

    public String getNameFilter() {
        return nameFilter;
    }

    public void setNameFilter(String nameFilter) {
        this.nameFilter = nameFilter;
    }

    public DataMediaConfig getDataMediaConfig() {
        return dataMediaConfig;
    }

    public void setDataMediaConfig(DataMediaConfig dataMediaConfig) {
        this.dataMediaConfig = dataMediaConfig;
    }

    public List<MqConfig> getMqConfigs() {
        return mqConfigs;
    }

    public void setMqConfigs(List<MqConfig> mqConfigs) {
        this.mqConfigs = mqConfigs;
    }

    public static MessengerProperties from(String properties) throws Exception {
        MessengerProperties messengerProperties;
        if (StringUtils.isNotBlank(properties)) {
            messengerProperties = JsonCodec.INSTANCE.decode(properties, MessengerProperties.class);
        } else {
            messengerProperties = new MessengerProperties();
        }
        messengerProperties.parse();
        return messengerProperties;
    }

    private void parse() throws Exception {
        if (mqConfigs == null) {
            return;
        }
        for (MqConfig mqConfig : mqConfigs) {
            String tableRegex = mqConfig.getTable().trim().toLowerCase();
            table2Config.put(tableRegex, mqConfig);
            table2Filter.put(tableRegex, new AviatorRegexFilter(tableRegex));
        }
    }

    public List<Producer> getProducers(String tableName) {
        String formatTableName = tableName.toLowerCase();
        List<Producer> producers = tableName2Producers.get(formatTableName);
        if (producers == null) {
            if (DELAY_MONITOR_TABLE.equalsIgnoreCase(formatTableName)) {
                Producer monitorProducer = createMonitorProducer();
                List<Producer> monitorProducers = Lists.newArrayList(monitorProducer);
                tableName2Producers.put(formatTableName, monitorProducers);
                return monitorProducers;
            }

            List<Producer> toCreateProducers = Lists.newArrayList();
            for (Map.Entry<String, MqConfig> entry : table2Config.entrySet()) {
                String tableRegex = entry.getKey().trim().toLowerCase();
                if (table2Filter.get(tableRegex).filter(formatTableName)) {
                    Producer producer = table2Producer.get(tableRegex);
                    if (producer == null) {
                        producer = DefaultProducerFactoryHolder.getInstance().createProducer(table2Config.get(formatTableName));
                        table2Producer.put(tableRegex, producer);
                    }
                    toCreateProducers.add(producer);
                }
            }
            tableName2Producers.put(formatTableName, toCreateProducers);
            return toCreateProducers;
        }
        return producers;
    }

    private Producer createMonitorProducer() {
        MqConfig monitorMqConfig = new MqConfig();
        monitorMqConfig.setMqType(MqType.qmq.name());
        monitorMqConfig.setTopic(DELAY_MONITOR_TOPIC);
        monitorMqConfig.setSerialization("arvo");
        monitorMqConfig.setOrder(true);
        monitorMqConfig.setOrderKey("id");
        return DefaultProducerFactoryHolder.getInstance().createProducer(monitorMqConfig);
    }
}
