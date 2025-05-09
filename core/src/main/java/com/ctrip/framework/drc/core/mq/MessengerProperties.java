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
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.DRC_DELAY_MONITOR_NAME;

/**
 * @ClassName MessengerProperties
 * @Author haodongPan
 * @Date 2022/10/9 11:08
 * @Version: $
 */
public class MessengerProperties {

    private String nameFilter;

    @JsonIgnore
    private DataMediaConfig dataMediaConfig;

    private List<MqConfig> mqConfigs;

    @JsonIgnore
    private Map<String, List<MqConfig>> regex2Configs = Maps.newConcurrentMap();  // regex : mqConfigs

    @JsonIgnore
    private Map<String, AviatorRegexFilter> regex2Filter = Maps.newConcurrentMap();  // regex : AviatorRegexFilter

    @JsonIgnore
    private Map<String, List<Producer>> regex2Producers = Maps.newConcurrentMap();  // regex : Producers

    @JsonIgnore
    private Map<String, List<Producer>> tableName2Producers = Maps.newConcurrentMap(); // tableName :  Producers

    @JsonIgnore
    private String mqType;

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

    public Map<String, List<Producer>> getRegex2Producers() {
        return regex2Producers;
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
        List<String> mqTypes = mqConfigs.stream().map(MqConfig::getMqType).distinct().collect(Collectors.toList());
        if (mqTypes.size() > 1) {
            throw new IllegalStateException("mqConfigs contains different mqTypes");
        }
        mqType = mqTypes.get(0);

        for (MqConfig mqConfig : mqConfigs) {
            String tableRegex = mqConfig.getTable().trim().toLowerCase();
            List<MqConfig> mqConfigs = regex2Configs.get(tableRegex);
            if (mqConfigs == null) {
                mqConfigs = Lists.newArrayList(mqConfig);
            } else {
                mqConfigs.add(mqConfig);
            }
            regex2Configs.put(tableRegex, mqConfigs);

            AviatorRegexFilter filter = regex2Filter.get(tableRegex);
            if (filter == null) {
                regex2Filter.put(tableRegex, new AviatorRegexFilter(tableRegex));
            }
        }

    }

    public List<Producer> getProducers(String tableName) {
        String formatTableName = tableName.toLowerCase();
        List<Producer> tableNameProducers = tableName2Producers.get(formatTableName);
        if (tableNameProducers != null) {
            return tableNameProducers;
        }

        if (DRC_DELAY_MONITOR_NAME.equalsIgnoreCase(formatTableName)) {
            return DelayMessageProducer.getInstance(mqType);
        }

        synchronized (this) {
            List<Producer> producers = Lists.newArrayList();
            if (tableName2Producers.get(formatTableName) != null) {
                return tableName2Producers.get(formatTableName);
            }
            for (Map.Entry<String, List<MqConfig>> entry : regex2Configs.entrySet()) {
                String tableRegex = entry.getKey().trim().toLowerCase();
                if (regex2Filter.get(tableRegex).filter(formatTableName)) {
                    List<Producer> regexProducers = regex2Producers.get(tableRegex);
                    if (regexProducers == null) {
                        List<Producer> newRegexProducers = Lists.newArrayList();
                        for (MqConfig mqConfig : entry.getValue()) {
                            Producer producer = DefaultProducerFactoryHolder.getInstance().createProducer(mqConfig);
                            producers.add(producer);
                            newRegexProducers.add(producer);
                        }
                        regex2Producers.put(tableRegex, newRegexProducers);
                    } else {
                        producers.addAll(regexProducers);
                    }
                }
            }
            tableName2Producers.put(formatTableName, producers);
            return producers;
        }
    }
}
