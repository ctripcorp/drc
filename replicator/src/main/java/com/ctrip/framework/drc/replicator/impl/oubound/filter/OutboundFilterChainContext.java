package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.monitor.kpi.OutboundMonitorReport;
import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.ctrip.framework.drc.replicator.impl.oubound.channel.ChannelAttributeKey;
import io.netty.channel.Channel;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public class OutboundFilterChainContext {

    private String registerKey;

    private Channel channel;

    private ConsumeType consumeType;

    private DataMediaConfig dataMediaConfig;

    private OutboundMonitorReport outboundMonitorReport;

    private GtidSet excludedSet;

    private boolean skipDrcGtidLogEvent;

    private AviatorRegexFilter aviatorFilter;

    private ChannelAttributeKey channelAttributeKey;

    private String srcRegion;

    private String dstRegion;

    public OutboundFilterChainContext(String registerKey, Channel channel, ConsumeType consumeType,
                                      DataMediaConfig dataMediaConfig, OutboundMonitorReport outboundMonitorReport,
                                      GtidSet excludedSet, boolean skipDrcGtidLogEvent, AviatorRegexFilter aviatorFilter,
                                      ChannelAttributeKey channelAttributeKey, String srcRegion, String dstRegion) {
        this.registerKey = registerKey;
        this.channel = channel;
        this.consumeType = consumeType;
        this.dataMediaConfig = dataMediaConfig;
        this.outboundMonitorReport = outboundMonitorReport;
        this.excludedSet = excludedSet;
        this.skipDrcGtidLogEvent = skipDrcGtidLogEvent;
        this.aviatorFilter = aviatorFilter;
        this.channelAttributeKey = channelAttributeKey;
        this.srcRegion = srcRegion;
        this.dstRegion = dstRegion;
    }

    public String getRegisterKey() {
        return registerKey;
    }

    public void setRegisterKey(String registerKey) {
        this.registerKey = registerKey;
    }

    public Channel getChannel() {
        return channel;
    }

    public ConsumeType getConsumeType() {
        return consumeType;
    }

    public boolean shouldExtract() {
        if (dataMediaConfig == null) {
            return false;
        }
        return dataMediaConfig.shouldFilterRows() || dataMediaConfig.shouldFilterColumns();
    }

    public boolean shouldFilterRows() {
        if (dataMediaConfig == null) {
            return false;
        }
        return dataMediaConfig.shouldFilterRows();
    }

    public boolean shouldFilterColumns() {
        if (dataMediaConfig == null) {
            return false;
        }
        return dataMediaConfig.shouldFilterColumns();
    }

    public DataMediaConfig getDataMediaConfig() {
        return dataMediaConfig;
    }

    public OutboundMonitorReport getOutboundMonitorReport() {
        return outboundMonitorReport;
    }

    public static OutboundFilterChainContext from(String registerKey, Channel channel, ConsumeType consumeType,
                                                  DataMediaConfig dataMediaConfig,
                                                  OutboundMonitorReport outboundMonitorReport, GtidSet excludedSet,
                                                  boolean skipDrcGtidLogEvent, AviatorRegexFilter aviatorFilter,
                                                  ChannelAttributeKey channelAttributeKey, String srcRegion,
                                                  String dstRegion) {
        return new OutboundFilterChainContext(registerKey, channel, consumeType, dataMediaConfig, outboundMonitorReport,
                excludedSet, skipDrcGtidLogEvent, aviatorFilter, channelAttributeKey, srcRegion, dstRegion);
    }

    public GtidSet getExcludedSet() {
        return excludedSet;
    }

    public void setExcludedSet(GtidSet excludedSet) {
        this.excludedSet = excludedSet;
    }

    public boolean isSkipDrcGtidLogEvent() {
        return skipDrcGtidLogEvent;
    }

    public void setSkipDrcGtidLogEvent(boolean skipDrcGtidLogEvent) {
        this.skipDrcGtidLogEvent = skipDrcGtidLogEvent;
    }

    public AviatorRegexFilter getAviatorFilter() {
        return aviatorFilter;
    }

    public void setAviatorFilter(AviatorRegexFilter aviatorFilter) {
        this.aviatorFilter = aviatorFilter;
    }

    public ChannelAttributeKey getChannelAttributeKey() {
        return channelAttributeKey;
    }

    public void setChannelAttributeKey(ChannelAttributeKey channelAttributeKey) {
        this.channelAttributeKey = channelAttributeKey;
    }

    public String getSrcRegion() {
        return srcRegion;
    }

    public void setSrcRegion(String srcRegion) {
        this.srcRegion = srcRegion;
    }

    public String getDstRegion() {
        return dstRegion;
    }

    public void setDstRegion(String dstRegion) {
        this.dstRegion = dstRegion;
    }
}
