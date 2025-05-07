package com.ctrip.framework.drc.replicator.impl.oubound.filter.sender;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.monitor.kpi.OutboundMonitorReport;
import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.ctrip.framework.drc.replicator.impl.oubound.binlog.BinlogSender;
import com.ctrip.framework.drc.replicator.impl.oubound.channel.ChannelAttributeKey;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.OutFilterChainContext;
import com.ctrip.framework.drc.replicator.impl.oubound.handler.binlog.DefaultBinlogSender;
import com.ctrip.xpipe.utils.VisibleForTesting;
import io.netty.channel.Channel;

/**
 * @Author limingdong
 * @create 2022/4/22
 */
public class SenderFilterChainContext implements OutFilterChainContext {

    private BinlogSender binlogSender;
    private String registerKey;

    private Channel channel;

    private ConsumeType consumeType;

    private DataMediaConfig dataMediaConfig;

    private OutboundMonitorReport outboundMonitorReport;

    private GtidSet excludedSet;

    private ChannelAttributeKey channelAttributeKey;

    private String srcRegion;

    private String dstRegion;

    @VisibleForTesting
    public SenderFilterChainContext() {
    }

    public SenderFilterChainContext(BinlogSender binlogSender, Channel channel,
                                    DataMediaConfig dataMediaConfig, OutboundMonitorReport outboundMonitorReport,
                                    String srcRegion, String dstRegion) {
        this.binlogSender = binlogSender;
        this.registerKey = binlogSender.getApplierName();
        this.consumeType = binlogSender.getConsumeType();
        this.excludedSet = binlogSender.getGtidSet();
        this.channelAttributeKey = binlogSender.getChannelAttributeKey();
        this.channel = channel;
        this.dataMediaConfig = dataMediaConfig;
        this.outboundMonitorReport = outboundMonitorReport;
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

    public static SenderFilterChainContext from(DefaultBinlogSender binlogSender, Channel channel,
                                                DataMediaConfig dataMediaConfig,
                                                OutboundMonitorReport outboundMonitorReport,
                                                String srcRegion,
                                                String dstRegion) {
        return new SenderFilterChainContext(binlogSender, channel, dataMediaConfig, outboundMonitorReport,
                srcRegion, dstRegion);
    }

    public GtidSet getExcludedSet() {
        return excludedSet;
    }

    public ChannelAttributeKey getChannelAttributeKey() {
        return channelAttributeKey;
    }

    public String getSrcRegion() {
        return srcRegion;
    }

    public String getDstRegion() {
        return dstRegion;
    }

    public BinlogSender getBinlogSender() {
        return binlogSender;
    }

    @VisibleForTesting
    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    @VisibleForTesting
    public void setConsumeType(ConsumeType consumeType) {
        this.consumeType = consumeType;
    }
    @VisibleForTesting
    public void setBinlogSender(BinlogSender binlogSender) {
        this.binlogSender = binlogSender;
    }
}
