package com.ctrip.framework.drc.replicator.impl.oubound.filter.scanner;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
import com.ctrip.framework.drc.replicator.impl.oubound.binlog.AbstractBinlogScanner;
import com.ctrip.framework.drc.replicator.impl.oubound.binlog.BinlogScanner;
import com.ctrip.framework.drc.replicator.impl.oubound.binlog.BinlogSender;
import com.ctrip.framework.drc.replicator.impl.oubound.channel.ChannelAttributeKey;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.context.FileChannelContext;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.context.FilterChainContext;
import com.ctrip.xpipe.utils.VisibleForTesting;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author yongnian
 */
public class ScannerFilterChainContext implements FilterChainContext, FileChannelContext {

    private  BinlogScanner scanner;
    private String registerKey;
    private ConsumeType consumeType;
    private GtidSet excludedSet;
    private List<ChannelAttributeKey> channelAttributeKeys;

    @VisibleForTesting
    public ScannerFilterChainContext() {
    }


    public ScannerFilterChainContext(String registerKey, ConsumeType consumeType,
                                     GtidSet excludedSet, AbstractBinlogScanner scanner) {
        this.registerKey = registerKey;
        this.consumeType = consumeType;
        this.excludedSet = excludedSet;
        this.scanner = scanner;
        this.channelAttributeKeys = scanner.getSenders().stream().map(BinlogSender::getChannelAttributeKey).collect(Collectors.toList());
    }


    public static ScannerFilterChainContext from(String registerKey, ConsumeType consumeType,
                                                 GtidSet excludedSet,
                                                 AbstractBinlogScanner scanner) {
        return new ScannerFilterChainContext(registerKey, consumeType, excludedSet, scanner);
    }


    public String getRegisterKey() {
        return registerKey;
    }

    public void setRegisterKey(String registerKey) {
        this.registerKey = registerKey;
    }


    public ConsumeType getConsumeType() {
        return consumeType;
    }


    public GtidSet getExcludedSet() {
        return excludedSet;
    }



    public List<ChannelAttributeKey> getChannelAttributeKey() {
        return channelAttributeKeys;
    }

    public BinlogScanner getScanner() {
        return scanner;
    }

    @VisibleForTesting
    public void setScanner(BinlogScanner scanner) {
        this.scanner = scanner;
    }
}
