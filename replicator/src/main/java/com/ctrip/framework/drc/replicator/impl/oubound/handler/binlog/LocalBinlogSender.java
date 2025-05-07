package com.ctrip.framework.drc.replicator.impl.oubound.handler.binlog;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.driver.command.packet.applier.ApplierDumpCommandPacket;
import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.ctrip.framework.drc.replicator.impl.oubound.binlog.BinlogSender;
import com.ctrip.framework.drc.replicator.impl.oubound.channel.ChannelAttributeKey;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.OutboundLogEventContext;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.SchemaFilter;
import com.ctrip.framework.drc.replicator.store.manager.file.BinlogPosition;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

public class LocalBinlogSender extends AbstractLifecycle implements BinlogSender {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private GtidSet excludedSet;

    String name;

    // For test
    public Report report;

    private final Set<String> schemas;
    private final AviatorRegexFilter aviatorFilter;


    public LocalBinlogSender(Channel channel, ApplierDumpCommandPacket dumpCommandPacket) {
        this.excludedSet = dumpCommandPacket.getGtidSet();
        name = "[Sender] [" + excludedSet + "]";
        report = new Report(excludedSet.toString());
        this.schemas = SchemaFilter.getSchemas(dumpCommandPacket.getNameFilter());
        this.aviatorFilter = new AviatorRegexFilter(dumpCommandPacket.getNameFilter());

    }

    public GtidSet getGtidSet() {
        return excludedSet;
    }

    @Override
    public boolean isRunning() {
        return true;
    }

    @Override
    public boolean concernSchema(String dbName) {
        return schemas.contains(dbName);
    }

    @Override
    public Channel getChannel() {
        return null;
    }

    @Override
    public void updatePosition(BinlogPosition binlogPosition) {
    }

    @Override
    public void refreshInExcludedGroup(OutboundLogEventContext scannerContext) {
    }

    @Override
    public boolean concernTable(String tableName) {
        return aviatorFilter.filter(tableName);
    }

    @Override
    public BinlogPosition getBinlogPosition() {
        return null;
    }

    @Override
    public void dispose() throws Exception {

    }

    @Override
    public void send(OutboundLogEventContext context) {
        if (new GtidSet(context.getGtid()).isContainedWithin(this.excludedSet)) {
            return;
        }

        this.excludedSet.add(context.getGtid());
        logger.info("{} -- {}", name, context.getGtid());
        report.record(context.getGtid());
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void send(ResultCode resultCode) {
        logger.warn("send {}", resultCode);
    }


    @Override
    public String toString() {
        return "LocalBinlogSender{" +
                "name='" + name + '\'' +
                '}';
    }

    @Override
    public ConsumeType getConsumeType() {
        return ConsumeType.Applier;
    }

    @Override
    public String getNameFilter() {
        return "null";
    }

    @Override
    public String getApplierName() {
        return name;
    }

    @Override
    public ChannelAttributeKey getChannelAttributeKey() {
        return null;
    }

    public Report getReport() {
        return report;
    }


    public static class Report {
        public String initGtid;
        public List<String> sendGtidRecord = new CopyOnWriteArrayList<>();

        public Report(String initGtid) {
            this.initGtid = initGtid;
        }

        public void record(String gtid) {
            this.sendGtidRecord.add(gtid);
        }
    }
}
