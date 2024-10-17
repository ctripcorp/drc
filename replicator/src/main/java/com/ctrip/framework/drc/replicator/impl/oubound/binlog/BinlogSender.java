package com.ctrip.framework.drc.replicator.impl.oubound.binlog;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
import com.ctrip.framework.drc.replicator.impl.oubound.channel.ChannelAttributeKey;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.OutboundLogEventContext;
import com.ctrip.framework.drc.replicator.store.manager.file.BinlogPosition;
import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import io.netty.channel.Channel;

/**
 * @author yongnian
 */
public interface BinlogSender extends Lifecycle {
    void send(OutboundLogEventContext context) throws Exception;

    void send(ResultCode resultCode);

    GtidSet getGtidSet();

    Channel getChannel();

    void updatePosition(BinlogPosition binlogPosition);

    void refreshInExcludedGroup(OutboundLogEventContext scannerContext);

    ConsumeType getConsumeType();

    String getNameFilter();

    String getApplierName();

    ChannelAttributeKey getChannelAttributeKey();

    boolean isRunning();

    boolean concernSchema(String dbName);

    boolean concernTable(String tableName);

    BinlogPosition getBinlogPosition();
}
