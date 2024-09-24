package com.ctrip.framework.drc.replicator.impl.oubound.filter;

import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.binlog.impl.*;
import com.ctrip.framework.drc.core.driver.util.LogEventUtils;
import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.monitor.kpi.OutboundMonitorReport;
import com.ctrip.framework.drc.core.monitor.log.Frequency;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.server.common.EventReader;
import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
import com.ctrip.framework.drc.core.server.common.filter.AbstractLogEventFilter;
import com.ctrip.framework.drc.core.server.common.filter.AbstractPostLogEventFilter;
import com.ctrip.framework.drc.core.server.common.filter.Filter;
import com.ctrip.framework.drc.core.server.common.filter.FilterChainFactory;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.ctrip.framework.drc.replicator.impl.oubound.channel.BinlogFileRegion;
import com.ctrip.framework.drc.replicator.impl.oubound.channel.ChannelAttributeKey;
import com.ctrip.framework.drc.replicator.impl.oubound.filter.extract.ExtractFilter;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventHeaderLength.eventHeaderLengthVersionGt1;
import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.*;
import static com.ctrip.framework.drc.core.driver.binlog.impl.FilterLogEvent.UNKNOWN;
import static com.ctrip.framework.drc.core.driver.util.LogEventUtils.isDrcGtidLogEvent;
import static com.ctrip.framework.drc.core.driver.util.LogEventUtils.isOriginGtidLogEvent;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.DRC_MONITOR_SCHEMA_NAME;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.GTID_LOGGER;

/**
 * preFilter
 * ReadFilter -> IndexFilter -> SkipFilter -> TypeFilter -> SchemaFilter -> TableNameFilter -> ExtractFilter
 * <p>
 * postFilter
 * MonitorFilter -> SendFilter
 *
 * @Author limingdong
 * @create 2022/4/22
 */
public class OutboundFilterChainFactory implements FilterChainFactory<OutboundFilterChainFactory.OutboundFilterChainContext, OutboundLogEventContext> {

    @Override
    public Filter<OutboundLogEventContext> createFilterChain(OutboundFilterChainContext context) {
        MonitorFilter monitorFilter = new MonitorFilter(context);

        OldLocalSendFilter sendFilter = new OldLocalSendFilter(context);
        monitorFilter.setSuccessor(sendFilter);

        ReadFilter readFilter = new ReadFilter(context.getRegisterKey());
        sendFilter.setSuccessor(readFilter);

        OldIndexFilter indexFilter = new OldIndexFilter(context.getExcludedSet());
        readFilter.setSuccessor(indexFilter);

        OldSkipFilter skipFilter = new OldSkipFilter(context);
        indexFilter.setSuccessor(skipFilter);

        TypeFilter consumeTypeFilter = new TypeFilter(context.getConsumeType());
        skipFilter.setSuccessor(consumeTypeFilter);

        if (ConsumeType.Replicator != context.getConsumeType()) {
            OldSchemaFilter schemaFilter = new OldSchemaFilter(context);
            consumeTypeFilter.setSuccessor(schemaFilter);

            OldTableNameFilter tableNameFilter = new OldTableNameFilter(context.getAviatorFilter());
            schemaFilter.setSuccessor(tableNameFilter);

            if (context.shouldExtract()) {
                ExtractFilter extractFilter = new ExtractFilter(context);
                tableNameFilter.setSuccessor(extractFilter);
            }
        }

        return monitorFilter;
    }

    public static class OldLocalSendFilter extends OldSendFilter implements LocalHistoryForTest {

        public final String name;
        private final ConsumeType consumeType;

        private static final Map<String, List<OutboundLogEventContext>> historyMap = new ConcurrentHashMap<>();

        public OldLocalSendFilter(OutboundFilterChainContext context) {
            super(context);
            this.name = context.getRegisterKey();
            this.consumeType = context.getConsumeType();
            if (!historyMap.containsKey(name)) {
                historyMap.put(name, new ArrayList<>());
            }
        }

        @Override
        protected void doSend(OutboundLogEventContext value) {
            super.doSend(value);
            historyMap.get(name).add(clone(value));
        }

        @Override
        public List<OutboundLogEventContext> getHistory(String name) {
            return historyMap.get(name);
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public ConsumeType getConsumeType() {
            return consumeType;
        }

        private OutboundLogEventContext clone(OutboundLogEventContext value) {
            OutboundLogEventContext clone = new OutboundLogEventContext();
            clone.setEventType(value.eventType);
            clone.setRewrite(value.isRewrite());
            clone.setLogEvent(value.logEvent);
            clone.setCause(value.getCause());
            clone.setSkipEvent(value.isSkipEvent());
            clone.setNoRewrite(value.isNoRewrite());
            clone.setGtid(value.getGtid());
            clone.setEverSeeGtid(value.isEverSeeGtid());
            return clone;
        }


    }

    /**
     * gtid、query、tablemap1、tablemap2、rows1、rows2、xid
     * <p>
     * Created by jixinwang on 2023/10/11
     */
    public static class OldTableNameFilter extends AbstractLogEventFilter<OutboundLogEventContext> {

        private Map<Long, TableMapLogEvent> skipRowsRelatedTableMap = Maps.newHashMap();

        private AviatorRegexFilter aviatorFilter;

        private boolean needFilter;

        private LogEventType lastEventType;

        public OldTableNameFilter(AviatorRegexFilter aviatorFilter) {
            this.aviatorFilter = aviatorFilter;
            this.needFilter = aviatorFilter != null;
        }

        @Override
        public boolean doFilter(OutboundLogEventContext value) {
            LogEventType eventType = value.getEventType();

            if (table_map_log_event == eventType) {
                filterTableMapEvent(value);
            } else if (LogEventUtils.isRowsEvent(eventType)) {
                filterRowsEvent(value);
            } else if (xid_log_event == value.getEventType()) {
                value.getRowsRelatedTableMap().clear();
                skipRowsRelatedTableMap.clear();
            }

            lastEventType = eventType;
            return doNext(value, value.isSkipEvent());
        }

        private void filterTableMapEvent(OutboundLogEventContext value) {
            Map<Long, TableMapLogEvent> rowsRelatedTableMap = value.getRowsRelatedTableMap();

            if (isFirstRowsRelatedTableMapEvent()) {
                rowsRelatedTableMap.clear();
                skipRowsRelatedTableMap.clear();
            }

            TableMapLogEvent tableMapLogEvent = value.readTableMapEvent();
            value.setLogEvent(tableMapLogEvent);
            rowsRelatedTableMap.put(tableMapLogEvent.getTableId(), tableMapLogEvent);

            if (shouldSkipTableMapEvent(tableMapLogEvent.getSchemaNameDotTableName())) {
                skipRowsRelatedTableMap.put(tableMapLogEvent.getTableId(), tableMapLogEvent);
                value.setSkipEvent(true);
                GTID_LOGGER.debug("[Skip] table map event {} for name filter", tableMapLogEvent.getSchemaNameDotTableName());
            }
        }

        private boolean isFirstRowsRelatedTableMapEvent() {
            return table_map_log_event != lastEventType;
        }

        private boolean shouldSkipTableMapEvent(String tableName) {
            return needFilter && !aviatorFilter.filter(tableName);
        }

        private boolean isSingleRowsRelatedTableMap(Map<Long, TableMapLogEvent> rowsRelatedTableMap) {
            return rowsRelatedTableMap.size() == 1;
        }

        private void filterRowsEvent(OutboundLogEventContext value) {
            Map<Long, TableMapLogEvent> rowsRelatedTableMap = value.getRowsRelatedTableMap();
            if (skipRowsRelatedTableMap.isEmpty()) {
                return;
            }

            //trigger has multi rowsRelatedTableMapEvents
            if (isSingleRowsRelatedTableMap(rowsRelatedTableMap)) {
                value.setSkipEvent(true);
            } else {
                AbstractRowsEvent rowsEvent = value.readRowsEvent();
                rowsEvent.loadPostHeader();

                TableMapLogEvent relatedTableMapEvent = skipRowsRelatedTableMap.get(rowsEvent.getRowsEventPostHeader().getTableId());
                if (relatedTableMapEvent != null) {
                    value.setSkipEvent(true);
                    GTID_LOGGER.info("[Skip] rows event {} for name filter", relatedTableMapEvent);
                }
            }
        }
    }

    /**
     * Created by jixinwang on 2023/11/20
     */
    public static class OldSchemaFilter extends AbstractLogEventFilter<OutboundLogEventContext> {

        private String registerKey;

        private Set<String> schemas = Sets.newHashSet(UNKNOWN, DRC_MONITOR_SCHEMA_NAME);

        public OldSchemaFilter(OutboundFilterChainContext context) {
            registerKey = context.getRegisterKey();
            String nameFilter = context.getNameFilter();
            initSchemas(nameFilter);
            logger.info("[Filter][Schema] init send schemas: {} for {}", schemas, registerKey);
        }

        @Override
        public boolean doFilter(OutboundLogEventContext value) {
            if (drc_filter_log_event == value.getEventType()) {
                FilterLogEvent filterLogEvent = value.readFilterEvent();
                value.setLogEvent(filterLogEvent);
                String schema = filterLogEvent.getSchemaNameLowerCaseV2();
                if (!schemas.contains(schema.toLowerCase())) {
                    long nextTransactionOffset = filterLogEvent.getNextTransactionOffset();
                    if (nextTransactionOffset > 0) {
                        value.skipPositionAfterReadEvent(nextTransactionOffset);
                        GTID_LOGGER.debug("[S][{}] filter schema, {}", registerKey, schema);
                    }
                }
                value.setSkipEvent(true);
            }

            return doNext(value, value.isSkipEvent());
        }

        private void initSchemas(String nameFilter) {
            logger.info("[SCHEMA][ADD] for {}, nameFilter {}", registerKey, nameFilter);
            String[] schemaDotTableNames = nameFilter.split(",");

            for (String schemaDotTableName : schemaDotTableNames) {
                String[] schemaAndTable = schemaDotTableName.split("\\\\.");
                if (schemaAndTable.length > 1) {
                    schemas.add(schemaAndTable[0].toLowerCase());
                    logger.info("[SCHEMA][ADD] for {}, schema {}", registerKey, schemaAndTable[0]);
                    continue;
                }

                String[] schemaAndTable2 = schemaDotTableName.split("\\.");
                if (schemaAndTable2.length > 1) {
                    schemas.add(schemaAndTable2[0].toLowerCase());
                    logger.info("[SCHEMA][ADD] for {}, schema {}", registerKey, schemaAndTable2[0]);
                }
            }
        }

        @VisibleForTesting
        protected Set<String> getSchemas() {
            return schemas;
        }
    }

    /**
     * @Author limingdong
     * @create 2022/4/22
     */
    public static class OldSendFilter extends AbstractPostLogEventFilter<OutboundLogEventContext> {

        private Channel channel;

        public OldSendFilter(OutboundFilterChainContext context) {
            this.channel = context.getChannel();
        }

        @Override
        public boolean doFilter(OutboundLogEventContext value) {
            boolean skipEvent = doNext(value, value.isSkipEvent());

            if (value.getCause() != null) {
                return true;
            }

            if (value.getLogEvent() == null) {
                try {
                    value.getFileChannel().position(value.getFileChannelPos() + value.getEventSize());
                } catch (IOException e) {
                    logger.error("skip position error:", e);
                    value.setCause(e);
                    value.setSkipEvent(true);
                    return true;
                }
            }

            if (skipEvent) {
                return true;
            }

            doSend(value);

            return false;
        }

        protected void doSend(OutboundLogEventContext value) {
            if (value.isRewrite()) {
                sendRewriteEvent(value);
            } else {
                channel.writeAndFlush(new BinlogFileRegion(value.getFileChannel(), value.getFileChannelPos(), value.getEventSize()).retain());
            }
        }

        private void sendRewriteEvent(OutboundLogEventContext value) {
            value.getLogEvent().write(byteBufs -> {
                for (ByteBuf byteBuf : byteBufs) {
                    byteBuf.readerIndex(0);
                    ChannelFuture future = channel.writeAndFlush(byteBuf);
                    future.addListener((GenericFutureListener) f -> {
                        if (!f.isSuccess()) {
                            channel.close();
                            logger.error("[Send] {} error", channel, f.cause());
                        }
                    });
                }
            });
        }
    }




    /**
     * Created by jixinwang on 2023/10/11
     */
    public static class OldIndexFilter extends AbstractLogEventFilter<OutboundLogEventContext> {

        private GtidSet excludedSet;

        public OldIndexFilter(GtidSet excludedSet) {
            this.excludedSet = excludedSet;
        }

        @Override
        public boolean doFilter(OutboundLogEventContext value) {

            if (LogEventUtils.isIndexEvent(value.getEventType())) { //first file and skip to first previous gtid event
                if (!value.isEverSeeGtid()) {
                    try {
                        trySkip(value);
                    } catch (IOException e) {
                        value.setCause(e);
                    }
                }
                value.setSkipEvent(true);
            }

            return doNext(value, value.isSkipEvent());
        }

        private void trySkip(OutboundLogEventContext value) throws IOException {
            FileChannel fileChannel = value.getFileChannel();
            DrcIndexLogEvent indexLogEvent = value.readIndexLogEvent();
            long currentPosition = fileChannel.position();

            List<Long> indices = indexLogEvent.getIndices();
            if (indices.size() > 1) {
                GtidSet firstGtidSet = readPreviousGtids(fileChannel, indices.get(0));
                for (int i = 1; i < indices.size(); ++i) {
                    if (indices.get(i).equals(indices.get(i - 1))) {
                        restorePosition(fileChannel, indices.get(i - 1), currentPosition);
                        break;
                    }
                    GtidSet secondGtidSet = readPreviousGtids(fileChannel, indices.get(i));
                    GtidSet stepGtidSet = secondGtidSet.subtract(firstGtidSet);
                    if (stepGtidSet.isContainedWithin(excludedSet)) {
                        logger.info("[GtidSet] update from {} to {}", firstGtidSet, secondGtidSet);
                        firstGtidSet = secondGtidSet;
                    } else {  // restore to last position
                        restorePosition(fileChannel, indices.get(i - 1), currentPosition);
                        break;
                    }
                }
            }
        }

        private void restorePosition(FileChannel fileChannel, long restorePosition, long currentPosition) throws IOException {
            logger.info("restorePosition is {} and currentPosition is {}", restorePosition, currentPosition);
            restorePosition = Math.max(restorePosition, currentPosition);
            fileChannel.position(restorePosition);
            logger.info("[restorePosition] set to {} finally", restorePosition);
        }

        private GtidSet readPreviousGtids(FileChannel fileChannel, long position) throws IOException {
            PreviousGtidsLogEvent previousGtidsLogEvent = new PreviousGtidsLogEvent();
            try {
                fileChannel.position(position);
                logger.info("[Update] position of fileChannel to {}", position);
                EventReader.readEvent(fileChannel, previousGtidsLogEvent);
                return previousGtidsLogEvent.getGtidSet();
            } finally {
                previousGtidsLogEvent.release();
            }
        }
    }

    /**
     * Created by jixinwang on 2023/10/12
     */
    public static class OldSkipFilter extends AbstractLogEventFilter<OutboundLogEventContext> {

        private Frequency frequencySend = new Frequency("FRE GTID SEND");

        private GtidSet excludedSet;

        private boolean skipDrcGtidLogEvent;

        private ConsumeType consumeType;

        private String previousGtid = StringUtils.EMPTY;

        private boolean inExcludeGroup = false;

        private ChannelAttributeKey channelAttributeKey;

        private String registerKey;

        public OldSkipFilter(OutboundFilterChainContext context) {
            this.excludedSet = Objects.requireNonNullElseGet(context.getExcludedSet(), () -> new GtidSet(StringUtils.EMPTY));
            this.skipDrcGtidLogEvent = context.isSkipDrcGtidLogEvent();
            this.consumeType = context.getConsumeType();
            this.registerKey = context.getRegisterKey();
            this.channelAttributeKey = context.getChannelAttributeKey();
        }

        @Override
        public boolean doFilter(OutboundLogEventContext value) {
            LogEventType eventType = value.getEventType();

            if (LogEventUtils.isGtidLogEvent(eventType)) {
                handleGtidEvent(value, eventType);
            } else {
                handleNonGtidEvent(value, eventType);
            }

            if (inExcludeGroup) {
                channelAttributeKey.handleEvent(false);
            } else {
                channelAttributeKey.handleEvent(true);
                //            logGtid(previousGtid, eventType);
            }

            return doNext(value, value.isSkipEvent());
        }

        private void handleGtidEvent(OutboundLogEventContext value, LogEventType eventType) {
            value.setEverSeeGtid(true);
            GtidLogEvent gtidLogEvent = value.readGtidEvent();
            value.setLogEvent(gtidLogEvent);
            value.setGtid(gtidLogEvent.getGtid());
            previousGtid = gtidLogEvent.getGtid();

            inExcludeGroup = skipEvent(excludedSet, eventType, gtidLogEvent.getGtid());
            if (inExcludeGroup) {
                GTID_LOGGER.info("[Skip] gtid log event, gtid:{}, lastCommitted:{}, sequenceNumber:{}, type:{}", previousGtid, gtidLogEvent.getLastCommitted(), gtidLogEvent.getSequenceNumber(), eventType);
                DefaultEventMonitorHolder.getInstance().logEvent("DRC.replicator.outbound.gtid.skip", registerKey);
                value.setSkipEvent(true);
                long nextTransactionOffset = gtidLogEvent.getNextTransactionOffset();
                if (nextTransactionOffset > 0) {
                    value.skipPositionAfterReadEvent(nextTransactionOffset);
                    inExcludeGroup = false;
                }
                return;
            }

            if (drc_gtid_log_event == eventType && !consumeType.requestAllBinlog()) {
                value.setSkipEvent(true);
                inExcludeGroup = true;
            }
        }

        private void handleNonGtidEvent(OutboundLogEventContext value, LogEventType eventType) {
            if (inExcludeGroup && !LogEventUtils.isSlaveConcerned(eventType)) {
                value.skipPosition(value.getEventSize() - eventHeaderLengthVersionGt1);
                value.setSkipEvent(true);

                //skip all transaction, clear in_exclude_group
                if (xid_log_event == eventType) {
                    GTID_LOGGER.info("[Reset] in_exclude_group to false, gtid:{}", previousGtid);
                    inExcludeGroup = false;
                }
            }
        }

        private boolean skipEvent(GtidSet excludedSet, LogEventType eventType, String gtid) {
            if (eventType == gtid_log_event) {
                return new GtidSet(gtid).isContainedWithin(excludedSet);
            }

            if (eventType == drc_gtid_log_event) {
                return skipDrcGtidLogEvent || new GtidSet(gtid).isContainedWithin(excludedSet);
            }
            return inExcludeGroup;
        }

        private void logGtid(String gtidForLog, LogEventType eventType) {
            if (xid_log_event == eventType) {
                GTID_LOGGER.debug("[S] X, {}", gtidForLog);
            } else if (isOriginGtidLogEvent(eventType)) {
                frequencySend.addOne();
                if (StringUtils.isNotBlank(gtidForLog)) {
                    GTID_LOGGER.info("[S] G, {}", gtidForLog);
                }
            } else if (isDrcGtidLogEvent(eventType)) {
                frequencySend.addOne();
                if (StringUtils.isNotBlank(gtidForLog)) {
                    GTID_LOGGER.info("[S] drc G, {}", gtidForLog);
                }
            } else if (LogEventUtils.isDrcTableMapLogEvent(eventType)) {
                GTID_LOGGER.info("[S] drc table map, {}", gtidForLog);
            } else if (LogEventUtils.isDrcDdlLogEvent(eventType)) {
                GTID_LOGGER.info("[S] drc ddl, {}", gtidForLog);
            }
        }
    }

    /**
     * @Author limingdong
     * @create 2022/4/22
     */
    public static class OutboundFilterChainContext implements OutFilterChainContext {

        private String registerKey;

        private Channel channel;

        private ConsumeType consumeType;

        private DataMediaConfig dataMediaConfig;

        private OutboundMonitorReport outboundMonitorReport;

        private GtidSet excludedSet;

        private boolean skipDrcGtidLogEvent;

        private AviatorRegexFilter aviatorFilter;

        private String nameFilter;

        private ChannelAttributeKey channelAttributeKey;

        private String srcRegion;

        private String dstRegion;

        @VisibleForTesting
        public OutboundFilterChainContext() {
        }

        public OutboundFilterChainContext(String registerKey, Channel channel, ConsumeType consumeType,
                                          DataMediaConfig dataMediaConfig, OutboundMonitorReport outboundMonitorReport,
                                          GtidSet excludedSet, boolean skipDrcGtidLogEvent, String nameFilter,
                                          AviatorRegexFilter aviatorFilter, ChannelAttributeKey channelAttributeKey,
                                          String srcRegion, String dstRegion) {
            this.registerKey = registerKey;
            this.channel = channel;
            this.consumeType = consumeType;
            this.dataMediaConfig = dataMediaConfig;
            this.outboundMonitorReport = outboundMonitorReport;
            this.excludedSet = excludedSet;
            this.skipDrcGtidLogEvent = skipDrcGtidLogEvent;
            this.nameFilter = nameFilter;
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
                                                      boolean skipDrcGtidLogEvent, String nameFilter,
                                                      AviatorRegexFilter aviatorFilter,
                                                      ChannelAttributeKey channelAttributeKey, String srcRegion,
                                                      String dstRegion) {
            return new OutboundFilterChainContext(registerKey, channel, consumeType, dataMediaConfig, outboundMonitorReport,
                    excludedSet, skipDrcGtidLogEvent, nameFilter, aviatorFilter, channelAttributeKey, srcRegion, dstRegion);
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

        public String getNameFilter() {
            return nameFilter;
        }

        public void setNameFilter(String nameFilter) {
            this.nameFilter = nameFilter;
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

        public void setChannel(Channel channel) {
            this.channel = channel;
        }
    }
}
