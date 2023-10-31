package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.util.ByteHelper;
import com.ctrip.framework.drc.core.driver.util.CharsetConversion;
import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by @author zhuYongMing on 2019/9/15.
 * see https://dev.mysql.com/doc/internals/en/query-event.html
 */
public class QueryLogEvent extends AbstractLogEvent {

    private long slaveProxyId;

    private long executeTime;

    private int schemaLength;

    private int errorCode;

    private int statusVarLength;

    private QueryStatus queryStatus;

    private String schemaName;

    private String query;

    private Long checksum;


    @Override
    public QueryLogEvent read(ByteBuf byteBuf) {
        final LogEvent logEvent = super.read(byteBuf);
        if (null == logEvent) {
            return null;
        }

        final ByteBuf payloadBuf = getPayloadBuf();
        // post-header 13 bytes
        this.slaveProxyId = payloadBuf.readUnsignedIntLE(); // 4
        this.executeTime = payloadBuf.readUnsignedIntLE();  // 4
        this.schemaLength = payloadBuf.readUnsignedByte(); // 1
        this.errorCode = payloadBuf.readUnsignedShortLE(); // 2
        this.statusVarLength = payloadBuf.readUnsignedShortLE(); // 2

        // payload
        this.queryStatus = new QueryStatus();
        final AtomicInteger readStatusVarLength = new AtomicInteger(0);
        while (readStatusVarLength.get() < statusVarLength) {
            parseQueryStatus(payloadBuf, queryStatus, readStatusVarLength, statusVarLength);
        }

        this.schemaName = ByteHelper.readZeroTerminalString(payloadBuf);

        final int queryLength = payloadBuf.readableBytes() - 4;
        final int clientCharset = queryStatus.getClientCharset();
        if (clientCharset >= 0) {
            String charset = CharsetConversion.getJavaCharset(clientCharset);
            if (null != charset && Charset.isSupported(charset)) {
                this.query = readFixLengthString(payloadBuf, queryLength, Charset.forName(charset));
            } else {
                this.query = readFixLengthStringDefaultCharset(payloadBuf, queryLength);
            }
        } else {
            this.query = readFixLengthStringDefaultCharset(payloadBuf, queryLength);
        }

        this.checksum = payloadBuf.readUnsignedIntLE(); // 4bytes;
        return this;
    }

    @Override
    public void write(IoCache ioCache) {
        super.write(ioCache);
    }

    //more QueryStatusCode in mysql 8.0
    private void parseQueryStatus(final ByteBuf payload, final QueryStatus queryStatus,
                                  final AtomicInteger readStatusVarLength, final int statusVarLength) {
        final QueryStatusCode code = QueryStatusCode.getQueryStatusCode(payload.readUnsignedByte());
        readStatusVarLength.addAndGet(1);

        switch (code) {
            case q_flags2_code:
                queryStatus.setFlags2(payload.readUnsignedIntLE());
                readStatusVarLength.addAndGet(4);
                break;
            case q_sql_mode_code:
                queryStatus.setSqlMode(payload.readLongLE());
                readStatusVarLength.addAndGet(8);
                break;
            case q_catalog: {
                final short length = payload.readUnsignedByte();
                readStatusVarLength.addAndGet(1);
                queryStatus.setCatalog(readFixLengthStringDefaultCharset(payload, length + 1));
                readStatusVarLength.addAndGet(length + 1);
                break;
            }
            case q_auto_increment:
                queryStatus.setAutoIncrementIncrement(payload.readUnsignedShortLE());
                readStatusVarLength.addAndGet(2);

                queryStatus.setAutoIncrementOffset(payload.readUnsignedShortLE());
                readStatusVarLength.addAndGet(2);
                break;
            case q_charset_code:
                queryStatus.setClientCharset(payload.readUnsignedShortLE());
                readStatusVarLength.addAndGet(2);

                queryStatus.setClientCollation(payload.readUnsignedShortLE());
                readStatusVarLength.addAndGet(2);

                queryStatus.setServerCollation(payload.readUnsignedShortLE());
                readStatusVarLength.addAndGet(2);
                break;
            case q_time_zone_code: {
                final short length = payload.readUnsignedByte();
                readStatusVarLength.addAndGet(1);
                queryStatus.setTimeZone(readFixLengthStringDefaultCharset(payload, length));
                readStatusVarLength.addAndGet(length);
                break;
            }
            case q_catalog_nz_code: {
                final short length = payload.readUnsignedByte();
                readStatusVarLength.addAndGet(1);
                queryStatus.setCatalog(readFixLengthStringDefaultCharset(payload, length));
                readStatusVarLength.addAndGet(length);
                break;
            }
            case q_lc_time_names_code:
                queryStatus.setLcTime(payload.readUnsignedShortLE());
                readStatusVarLength.addAndGet(2);
                break;
            case q_charset_database_code:
                queryStatus.setCharsetDatabase(payload.readUnsignedShortLE());
                readStatusVarLength.addAndGet(2);
                break;
            case q_table_map_for_update_code:
                queryStatus.setTableMapForUpdate(payload.readLongLE());
                readStatusVarLength.addAndGet(8);
                break;
            case q_master_data_written_code:
                queryStatus.setMasterDataWritten(payload.readUnsignedIntLE());
                readStatusVarLength.addAndGet(4);
                break;
            case q_invoker: {
                final short userLength = payload.readUnsignedByte();
                readStatusVarLength.addAndGet(1);
                queryStatus.setUser(readFixLengthStringDefaultCharset(payload, userLength));
                readStatusVarLength.addAndGet(userLength);

                final short hostLength = payload.readUnsignedByte();
                readStatusVarLength.addAndGet(1);
                queryStatus.setHost(readFixLengthStringDefaultCharset(payload, hostLength));
                readStatusVarLength.addAndGet(hostLength);

                break;
            }
            case q_update_db_names: {
                final int updateDbCount = payload.readUnsignedByte();
                readStatusVarLength.addAndGet(1);
                // ignore check updateDbCount
                // updateDbCount within [1, 16], MAX_DBS_IN_EVENT_MTS = 16, if updateDbCount ge MAX_DBS_IN_EVENT_MTS(16),
                // updateDbCount = OVER_MAX_DBS_IN_EVENT_MTS = 254

                final String[] updateDbNames = new String[updateDbCount];
                for (int i = 0; i < updateDbCount && readStatusVarLength.get() < statusVarLength; i++) {
                    // hardcode, believe update db count = 1, follow canal
                    int dbNameLength = statusVarLength - readStatusVarLength.get();
                    // maxDbNameLength = NAME_CHAR_LEN * SYSTEM_CHARSET_MBMAXLEN
                    // NAME_CHAR_LEN = 3, SYSTEM_CHARSET_MBMAXLEN = 64
                    int maxDbNameLength = 3 * (1 << 6);
                    dbNameLength = dbNameLength < maxDbNameLength ? dbNameLength : maxDbNameLength;
                    updateDbNames[i] = readFixLengthStringDefaultCharset(payload, dbNameLength);
                    readStatusVarLength.addAndGet(dbNameLength);
                }
                queryStatus.setUpdateDbNames(updateDbNames);
                break;
            }
            case q_microseconds:
                queryStatus.setMicroseconds(payload.readUnsignedMediumLE());
                readStatusVarLength.addAndGet(3);
                break;
            case q_explicit_defaults_for_timestamp:
                payload.readBoolean();
                readStatusVarLength.addAndGet(1);
                break;
            case q_ddl_logged_with_xid: 
                queryStatus.setDdlXid(payload.readLongLE());
                readStatusVarLength.addAndGet(8);
                break;
            case q_default_collation_for_utf8mb4:
                payload.readUnsignedShortLE(); 
                readStatusVarLength.addAndGet(2);
                break;
            case q_sql_require_primary_key:
                payload.readBoolean();
                readStatusVarLength.addAndGet(1);
                break;
            case q_commit_ts:
            case q_commit_ts2:
            default:
                throw new IllegalStateException(String.format("can't match QueryStatusCode when parse QueryLogEvent.QueryStatus, code is %d", code.getCode()));
        }
    }

    public class QueryStatus {
        // -1 is init
        private long flags2 = -1;

        private long sqlMode = -1;

        private String catalog;

        private int autoIncrementIncrement = -1;
        private int autoIncrementOffset = -1;

        private int clientCharset = -1;
        private int clientCollation = -1;
        private int serverCollation = -1;

        private String timeZone;

        private int lcTime = -1;

        private int charsetDatabase = -1;

        private long tableMapForUpdate = -1;

        private long masterDataWritten = -1;

        private String user;

        private String host;

        private long microseconds = -1;

        private String[] updateDbNames;
        
        private long  ddlXid  = -1;
        

        public long getFlags2() {
            return flags2;
        }

        public void setFlags2(long flags2) {
            this.flags2 = flags2;
        }

        public long getSqlMode() {
            return sqlMode;
        }

        public void setSqlMode(long sqlMode) {
            this.sqlMode = sqlMode;
        }

        public String getCatalog() {
            return catalog;
        }

        public void setCatalog(String catalog) {
            this.catalog = catalog;
        }

        public int getAutoIncrementIncrement() {
            return autoIncrementIncrement;
        }

        public void setAutoIncrementIncrement(int autoIncrementIncrement) {
            this.autoIncrementIncrement = autoIncrementIncrement;
        }

        public int getAutoIncrementOffset() {
            return autoIncrementOffset;
        }

        public void setAutoIncrementOffset(int autoIncrementOffset) {
            this.autoIncrementOffset = autoIncrementOffset;
        }

        public int getClientCharset() {
            return clientCharset;
        }

        public void setClientCharset(int clientCharset) {
            this.clientCharset = clientCharset;
        }

        public int getClientCollation() {
            return clientCollation;
        }

        public void setClientCollation(int clientCollation) {
            this.clientCollation = clientCollation;
        }

        public int getServerCollation() {
            return serverCollation;
        }

        public void setServerCollation(int serverCollation) {
            this.serverCollation = serverCollation;
        }

        public String getTimeZone() {
            return timeZone;
        }

        public void setTimeZone(String timeZone) {
            this.timeZone = timeZone;
        }

        public int getLcTime() {
            return lcTime;
        }

        public void setLcTime(int lcTime) {
            this.lcTime = lcTime;
        }

        public int getCharsetDatabase() {
            return charsetDatabase;
        }

        public void setCharsetDatabase(int charsetDatabase) {
            this.charsetDatabase = charsetDatabase;
        }

        public long getTableMapForUpdate() {
            return tableMapForUpdate;
        }

        public void setTableMapForUpdate(long tableMapForUpdate) {
            this.tableMapForUpdate = tableMapForUpdate;
        }

        public long getMasterDataWritten() {
            return masterDataWritten;
        }

        public void setMasterDataWritten(long masterDataWritten) {
            this.masterDataWritten = masterDataWritten;
        }

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public long getMicroseconds() {
            return microseconds;
        }

        public void setMicroseconds(long microseconds) {
            this.microseconds = microseconds;
        }

        public String[] getUpdateDbNames() {
            return updateDbNames;
        }

        public void setUpdateDbNames(String[] updateDbNames) {
            this.updateDbNames = updateDbNames;
        }

        public long getDdlXid() {
            return ddlXid;
        }

        public void setDdlXid(long ddlXid) {
            this.ddlXid = ddlXid;
        }
        
        }
        
        

    enum QueryStatusCode {
        q_flags2_code(0),
        q_sql_mode_code(1),
        q_catalog(2),
        q_auto_increment(3),
        q_charset_code(4),
        q_time_zone_code(5),
        q_catalog_nz_code(6),
        q_lc_time_names_code(7),
        q_charset_database_code(8),
        q_table_map_for_update_code(9),
        q_master_data_written_code(10),
        q_invoker(11),
        q_update_db_names(12),
        q_microseconds(13),
        q_commit_ts(14),
        q_commit_ts2(15),
        q_explicit_defaults_for_timestamp(16),
        q_ddl_logged_with_xid(17),
        q_default_collation_for_utf8mb4(18),
        q_sql_require_primary_key(19);
        
        static QueryStatusCode getQueryStatusCode(final int code) {
            switch (code) {
                case 0:
                    return q_flags2_code;
                case 1:
                    return q_sql_mode_code;
                case 2:
                    return q_catalog;
                case 3:
                    return q_auto_increment;
                case 4:
                    return q_charset_code;
                case 5:
                    return q_time_zone_code;
                case 6:
                    return q_catalog_nz_code;
                case 7:
                    return q_lc_time_names_code;
                case 8:
                    return q_charset_database_code;
                case 9:
                    return q_table_map_for_update_code;
                case 10:
                    return q_master_data_written_code;
                case 11:
                    return q_invoker;
                case 12:
                    return q_update_db_names;
                case 13:
                    return q_microseconds;
                case 14:
                    return q_commit_ts;
                case 15:
                    return q_commit_ts2;
                case 16:
                    return q_explicit_defaults_for_timestamp;
                case 17:
                    return q_ddl_logged_with_xid;
                case 18:
                    return q_default_collation_for_utf8mb4;
                case 19:
                    return q_sql_require_primary_key;
                default:
                    throw new IllegalStateException(String.format("can't match QueryStatusCode when parse QueryLogEvent.QueryStatusCode, code is %d", code));
            }
        }

        QueryStatusCode(int code) {
            this.code = code;
        }

        private int code;

        public int getCode() {
            return code;
        }
    }


    public long getSlaveProxyId() {
        return slaveProxyId;
    }

    public long getExecuteTime() {
        return executeTime;
    }

    public int getSchemaLength() {
        return schemaLength;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public int getStatusVarLength() {
        return statusVarLength;
    }

    public QueryStatus getQueryStatus() {
        return queryStatus;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getQuery() {
        return query;
    }

    public Long getChecksum() {
        return checksum;
    }
}