package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.constant.MysqlFieldType;
import com.ctrip.framework.drc.core.driver.binlog.header.LogEventHeader;
import com.ctrip.framework.drc.core.driver.util.ByteHelper;
import com.ctrip.framework.drc.core.driver.util.CharsetConversion;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.commons.lang3.StringUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventHeaderLength.eventHeaderLengthVersionGt1;
import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.drc_table_map_log_event;
import static com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent.long64Max;
import static com.ctrip.framework.drc.core.driver.config.GlobalConfig.LOG_EVENT_IGNORABLE_F;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Created by @author zhuYongMing on 2019/9/11.
 * see https://dev.mysql.com/doc/internals/en/table-map-event.html
 */
public class TableMapLogEvent extends AbstractLogEvent implements LogEventMerger {

    private long tableId;

    private int flags;

    private String schemaName;

    private String tableName;

    private long columnsCount;

    private List<Column> columns;

    private Long checksum;

    private List<List<String>> identifiers;

    private List<ByteBuf> mergedByteBufs;

    public TableMapLogEvent() {}

    public TableMapLogEvent(final long serverId, final long currentEventStartPosition,
                            final long tableId, final String schemaName, final String tableName,
                            final List<Column> columns, final List<List<String>> identifiers) throws IOException {
        // valid params
        this(serverId, currentEventStartPosition, tableId, schemaName, tableName, columns, identifiers, drc_table_map_log_event, LOG_EVENT_IGNORABLE_F);
    }

    // for test
    public TableMapLogEvent(final long serverId, final long currentEventStartPosition,
                            final long tableId, final String schemaName, final String tableName,
                            final List<Column> columns, final List<List<String>> identifiers,
                            final LogEventType logEventType, final int flags) throws IOException {
        // valid params
        this.tableId = tableId;
        this.flags = this.flags | flags;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columnsCount = columns.size();
        this.columns = columns;
        this.identifiers = identifiers;
        this.checksum = 0L;
        final byte[] payloadBytes = payloadToBytes(logEventType);
        final int payloadLength = payloadBytes.length;

        // set logEventHeader
        int eventSize = eventHeaderLengthVersionGt1 + payloadLength;
        setLogEventHeader(
                new LogEventHeader(
                        logEventType.getType(), serverId, eventSize,
                        currentEventStartPosition + eventSize, this.flags
                )
        );

        // set payload
        final ByteBuf payloadByteBuf = PooledByteBufAllocator.DEFAULT.directBuffer(payloadLength);
        payloadByteBuf.writeBytes(payloadBytes);
        payloadByteBuf.skipBytes(payloadLength);
        setPayloadBuf(payloadByteBuf);
    }

    @Override
    public TableMapLogEvent read(ByteBuf byteBuf) {
        final LogEvent logEvent = super.read(byteBuf);
        if (null == logEvent) {
            return null;
        }

        final ByteBuf payloadBuf = getPayloadBuf();
        // table-map event post-header length is 8 instead of 6, so tableId length is 6 bytes
        final long low4 = payloadBuf.readUnsignedIntLE();
        final long high2 = payloadBuf.readUnsignedShortLE();
        this.tableId = low4 | (high2 << 32); // 6bytes
        this.flags = payloadBuf.readUnsignedShortLE(); // 2bytes

        this.schemaName = readVariableLengthStringDefaultCharset(payloadBuf); // string.var length bytes
        payloadBuf.skipBytes(1); //termination null

        this.tableName = readVariableLengthStringDefaultCharset(payloadBuf); // string.var length bytes
        payloadBuf.skipBytes(1); //termination null

        if (!decodeColumn()) {
            payloadBuf.skipBytes(payloadBuf.readableBytes());
            return this;
        }

        this.columnsCount = readLengthEncodeInt(payloadBuf);
        this.columns = new ArrayList<>((int) columnsCount);
        // type
        IntStream.range(0, (int) columnsCount).forEach(i -> {
            final Column column = new Column();
            column.setType(payloadBuf.readUnsignedByte());
            columns.add(column);
        });

        if (hasRemaining(payloadBuf)) {
            // replicator total Length
            readLengthEncodeInt(payloadBuf);
            // replicator
            IntStream.range(0, (int) columnsCount).forEach(i -> {
                final Column column = columns.get(i);
                column.setMeta(readFieldMeta(payloadBuf, column.getType()));
            });

            // nullable
            final BitSet nullBitMap = readBitSet(payloadBuf, columnsCount);
            IntStream.range(0, (int) columnsCount).forEach(i -> {
                final Column column = columns.get(i);
                column.setNullable(nullBitMap.get(i));
            });

            // name, charset, collation, unsigned, binary is added by DRC
            if (isDrcTableMapLogEvent()) {
                // name
                IntStream.range(0, (int) columnsCount).forEach(i -> {
                    final Column column = columns.get(i);
                    column.setName(readVariableLengthStringDefaultCharset(payloadBuf));
                });

                // charset nullable
                final BitSet charsetNullBitMap = readBitSet(payloadBuf, columnsCount);
                // charset
                IntStream.range(0, (int) columnsCount).forEach(i -> {
                    final Column column = columns.get(i);
                    if (charsetNullBitMap.get(i)) {
                        column.setCharset(null);
                    } else {
                        column.setCharset(readVariableLengthStringDefaultCharset(payloadBuf));
                    }
                });

                // collation nullable
                final BitSet collationNullBitMap = readBitSet(payloadBuf, columnsCount);
                // collation
                IntStream.range(0, (int) columnsCount).forEach(i -> {
                    final Column column = columns.get(i);
                    if (collationNullBitMap.get(i)) {
                        column.setCollation(null);
                    } else {
                        column.setCollation(readVariableLengthStringDefaultCharset(payloadBuf));
                    }
                });

                // unsigned
                final BitSet unsignedBitMap = readBitSet(payloadBuf, columnsCount);
                IntStream.range(0, (int) columnsCount).forEach(i -> {
                    final Column column = columns.get(i);
                    column.setUnsigned(unsignedBitMap.get(i));
                });

                // binary
                final BitSet binaryBitMap = readBitSet(payloadBuf, columnsCount);
                IntStream.range(0, (int) columnsCount).forEach(i -> {
                    final Column column = columns.get(i);
                    column.setBinary(binaryBitMap.get(i));
                });

                // pk
                final BitSet pkBitMap = readBitSet(payloadBuf, columnsCount);
                IntStream.range(0, (int) columnsCount).forEach(i -> {
                    final Column column = columns.get(i);
                    column.setPk(pkBitMap.get(i));
                });

                // uk
                final BitSet ukBitMap = readBitSet(payloadBuf, columnsCount);
                IntStream.range(0, (int) columnsCount).forEach(i -> {
                    final Column column = columns.get(i);
                    column.setUk(ukBitMap.get(i));
                });

                // onUpdate
                final BitSet onUpdateBitMap = readBitSet(payloadBuf, columnsCount);
                IntStream.range(0, (int) columnsCount).forEach(i -> {
                    final Column column = columns.get(i);
                    column.setOnUpdate(onUpdateBitMap.get(i));
                });

                // columnDefault nullable
                final BitSet columnDefaultNullBitMap = readBitSet(payloadBuf, columnsCount);
                // columnDefault
                IntStream.range(0, (int) columnsCount).forEach(i -> {
                    final Column column = columns.get(i);
                    if (columnDefaultNullBitMap.get(i)) {
                        column.setColumnDefault(null);
                    } else {
                        column.setColumnDefault(readVariableLengthStringDefaultCharset(payloadBuf));
                    }
                });

                if (hasRemaining(payloadBuf)) {
                    identifiers = Lists.newArrayList();
                    long indexSize = readLengthEncodeInt(payloadBuf);

                    if (indexSize > 0) {
                        for (int i = 0; i < indexSize; ++i) {
                            List<String> indexNames = Lists.newArrayList();
                            long indexNamesSize = readLengthEncodeInt(payloadBuf);
                            for (int j = 0; j < indexNamesSize; ++j) {
                                String indexName = readVariableLengthStringDefaultCharset(payloadBuf);
                                indexNames.add(indexName);
                            }
                            identifiers.add(indexNames);
                        }
                    }
                }
            }
            // extra data, include charset... mysql version 8.0.1+
        }

        this.checksum = readChecksumIfPossible(payloadBuf); // 4bytes
        return this;
    }

    protected boolean decodeColumn() {
        return true;
    }

    @Override
    public void write(IoCache ioCache) {
        super.write(ioCache);
    }

    @Override
    protected List<ByteBuf> getEventByteBuf(ByteBuf headByteBuf, ByteBuf payloadBuf) {
        mergedByteBufs = mergeByteBuf(headByteBuf, payloadBuf);
        return mergedByteBufs;
    }

    public void releaseMergedByteBufs() {
        if (mergedByteBufs == null) {
            return;
        }
        for (ByteBuf byteBuf : mergedByteBufs) {
            if (byteBuf != null && byteBuf.refCnt() > 0) {
                byteBuf.release(byteBuf.refCnt());
            }
        }
    }

    private byte[] payloadToBytes(final LogEventType logEventType) throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteHelper.writeUnsignedInt48LittleEndian(getTableId(), out);
        ByteHelper.writeUnsignedShortLittleEndian(getFlags(), out);
        ByteHelper.writeVariablesLengthStringDefaultCharset(getSchemaName(), out);
        ByteHelper.writeNullTerminated(out);
        ByteHelper.writeVariablesLengthStringDefaultCharset(getTableName(), out);
        ByteHelper.writeNullTerminated(out);
        ByteHelper.writeLengthEncodeInt(getColumnsCount(), out);
        final List<Column> columns = getColumns();

        // columns type
        columns.forEach(column -> ByteHelper.writeUnsignedByte(column.getType(), out));
        // columns replicator total length
        final int length = columns.stream().map(
                column -> getFiledMetaLength(column.getType())
        ).mapToInt(integer -> integer).sum();
        ByteHelper.writeLengthEncodeInt(length, out);

        // columns replicator
        columns.forEach(
                column -> writeFieldMeta(
                        column.getMeta(), column.getType(),
                        out, logEventType
                )
        );
        // columns nullable
        writeBoolean(
                columns.stream().map(Column::isNullable).collect(Collectors.toList()), out
        );

        // name, charset, collation, unsigned, binary is added by DRC
        if (drc_table_map_log_event == logEventType) {
            // columns field name
            for (Column column : columns) {
                ByteHelper.writeVariablesLengthStringDefaultCharset(column.getName(), out);
            }

            // columns charset nullable
            final List<Boolean> charsetsNull = columns.stream().map(column -> Objects.isNull(column.getCharset())).collect(Collectors.toList());
            writeBoolean(
                    charsetsNull, out
            );

            // columns charset
            for (int i = 0; i < columns.size(); i++) {
                if (!charsetsNull.get(i)) {
                    final Column column = columns.get(i);
                    ByteHelper.writeVariablesLengthStringDefaultCharset(column.getCharset(), out);
                }
            }

            // columns collation nullable
            final List<Boolean> collationNull = columns.stream().map(column -> Objects.isNull(column.getCollation())).collect(Collectors.toList());
            writeBoolean(collationNull, out);

            // columns collation
            for (int i = 0; i < columns.size(); i++) {
                if (!collationNull.get(i)) {
                    final Column column = columns.get(i);
                    ByteHelper.writeVariablesLengthStringDefaultCharset(column.getCollation(), out);
                }
            }

            // columns unsigned
            writeBoolean(columns.stream().map(Column::isUnsigned).collect(Collectors.toList()), out);

            // columns binary
            writeBoolean(columns.stream().map(Column::isBinary).collect(Collectors.toList()), out);

            // columns pk
            writeBoolean(columns.stream().map(Column::isPk).collect(Collectors.toList()), out);

            // columns uk
            writeBoolean(columns.stream().map(Column::isUk).collect(Collectors.toList()), out);

            // columns onUpdate
            writeBoolean(columns.stream().map(Column::isOnUpdate).collect(Collectors.toList()), out);

            // columns default value nullable
            final List<Boolean> columnDefaultNull = columns.stream().map(column -> Objects.isNull(column.getColumnDefault())).collect(Collectors.toList());
            writeBoolean(columnDefaultNull, out);

            // columns default value
            for (int i = 0; i < columns.size(); i++) {
                if (!columnDefaultNull.get(i)) {
                    final Column column = columns.get(i);
                    ByteHelper.writeVariablesLengthStringDefaultCharset(column.getColumnDefault(), out);
                }
            }

            List<List<String>> indexes = getIdentifiers();
            if (indexes != null && !indexes.isEmpty()) {
                int indexSize = indexes.size();
                ByteHelper.writeLengthEncodeInt(indexSize, out);
                for (int i = 0; i < indexSize; ++i) {
                    List<String> indexNames = indexes.get(i);
                    int indexNamesSize = indexNames.size();
                    ByteHelper.writeLengthEncodeInt(indexNamesSize, out);
                    for (int j = 0; j < indexNamesSize; ++j) {
                        ByteHelper.writeVariablesLengthStringDefaultCharset(indexNames.get(j), out);
                    }
                }
            } else {
                ByteHelper.writeLengthEncodeInt(0, out);
            }
        }

        // checksum
        ByteHelper.writeUnsignedIntLittleEndian(getChecksum(), out);

        return out.toByteArray();
    }

    private void writeBoolean(final List<Boolean> nullable, final ByteArrayOutputStream out) {
        for (int bit = 0; bit < nullable.size(); bit += 8) {
            byte nullableByte = 0x00;

            if (bit < nullable.size() && nullable.get(bit)) nullableByte |= 0x01;
            if (bit + 1 < nullable.size() && nullable.get(bit + 1)) nullableByte |= 0x02;
            if (bit + 2 < nullable.size() && nullable.get(bit + 2)) nullableByte |= 0x04;
            if (bit + 3 < nullable.size() && nullable.get(bit + 3)) nullableByte |= 0x08;
            if (bit + 4 < nullable.size() && nullable.get(bit + 4)) nullableByte |= 0x10;
            if (bit + 5 < nullable.size() && nullable.get(bit + 5)) nullableByte |= 0x20;
            if (bit + 6 < nullable.size() && nullable.get(bit + 6)) nullableByte |= 0x40;
            if (bit + 7 < nullable.size() && nullable.get(bit + 7)) nullableByte |= 0x80;

            out.write(nullableByte);
        }
    }

    private boolean isDrcTableMapLogEvent() {
        return drc_table_map_log_event.equals(getLogEventType());
    }

    private int readFieldMeta(final ByteBuf byteBuf, final int type) {
        switch (getFiledMetaLength(type)) {
            case 1:
                return byteBuf.readUnsignedByte();
            case 2:
                if (isDrcTableMapLogEvent()) {
                    return byteBuf.readUnsignedShortLE();
                } else {
                    final MysqlFieldType mysqlFieldType = MysqlFieldType.getMysqlFieldType(type);
                    if (MysqlFieldType.mysql_type_string.equals(mysqlFieldType) || MysqlFieldType.mysql_type_newdecimal.equals(mysqlFieldType)) {
                        // read big endian
                        return byteBuf.readUnsignedShort();
                    }
                    return byteBuf.readUnsignedShortLE();
                }
            default:
                return 0;
        }
    }

    private void writeFieldMeta(final int meta, final int type, final ByteArrayOutputStream out,
                                final LogEventType logEventType) {
        switch (getFiledMetaLength(type)) {
            case 1:
                ByteHelper.writeUnsignedByte(meta, out);
                break;
            case 2: {
                if (LogEventType.drc_table_map_log_event.equals(logEventType)) {
                    ByteHelper.writeUnsignedShortLittleEndian(meta, out);
                } else {
                    final MysqlFieldType mysqlFieldType = MysqlFieldType.getMysqlFieldType(type);
                    if (MysqlFieldType.mysql_type_string.equals(mysqlFieldType) || MysqlFieldType.mysql_type_newdecimal.equals(mysqlFieldType)) {
                        // write big endian
                        ByteHelper.writeUnsignedShortBigEndian(meta, out);
                    } else {
                        ByteHelper.writeUnsignedShortLittleEndian(meta, out);
                    }
                }
                break;
            }
        }
    }

    /**
     * see https://dev.mysql.com/doc/internals/en/table-map-event.html
     * all type are finished
     */
    private int getFiledMetaLength(final int type) {
        switch (MysqlFieldType.getMysqlFieldType(type)) {
            // length is 1
            case mysql_type_tiny_blob:
            case mysql_type_medium_blob:
            case mysql_type_long_blob:
            case mysql_type_blob:
            case mysql_type_double:
            case mysql_type_float:
            case mysql_type_geometry:
            case mysql_type_json:
            case mysql_type_time2:
            case mysql_type_datetime2:
            case mysql_type_timestamp2:
                return 1;

            // length is 2
            // document show bit replicator length = 0, canal show length is 2?
            case mysql_type_bit:
            case mysql_type_string:
            case mysql_type_var_string:
            case mysql_type_varchar:
            case mysql_type_decimal:
            case mysql_type_newdecimal:
                /*
                 * log_event.h : MYSQL_TYPE_SET & MYSQL_TYPE_ENUM : This
                 * enumeration value is only used internally and cannot
                 * exist in a binlog.
                 */
            case mysql_type_set:
            case mysql_type_enum:
                return 2;

            default:
                // other field type replicator length is 0
                return 0;
        }
    }

    public static final class Column {

        private int type;

        private int meta;

        private boolean nullable; // Can it be Null

        // name, charset collation added by DRC
        private String name;

        private String charset;

        private String collation;

        private String columnDefault;

        private boolean unsigned;

        private boolean binary;

        private boolean pk;

        private boolean uk;

        private boolean onUpdate;

        public Column() {

        }

        /**
         * @param columnName           字段名
         * @param nullable             是否可以为null
         * @param dataTypeLiteral      数据类型字面值
         * @param characterOctetLength 最大占用字节数
         * @param numberPrecision      数字类型精度M, e.g:bit[M]
         * @param datetimePrecision    时间类型精度M, e.g:timestamp[M]
         * @param charset              字符集
         * @param collation            字符序
         * @param columnType           数据类型字面值，包括精度unsigned 和申明的精度,e.g: COLUMN_TYPE: int(20) unsigned
         * @param columnKey            索引, e.g:PRI(主键)
         * @param extra                额外信息, e.g:on update CURRENT_TIMESTAMP
         * @param columnDefault        default value
         */
        public Column(final String columnName, final boolean nullable,
                      final String dataTypeLiteral, final String characterOctetLength,
                      final String numberPrecision, final String numberScale, final String datetimePrecision,
                      final String charset, final String collation, final String columnType,
                      final String columnKey, final String extra, final String columnDefault) {

            this.type = MysqlFieldType.getMysqlFieldTypeByLiteral(dataTypeLiteral).getType();
            this.nullable = nullable;
            this.name = columnName;
            this.columnDefault = columnDefault;

            if (StringUtils.isNotBlank(columnKey) && columnKey.contains("PRI")) {
                this.pk = true;
            }

            if (StringUtils.isNotBlank(columnKey) && columnKey.contains("UNI")) {
                this.uk = true;
            }

            if (StringUtils.isNotBlank(extra) && extra.contains("on update")) {
                this.onUpdate = true;
            }

            if (MysqlFieldType.isTextType(dataTypeLiteral) || MysqlFieldType.isCharsetType(dataTypeLiteral)) {
                this.charset = charset;
                this.collation = collation;
            } else if (MysqlFieldType.isBinaryType(dataTypeLiteral)) {
                this.charset = "binary";
                this.collation = "binary";
            }

            if (MysqlFieldType.isNumberType(dataTypeLiteral)) {
                this.unsigned = columnType.contains("unsigned");

            } else if (MysqlFieldType.isCharsetType(dataTypeLiteral)) {
                this.meta = Integer.parseInt(characterOctetLength);
            } else if (MysqlFieldType.isDatetimePrecisionType(dataTypeLiteral)) {
                // date,year replicator is null
                this.meta = Integer.parseInt(datetimePrecision);

            } else if (MysqlFieldType.isDecimalType(dataTypeLiteral)) {
                // high8 precision, low8 number scale
                this.meta = Integer.parseInt(numberPrecision) << 8 | Integer.parseInt(numberScale);

            } else if (MysqlFieldType.isBinaryType(dataTypeLiteral)) {
                this.meta = Integer.parseInt(characterOctetLength);
                this.binary = true;

            } else if (MysqlFieldType.is1ByteLengthBlobOrTextType(dataTypeLiteral)) {
                this.meta = 1;
                this.binary = true;
            } else if (MysqlFieldType.is2ByteLengthBlobOrTextType(dataTypeLiteral)) {
                this.meta = 2;
                this.binary = true;
            } else if (MysqlFieldType.is3ByteLengthBlobOrTextType(dataTypeLiteral)) {
                this.meta = 3;
                this.binary = true;
            } else if (MysqlFieldType.is4ByteLengthBlobOrTextType(dataTypeLiteral)) {
                this.meta = 4;
                this.binary = true;

            } else if (MysqlFieldType.isBitType(dataTypeLiteral)) {
                this.meta = Integer.parseInt(numberPrecision);

            } else if (MysqlFieldType.isEnumType(dataTypeLiteral)) {
                final String[] enums = columnType
                        .replace("enum('", "").replace("')", "")
                        .split("','");
                this.meta = enums.length < 256 ? 1 : 2;
            }
        }

        public int getType() {
            return type;
        }

        public void setType(int type) {
            this.type = type;
        }

        public int getMeta() {
            return meta;
        }

        public void setMeta(int meta) {
            this.meta = meta;
        }

        public boolean isNullable() {
            return nullable;
        }

        public void setNullable(boolean nullable) {
            this.nullable = nullable;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getCharset() {
            return charset;
        }

        public void setCharset(String charset) {
            this.charset = charset;
        }

        public String getCollation() {
            return collation;
        }

        public void setCollation(String collation) {
            this.collation = collation;
        }

        public boolean isUnsigned() {
            return unsigned;
        }

        public void setUnsigned(boolean unsigned) {
            this.unsigned = unsigned;
        }

        public boolean isBinary() {
            return binary;
        }

        public void setBinary(boolean binary) {
            this.binary = binary;
        }

        public boolean isPk() {
            return pk;
        }

        public void setPk(boolean pk) {
            this.pk = pk;
        }

        public boolean isUk() {
            return uk;
        }

        public void setUk(boolean uk) {
            this.uk = uk;
        }

        public boolean isOnUpdate() {
            return onUpdate;
        }

        public void setOnUpdate(boolean onUpdate) {
            this.onUpdate = onUpdate;
        }

        public String getColumnDefault() {
            return columnDefault;
        }

        @JsonIgnore
        public Object getColumnDefaultObject() {
            if (columnDefault == null) {
                return null;
            }
            MysqlFieldType mysqlFieldType = MysqlFieldType.getMysqlFieldType(this.type);
            switch (mysqlFieldType) {
                case mysql_type_tiny: {
                    // 1byte
                    // [-2^7，2^7 -1], [-128, 127]
                    // unsigned [0, 2^8 - 1], [0, 255]
                    // mysql literal is tinyint
                    if (this.unsigned) {
                        return Short.parseShort(columnDefault);
                    } else {
                        return Byte.parseByte(columnDefault);
                    }
                }
                case mysql_type_short: {
                    // 4bytes
                    // [-2^31, 2^31 - 1], [-2,147,483,648, 2,147,483,647]
                    // unsigned [0, 2^32 - 1], [0, 4294967295]
                    // mysql literal is int
                    if (this.unsigned) {
                        return Integer.parseInt(columnDefault);
                    } else {
                        return Short.parseShort(columnDefault);
                    }
                }
                case mysql_type_int24: {
                    // 3bytes
                    // [-2^23, 2^23 - 1], [-8388608, 8388607]
                    // unsigned [0, 2^24 - 1], [0, 16777215]
                    // mysql literal is int, integer
                    return Integer.parseInt(columnDefault);
                }
                case mysql_type_long: {
                    // 4bytes
                    // [-2^31, 2^31 - 1], [-2,147,483,648, 2,147,483,647]
                    // unsigned [0, 2^32 - 1], [0, 4294967295]
                    // mysql literal is int
                    if (this.unsigned) {
                        return Long.parseLong(columnDefault);
                    } else {
                        return Integer.parseInt(columnDefault);
                    }
                }
                case mysql_type_longlong: {
                    // 8bytes
                    // [-2^63, 2^63 - 1], [-9223372036854775808, 9223372036854775807],
                    // unsigned [0, 2^64 - 1], [0, 18446744073709551615]
                    // mysql literal is bigint
                    if (this.unsigned) {
                        long long64 = Long.parseLong(columnDefault);
                        return long64 >= 0 ? BigDecimal.valueOf(long64) : long64Max.add(BigDecimal.valueOf(1 + long64));
                    } else {
                        return Long.parseLong(columnDefault);
                    }
                }
                case mysql_type_newdecimal: {
                    return new BigDecimal(columnDefault);
                }
                case mysql_type_float: {
                    // 4bytes
                    return Float.parseFloat(columnDefault);
                }
                case mysql_type_double: {
                    // 8bytes
                    return Double.parseDouble(columnDefault);
                }
                case mysql_type_bit: {
                    int byteCount = (this.meta + 7) / 8;
                    String defaultValue = columnDefault.replaceAll("b", "").replaceAll("B", "").replaceAll("'", "");
                    switch (byteCount) {
                        case 1:
                            return Short.parseShort(defaultValue, 2);
                        case 2:
                        case 3:
                            return Integer.parseInt(defaultValue, 2);
                        case 4:
                        case 5:
                        case 6:
                        case 7:
                            return Long.parseLong(defaultValue, 2);
                        case 8:
                            final long long64 = Long.parseLong(defaultValue, 2);
                            return long64 >= 0 ? BigDecimal.valueOf(long64) : long64Max.add(BigDecimal.valueOf(1 + long64));
                        default:
                            // ignore, can't more than 8 bytes
                            throw new UnsupportedOperationException(String.format("parse mysql field type bit error, bit(%d)", this.meta));
                    }
                }
                case mysql_type_string:
                case mysql_type_varchar: {
                    if (this.binary) {
                        byte[] res = toBytes();
                        int length = -1;
                        for (int i = 0; i < res.length; ++i) {
                            if (res[i] == '\0') {
                                length = i;
                                break;
                            }
                        }
                        if (res.length == length || length < 0) { //varbinary length == -1
                            return res;
                        } else {
                            byte[] bytes = new byte[length];
                            for(int i = 0; i < length; i++){
                                bytes[i] = res[i];
                            }
                            return bytes;
                        }
                    }
                    return columnDefault;
                }
                case mysql_type_blob:  //no default
                case mysql_type_date:
                case mysql_type_timestamp2:
                case mysql_type_datetime2:
                case mysql_type_time2:
                case mysql_type_year:
                    return columnDefault;
                case mysql_type_enum: {
                    if (this.meta < 256) {
                        return Short.parseShort(columnDefault);
                    } else {
                        return Integer.parseInt(columnDefault);
                    }
                }

            }
            return columnDefault;
        }

        private byte[] toBytes() {
            byte[] res;
            try {
                Charset charSet = calCharset();
                res = columnDefault.getBytes(charSet);
            } catch (Throwable e) {
                res = columnDefault.getBytes();
            }

            return res;
        }

        private Charset calCharset() {
            Charset javaCharset = calJavaCharset();
            return Objects.requireNonNullElse(javaCharset, UTF_8);
        }

        public Charset calJavaCharset() {
            final String charset = this.charset;
            final String collation = this.collation;
            if (null != charset && !"".equals(charset) && null != collation && !"".equals(collation)) {
                String javaCharset = CharsetConversion.getJavaCharset(charset, collation);
                if (null != javaCharset && Charset.isSupported(javaCharset)) {
                    return Charset.forName(javaCharset);
                }
            }
            return null;
        }

        public void setColumnDefault(String columnDefault) {
            this.columnDefault = columnDefault;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Column column = (Column) o;
            return type == column.type &&
                    meta == column.meta &&
                    nullable == column.nullable &&
                    unsigned == column.unsigned &&
                    binary == column.binary &&
                    pk == column.pk &&
                    uk == column.uk &&
                    onUpdate == column.onUpdate &&
                    Objects.equals(name, column.name) &&
                    Objects.equals(charset, column.charset) &&
                    Objects.equals(collation, column.collation) &&
                    Objects.equals(columnDefault, column.columnDefault);
        }

        @Override
        public int hashCode() {

            return Objects.hash(type, meta, nullable, name, charset, collation, columnDefault, unsigned, binary, pk, uk, onUpdate);
        }

        @Override
        public String toString() {
            return "Column{" +
                    "type=" + type +
                    ", meta=" + meta +
                    ", nullable=" + nullable +
                    ", name='" + name + '\'' +
                    ", charset='" + charset + '\'' +
                    ", collation='" + collation + '\'' +
                    ", columnDefault='" + columnDefault + '\'' +
                    ", unsigned=" + unsigned +
                    ", binary=" + binary +
                    ", pk=" + pk +
                    ", uk=" + uk +
                    ", onUpdate=" + onUpdate +
                    '}';
        }
    }

    public long getTableId() {
        return tableId;
    }

    public int getFlags() {
        return flags;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getSchemaNameDotTableName() {
        return schemaName + "." + tableName;
    }

    public long getColumnsCount() {
        return columnsCount;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public List<String> getColumnsName() {
        return getColumns().stream().map(Column::getName)
                .collect(Collectors.toList());
    }

    public Long getChecksum() {
        return checksum;
    }

    public List<List<String>> getIdentifiers() {
        return identifiers;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableMapLogEvent that = (TableMapLogEvent) o;
        return tableId == that.tableId &&
                flags == that.flags &&
                columnsCount == that.columnsCount &&
                Objects.equals(schemaName, that.schemaName) &&
                Objects.equals(tableName, that.tableName) &&
                Objects.equals(columns, that.columns) &&
                Objects.equals(checksum, that.checksum);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, flags, schemaName, tableName, columnsCount, columns, checksum);
    }

}
