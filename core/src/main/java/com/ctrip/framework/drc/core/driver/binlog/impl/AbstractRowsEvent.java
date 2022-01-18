package com.ctrip.framework.drc.core.driver.binlog.impl;

import com.ctrip.framework.drc.core.driver.IoCache;
import com.ctrip.framework.drc.core.driver.binlog.LogEvent;
import com.ctrip.framework.drc.core.driver.binlog.RowsEvent;
import com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType;
import com.ctrip.framework.drc.core.driver.binlog.constant.MysqlFieldType;
import com.ctrip.framework.drc.core.driver.binlog.header.RowsEventPostHeader;
import com.ctrip.framework.drc.core.driver.util.CharsetConversion;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.core.driver.binlog.constant.LogEventType.update_rows_event_v2;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author wenchao.meng
 * <p>
 * Sep 01, 2019
 * see https://dev.mysql.com/doc/internals/en/rows-event.html
 */
public abstract class AbstractRowsEvent extends AbstractLogEvent implements RowsEvent {

    // time constant
    private static final char[] digits = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};
    private static final long datetime_int_ofs = 0x8000000000L;
    private static final long time_int_ofs = 0x800000L;
    private static final long time_ofs = 0x800000000000L;

    // decimal constant
    public static final BigDecimal long64Max = new BigDecimal("18446744073709551615");
    private static final int DIGITS_PER_4BYTES = 9;
    private static final BigDecimal POSITIVE_ONE = BigDecimal.ONE;
    private static final BigDecimal NEGATIVE_ONE = new BigDecimal("-1");
    private static final int DECIMAL_BINARY_SIZE[] = {0, 1, 1, 2, 2, 3, 3, 4, 4, 4};

    private RowsEventPostHeader rowsEventPostHeader;

    private long numberOfColumns;

    private BitSet beforePresentBitMap;

    private BitSet afterPresentBitMap;

    private List<Row> rows;

    private Long checksum;

    @Override
    public LogEvent read(ByteBuf byteBuf) {
        // this logEvent only include header or null right now
        final LogEvent logEvent = super.read(byteBuf);
        if (null == logEvent) {
            return null;
        }

        return this;
    }

    @Override
    public void write(IoCache ioCache) {
        super.write(ioCache);
    }

    public void loadPostHeader() {
        final ByteBuf payloadBuf = getPayloadBuf();
        // do read row event payload post-header
        rowsEventPostHeader = new RowsEventPostHeader().read(payloadBuf);
    }

    @Override
    public void load(final List<TableMapLogEvent.Column> columns) {
        if (checksum != null) {
            // already load
            return;
        }

        final ByteBuf payloadBuf = getPayloadBuf();
        // do read row event payload post-header
        rowsEventPostHeader = new RowsEventPostHeader().read(payloadBuf);

        // do read payload body
        this.numberOfColumns = readLengthEncodeInt(payloadBuf);
        this.beforePresentBitMap = readBitSet(payloadBuf, numberOfColumns);
        if (update_rows_event_v2 == LogEventType.getLogEventType(super.getLogEventHeader().getEventType())) {
            this.afterPresentBitMap = readBitSet(payloadBuf, numberOfColumns);
        }

        List<Row> rows = Lists.newArrayList();
        while (hasNextRow(payloadBuf)) {
            Row row = new Row();
            final BitSet beforeNullBitSet = readBitSet(payloadBuf, readNullBitSetLength(beforePresentBitMap, numberOfColumns));
            row.setBeforeNullBitMap(beforeNullBitSet);
            row.setBeforeValues(readValues(payloadBuf, beforePresentBitMap, beforeNullBitSet, numberOfColumns, columns));

            if (update_rows_event_v2 == LogEventType.getLogEventType(super.getLogEventHeader().getEventType())) {
                final BitSet afterNullBitSet = readBitSet(payloadBuf, readNullBitSetLength(afterPresentBitMap, numberOfColumns));
                row.setAfterNullBitMap(afterNullBitSet);
                row.setAfterValues(readValues(payloadBuf, afterPresentBitMap, afterNullBitSet, numberOfColumns, columns));
            }
            rows.add(row);
        }
        this.rows = rows;
        this.checksum = payloadBuf.readUnsignedIntLE(); // 4bytes
    }

    public List<List<Object>> getBeforePresentRowsValues() {
        return getRows().stream().map(Row::getBeforeValues)
                .collect(Collectors.toList());
    }

    public List<List<Object>> getAfterPresentRowsValues() {
        return getRows().stream().map(Row::getAfterValues)
                .collect(Collectors.toList());
    }

    public List<Boolean> getBeforeRowsKeysPresent() {
        final BitSet beforePresentBitMap = getBeforePresentBitMap();
        final long numberOfColumns = getNumberOfColumns();

        final List<Boolean> keysPresent = new ArrayList<>((int) numberOfColumns);
        for (int i = 0; i < numberOfColumns; i++) {
            if (beforePresentBitMap.get(i)) {
                keysPresent.add(true);
            } else {
                keysPresent.add(false);
            }
        }

        return keysPresent;
    }

    public List<Boolean> getAfterRowsKeysPresent() {
        final BitSet afterPresentBitMap = getAfterPresentBitMap();
        final long numberOfColumns = getNumberOfColumns();

        final List<Boolean> keysPresent = new ArrayList<>((int) numberOfColumns);
        for (int i = 0; i < numberOfColumns; i++) {
            if (afterPresentBitMap.get(i)) {
                keysPresent.add(true);
            } else {
                keysPresent.add(false);
            }
        }

        return keysPresent;
    }

    protected final class Row {

        private BitSet beforeNullBitMap; // is it null now

        private List<Object> beforeValues;

        private BitSet afterNullBitMap;

        private List<Object> afterValues;


        protected BitSet getBeforeNullBitMap() {
            return beforeNullBitMap;
        }

        private void setBeforeNullBitMap(BitSet beforeNullBitMap) {
            this.beforeNullBitMap = beforeNullBitMap;
        }

        protected List<Object> getBeforeValues() {
            return beforeValues;
        }

        private void setBeforeValues(List<Object> beforeValues) {
            this.beforeValues = beforeValues;
        }

        protected BitSet getAfterNullBitMap() {
            return afterNullBitMap;
        }

        private void setAfterNullBitMap(BitSet afterNullBitMap) {
            this.afterNullBitMap = afterNullBitMap;
        }

        protected List<Object> getAfterValues() {
            return afterValues;
        }

        private void setAfterValues(List<Object> afterValues) {
            this.afterValues = afterValues;
        }
    }

    private List<Object> readValues(final ByteBuf byteBuf, final BitSet presentBitMap, final BitSet nullBitMap,
                                    final long numberOfColumns, final List<TableMapLogEvent.Column> columns) {

        final List<Object> values = new ArrayList<>(presentBitMap.length());
        // canal use table-map event columns num, why?,
        // in the fact, if a field does not change before and after the transaction,
        // updateRowsEvent before data size always full
        int nullIndex = 0;
        for (int i = 0; i < numberOfColumns; i++) {
            final TableMapLogEvent.Column column = columns.get(i);
            if (presentBitMap.get(i)) {
                if (!nullBitMap.get(nullIndex)) {
                    values.add(readFieldValue(byteBuf, column));
                } else {
                    values.add(null);
                }
                nullIndex += 1;
            }
        }

        return values;
    }

    public Pair<Integer, Integer> getRealMetaAndType(final int meta) {
        int byte0 = meta >> 8;
        int byte1 = meta & 0xff;
        if ((byte0 & 0x30) != 0x30) { // 0x30 = 0011 0000
            return new Pair<>(
                    byte1 | (((byte0 & 0x30) ^ 0x30) << 4),
                    byte0 | 0x30
            );
        } else {
            return new Pair<>(byte1, byte0);
        }
    }

    // see log_event.cc#log_event_print_value
    private Object readFieldValue(final ByteBuf byteBuf, final TableMapLogEvent.Column column) {
        // mysql all field type, see https://dev.mysql.com/doc/refman/5.7/en/integer-types.html, see https://www.jianshu.com/p/b08f848793d4
        MysqlFieldType type = MysqlFieldType.getMysqlFieldType(column.getType());

        switch (type) {
            // default M = 11, [M] length see https://www.cnblogs.com/totian/p/7065123.html
            // long, tiny, short, int24, longlong, little-endian
            case mysql_type_tiny: {
                // 1byte
                // [-2^7，2^7 -1], [-128, 127]
                // unsigned [0, 2^8 - 1], [0, 255]
                // mysql literal is tinyint
                if (column.isUnsigned()) {
                    return byteBuf.readUnsignedByte();
                } else {
                    return byteBuf.readByte();
                }
            }

            case mysql_type_short: {
                // 2bytes
                // [-2^15, 2^15-1], [-32,768, 32,767]
                // unsigned [0, 2^16 - 1], [0, 65535]
                // mysql literal is smallint
                if (column.isUnsigned()) {
                    return byteBuf.readUnsignedShortLE();
                } else {
                    return byteBuf.readShortLE();
                }
            }

            case mysql_type_int24: {
                // 3bytes
                // [-2^23, 2^23 - 1], [-8388608, 8388607]
                // unsigned [0, 2^24 - 1], [0, 16777215]
                // mysql literal is int, integer
                if (column.isUnsigned()) {
                    return byteBuf.readUnsignedMediumLE();
                } else {
                    return byteBuf.readMediumLE();
                }
            }

            case mysql_type_long: {
                // 4bytes
                // [-2^31, 2^31 - 1], [-2,147,483,648, 2,147,483,647]
                // unsigned [0, 2^32 - 1], [0, 4294967295]
                // mysql literal is int
                if (column.isUnsigned()) {
                    return byteBuf.readUnsignedIntLE();
                } else {
                    return byteBuf.readIntLE();
                }
            }

            case mysql_type_longlong: {
                // 8bytes
                // [-2^63, 2^63 - 1], [-9223372036854775808, 9223372036854775807],
                // unsigned [0, 2^64 - 1], [0, 18446744073709551615]
                // mysql literal is bigint
                if (column.isUnsigned()) {
                    final long long64 = byteBuf.readLongLE();
                    return long64 >= 0 ? BigDecimal.valueOf(long64) : long64Max.add(BigDecimal.valueOf(1 + long64));
                } else {
                    return byteBuf.readLongLE();
                }
            }

            /*
             * Decimal representation in binlog seems to be as follows:
             *
             * 1st bit - sign such that set == +, unset == -
             * every 4 bytes represent 9 digits in big-endian order, so that
             * if you print the values of these quads as big-endian integers one after
             * another, you get the whole number string representation in decimal. What
             * remains is to put a sign and a decimal dot.
             *
             * see mysql-server-5.7/strings/decimal.c - bin2decimal();
             * see mysql-server-5.7/strings/decimal.c - decimal2string();
             *
             * 8b 06 9f 6b
             * c8 0d 3e d7 8e 13 de 43  55 14 87 ce 1c 1a 8e a3
             * 63 21 1e cc ea 27 bc b2  11 means:
             *
             * 0x8b - positive
             * 8b ^ 0x80 = 0b = 11
             * 06 9f 6b c8 = 111111112
             * 0d 3e d7 8e = 222222222
             * 13 de 43 55 = 333333333
             * 14 87 ce 1c = 344444444
             * 1a 8e a3 63 = 445555555
             * 21 1e cc ea = 555666666
             * 27 bc b2 11 = 666677777
             * result = BigDecimal("11111111112222222222333333333344444444445555555555666666666677777");
             */
            case mysql_type_newdecimal: {
                // decimal[M, D] default M = 10, unpack float type, The number is stored as a string, one byte per number,
                // max M = 65, e.g: decimal[4, 2] = [-99.99, 99.99]
                // mysql literal is decimal, numeric
                final int meta = column.getMeta();
                final int precision = meta >> 8;
                final int scale = meta & 0xff;

                final int decimalLength = getDecimalBinarySize(precision, scale);
                final byte[] decimalBytes = new byte[decimalLength];
                byteBuf.readBytes(decimalBytes, 0, decimalLength);

                final boolean positive = (decimalBytes[0] & 0x80) == 0x80;
                decimalBytes[0] ^= 0x80;
                if (!positive) {
                    for (int i = 0; i < decimalBytes.length; i++) {
                        decimalBytes[i] ^= 0xFF;
                    }
                }

                //
                final int x = precision - scale;
                final int ipDigits = x / DIGITS_PER_4BYTES;
                final int ipDigitsX = x - ipDigits * DIGITS_PER_4BYTES;
                final int ipSize = (ipDigits << 2) + DECIMAL_BINARY_SIZE[ipDigitsX];
                int offset = DECIMAL_BINARY_SIZE[ipDigitsX];
                BigDecimal ip = offset > 0 ? BigDecimal.valueOf(toInt(decimalBytes, 0, offset)) : BigDecimal.ZERO;
                for(; offset < ipSize; offset += 4) {
                    final int i = toInt(decimalBytes, offset, 4);
                    ip = ip.movePointRight(DIGITS_PER_4BYTES).add(BigDecimal.valueOf(i));
                }

                int shift = 0;
                BigDecimal fp = BigDecimal.ZERO;
                for (; shift + DIGITS_PER_4BYTES <= scale; shift += DIGITS_PER_4BYTES, offset += 4) {
                    final int i = toInt(decimalBytes, offset, 4);
                    fp = fp.add(BigDecimal.valueOf(i).movePointLeft(shift + DIGITS_PER_4BYTES));
                }
                if(shift < scale) {
                    final int i = toInt(decimalBytes, offset, DECIMAL_BINARY_SIZE[scale - shift]);
                    fp = fp.add(BigDecimal.valueOf(i).movePointLeft(scale));
                }

                return positive ? POSITIVE_ONE.multiply(ip.add(fp)) : NEGATIVE_ONE.multiply(ip.add(fp));
            }

            case mysql_type_float: {
                // 4bytes
                return byteBuf.readFloatLE();
            }

            case mysql_type_double: {
                // 8bytes
                return byteBuf.readDoubleLE();
            }

            case mysql_type_bit: {
                // bit(M), M=[1-64], default M = 1
                // 1-8bytes
                // big-endian, unsigned
                int meta = column.getMeta();
                final int nbits = ((meta >> 8) * 8) + (meta & 0xff);
                int byteCount = (nbits + 7) / 8;
                if (nbits > 1) {
                    switch (byteCount) {
                        case 1:
                            return byteBuf.readUnsignedByte();
                        case 2:
                            return byteBuf.readUnsignedShort();
                        case 3:
                            return byteBuf.readUnsignedMedium();
                        case 4:
                            return byteBuf.readUnsignedInt();
                        case 5: {
                            final long high1 = byteBuf.readUnsignedByte();
                            final long low4 = byteBuf.readUnsignedInt();
                            return low4 | (high1 << 32);
                        }
                        case 6: {
                            final long high2 = byteBuf.readUnsignedShort();
                            final long low4 = byteBuf.readUnsignedInt();
                            return low4 | (high2 << 32);
                        }
                        case 7: {
                            final long high3 = byteBuf.readUnsignedMedium();
                            final long low4 = byteBuf.readUnsignedInt();
                            return low4 | (high3 << 32);
                        }
                        case 8:
                            final long long64 = byteBuf.readLong();
                            return long64 >= 0 ? BigDecimal.valueOf(long64) : long64Max.add(BigDecimal.valueOf(1 + long64));
                        default:
                            // ignore, can't more than 8 bytes
                            throw new UnsupportedOperationException(String.format("parse mysql field type bit error, bit(%d)", column.getMeta()));
                    }
                } else {
                    return byteBuf.readUnsignedByte();
                }
            }

            /*
             * MySQL一行数据不能超过65535byte:
             * 1、若一个表只有一个varchar类型，如定义为
             * create table t4(c varchar(N)) charset=gbk;
             * 则此处N的最大值为(65535-1-2)/2= 32766。
             * 减1的原因是实际行存储从第二个字节开始';
             * 减2的原因是varchar头部的2个字节表示长度;
             * 除2的原因是字符编码是gbk。
             *
             * 2、若一个表定义为
             * create table t4(c int, c2 char(30), c3 varchar(N)) charset=utf8;
             * 则此处N的最大值为 (65535-1-2-4-30*3)/3=21812
             * 减1和减2与上例相同;
             * 减4的原因是int类型的c占4个字节;
             * 减30*3的原因是char(30)占用90个字节，编码是utf8。
             * see https://www.jb51.net/article/31589.htm
             */
            case mysql_type_string:
                // include binary, char
                // char存储字符数[0-255], 无论何种字符集; binary没有字符集
            case mysql_type_varchar: {
                // include varbinary, varchar;
                // varchar[M], M=[0-65535], 存储字符数取值要看字符集
                int valueLength;
                if (column.getMeta() < 256) {
                    valueLength = byteBuf.readUnsignedByte();
                } else {
                    valueLength = byteBuf.readUnsignedShortLE();
                }

                if (column.isBinary()) {
                    // binary or varbinary
                    final byte[] values = new byte[valueLength];
                    byteBuf.readBytes(values, 0, valueLength);
                    return values;
                }
                return readFixLengthString(byteBuf, valueLength, getCharset(column));
            }

            case mysql_type_blob: {
                // include all blob and text
                switch (column.getMeta()) {
                    case 1: {
                        final short valueLength = byteBuf.readUnsignedByte();
                        final byte[] values = new byte[valueLength];
                        byteBuf.readBytes(values, 0, valueLength);
                        return values;
                    }

                    case 2: {
                        final int valueLength = byteBuf.readUnsignedShortLE();
                        final byte[] values = new byte[valueLength];
                        byteBuf.readBytes(values, 0, valueLength);
                        return values;
                    }

                    case 3: {
                        final int valueLength = byteBuf.readUnsignedMediumLE();
                        final byte[] values = new byte[valueLength];
                        byteBuf.readBytes(values, 0, valueLength);
                        return values;
                    }

                    case 4: {
                        // TODO: 2019/10/16 int problem
                        final int valueLength = (int) byteBuf.readUnsignedIntLE();
                        final byte[] values = new byte[valueLength];
                        byteBuf.readBytes(values, 0, valueLength);
                        return values;
                    }
                }
            }

            case mysql_type_date: {
                // document show range : '1000-01-01' to '9999-12-31'
                // real range : '0000-01-01' to '9999-12-31'
                final int date = byteBuf.readUnsignedMediumLE(); // unsigned litter-endian

                if (date == 0) {
                    return "1000-01-01";
                } else {
                    StringBuilder builder = new StringBuilder(12);
                    appendNumber4(builder, date / (16 * 32));
                    builder.append('-');
                    appendNumber2(builder, date / 32 % 16);
                    builder.append('-');
                    appendNumber2(builder, date % 32);
                    return builder.toString();
                }
            }

            case mysql_type_timestamp2: {
                // '1970-01-01 00:00:01.000000' to '2038-01-19 03:14:07.999999'
                final long second = byteBuf.readUnsignedInt();// unsigned big-endian

                int microsecond = 0;
                final int meta = column.getMeta();
                switch (meta) {
                    case 0:
                        break;
                    case 1:
                    case 2:
                        microsecond = byteBuf.readByte() * 10000; // signed big-endian
                        break;
                    case 3:
                    case 4:
                        microsecond = byteBuf.readShort() * 100; // signed big-endian
                        break;
                    case 5:
                    case 6:
                        microsecond = byteBuf.readMedium(); // signed big-endian
                        break;
                    default:
                        break;
                }

                String secondString;
                if (second == 0) {
                    // never
                    secondString = "1970-01-01 00:00:01";
                } else {
                    // 1.Java中，Timestamp带当下时区。
                    // 2.MySQL数据库time_zone默认是system，system_time_zone跟着机器走(CST中国标准时间)，所以MySQL默认带时区。
                    // 3.replicator对应source MySQL无论是否带时区，存入binlog的都是与时区无关的long值时间戳。
                    // 4.applier根据binlog存储的long值时间戳，利用Java Timestamp解析，然后insert到带时区的target MySQL，如果target MySQL不带时区就会有问题
                    // 5.解决方案：
                    //      a.查出target mysql时区，applier根据target MySQL时区，使用ZonedDateTime将long值转换成target MySQL时区insert(MySQL时区随时可变，无法实时)
                    //      b.控制target MySQL必须是当下时区，applier使用Timestamp就行。
                    Timestamp timestamp = new Timestamp(second * 1000);
                    secondString = timestamp.toString();
                    secondString = secondString.substring(0, secondString.length() - 2);// 去掉毫秒精度.0
                }

                if (meta >= 1) {
                    String microsecondString = microsecondsToString(microsecond, meta);
                    microsecondString = microsecondString.substring(0, meta);
                    return secondString + "." + microsecondString;
                } else {
                    return secondString;
                }
            }

            case mysql_type_datetime2: {
                // document show range : '1000-01-01 00:00:00.000000' to '9999-12-31 23:59:59.999999'
                // real range : '0000-01-01 00:00:00.000000' to '9999-12-31 23:59:59.999999'
                final long high1 = byteBuf.readUnsignedByte(); // unsigned big-endian
                final long low4 = byteBuf.readUnsignedInt(); // unsigned big-endian
                long intPart = low4 | (high1 << 32) - datetime_int_ofs;

                int fracture = 0;
                final int meta = column.getMeta();
                switch (meta) {
                    case 0:
                        break;
                    case 1:
                    case 2:
                        fracture = byteBuf.readByte() * 10000; // signed big-endian
                        break;
                    case 3:
                    case 4:
                        fracture = byteBuf.readShort() * 100; // signed big-endian
                        break;
                    case 5:
                    case 6:
                        fracture = byteBuf.readMedium(); // signed big-endian
                        break;
                    default:
                        break;
                }

                String secondString;
                if (intPart == 0) {
                    secondString = "1000-01-01 00:00:00";
                } else {
                    long ymd = intPart >> 17;
                    long ym = ymd >> 5;
                    long hms = intPart % (1 << 17);

                    StringBuilder builder = new StringBuilder(26);
                    appendNumber4(builder, (int) (ym / 13));
                    builder.append('-');
                    appendNumber2(builder, (int) (ym % 13));
                    builder.append('-');
                    appendNumber2(builder, (int) (ymd % (1 << 5)));
                    builder.append(' ');
                    appendNumber2(builder, (int) (hms >> 12));
                    builder.append(':');
                    appendNumber2(builder, (int) ((hms >> 6) % (1 << 6)));
                    builder.append(':');
                    appendNumber2(builder, (int) (hms % (1 << 6)));
                    secondString = builder.toString();
                }

                if (meta >= 1) {
                    String microsecondString = microsecondsToString(fracture, meta);
                    microsecondString = microsecondString.substring(0, meta);
                    return secondString + "." + microsecondString;
                } else {
                    return secondString;
                }
            }

            case mysql_type_time2: {
                // document show range '-838:59:59.000000' to '838:59:59.000000'
                long intPart = 0;
                int fracture = 0;
                long ltime = 0;

                final int meta = column.getMeta();
                switch (meta) {
                    case 0:
                        intPart = byteBuf.readUnsignedMedium() - time_int_ofs; // unsigned big-endian
                        ltime = intPart << 24;
                        break;

                    case 1:
                    case 2:
                        intPart = byteBuf.readUnsignedMedium() - time_int_ofs; // unsigned big-endian
                        fracture = byteBuf.readUnsignedByte(); // unsigned bit-endian
                        if (intPart < 0 && fracture > 0) {
                            intPart++;
                            fracture -= 0x100;
                        }

                        fracture = fracture * 10000;
                        ltime = intPart << 24;
                        break;

                    case 3:
                    case 4:
                        intPart = byteBuf.readUnsignedMedium() - time_int_ofs; // unsigned big-endian
                        fracture = byteBuf.readUnsignedShort(); // unsigned bit-endian
                        if (intPart < 0 && fracture > 0) {
                            intPart++;
                            fracture -= 0x100;
                        }

                        fracture = fracture * 100;
                        ltime = intPart << 24;
                        break;

                    case 5:
                    case 6:
                        final long high2 = byteBuf.readUnsignedShort(); // unsigned big-endian
                        final long low4 = byteBuf.readUnsignedInt(); // unsigned big-endian
                        intPart = low4 | (high2 << 32) - time_ofs;
                        ltime = intPart;
                        fracture = (int) (intPart % (1L << 24));
                        break;
                    default:
                        break;
                }

                String secondString;
                if (intPart == 0) {
                    secondString = "00:00:00";
                } else {
                    long ultime = Math.abs(ltime);
                    intPart = ultime >> 24;
                    StringBuilder builder = new StringBuilder(12);
                    if (ltime < 0) {
                        builder.append('-');
                    }

                    int d = (int) ((intPart >> 12) % (1 << 10));
                    if (d > 100) {
                        builder.append(String.valueOf(d));
                    } else {
                        appendNumber2(builder, d);
                    }
                    builder.append(':');
                    appendNumber2(builder, (int) ((intPart >> 6) % (1 << 6)));
                    builder.append(':');
                    appendNumber2(builder, (int) (intPart % (1 << 6)));
                    secondString = builder.toString();
                }

                if (meta >= 1) {
                    String microSecond = microsecondsToString(Math.abs(fracture), meta);
                    microSecond = microSecond.substring(0, meta);
                    return secondString + '.' + microSecond;
                } else {
                    return secondString;
                }
            }

            case mysql_type_year: {
                // '1901' to '2155'
                final short year = byteBuf.readUnsignedByte();
                if (year == 0) {
                    return "1901";
                } else {
                    return String.valueOf(1900 + year);
                }
            }

            case mysql_type_enum: {
                switch (column.getMeta()) {
                    case 1:
                        return byteBuf.readUnsignedByte();
                    case 2:
                        return byteBuf.readUnsignedShortLE();
                    default:
                        throw new IllegalStateException("enum type meta only be 1 or 2.");
                }
            }

            default:
                throw new UnsupportedOperationException(String.format("unsupported parse field value, column type is %d", type.getType()));
        }
    }

    private Charset getCharset(TableMapLogEvent.Column column) {
        final String charset = column.getCharset();
        final String collation = column.getCollation();
        if (null != charset && !"".equals(charset) && null != collation && !"".equals(collation)) {
            String javaCharset = CharsetConversion.getJavaCharset(charset, collation);
            if (null != javaCharset && Charset.isSupported(javaCharset)) {
                return Charset.forName(javaCharset);
            }
        }

        // default
        return UTF_8;
    }

    private long readNullBitSetLength(final BitSet presentBitMap, final long numberOfColumns) {
        long nullBitMapLength = 0;
        for (int i = 0; i < numberOfColumns; i++) {
            if (presentBitMap.get(i)) {
                nullBitMapLength++;
            }
        }
        return nullBitMapLength;
    }

    private boolean hasNextRow(final ByteBuf payloadBuf) {
        // the last 4byte of the event is checksum
        return payloadBuf.readableBytes() > 4;
    }

    private void appendNumber4(StringBuilder builder, int d) {
        if (d >= 1000) {
            builder.append(digits[d / 1000])
                    .append(digits[(d / 100) % 10])
                    .append(digits[(d / 10) % 10])
                    .append(digits[d % 10]);
        } else {
            builder.append('0');
            appendNumber3(builder, d);
        }
    }

    private void appendNumber3(StringBuilder builder, int d) {
        if (d >= 100) {
            builder.append(digits[d / 100]).append(digits[(d / 10) % 10]).append(digits[d % 10]);
        } else {
            builder.append('0');
            appendNumber2(builder, d);
        }
    }

    private void appendNumber2(StringBuilder builder, int d) {
        if (d >= 10) {
            builder.append(digits[(d / 10) % 10]).append(digits[d % 10]);
        } else {
            builder.append('0').append(digits[d]);
        }
    }

    private String microsecondsToString(int fracture, int meta) {
        String sec = String.valueOf(fracture);
        if (meta > 6) {
            throw new IllegalArgumentException("datetime replicator can't gt 6 : " + meta);
        }

        if (sec.length() < 6) {
            StringBuilder result = new StringBuilder(6);
            int len = 6 - sec.length();
            for (; len > 0; len--) {
                result.append('0');
            }
            result.append(sec);
            sec = result.toString();
        }

        return sec.substring(0, meta);
    }

    private int getDecimalBinarySize(int precision, int scale) {
        final int x = precision - scale;
        final int ipDigits = x / DIGITS_PER_4BYTES;
        final int fpDigits = scale / DIGITS_PER_4BYTES;
        final int ipDigitsX = x - ipDigits * DIGITS_PER_4BYTES;
        final int fpDigitsX = scale - fpDigits * DIGITS_PER_4BYTES;
        return (ipDigits << 2) + DECIMAL_BINARY_SIZE[ipDigitsX] + (fpDigits << 2) + DECIMAL_BINARY_SIZE[fpDigitsX];
    }

    private int toInt(byte[] data, int offset, int length) {
        int r = 0;
        for (int i = offset; i < (offset + length); i++) {
            final byte b = data[i];
            r = (r << 8) | (b >= 0 ? (int)b : (b + 256));
        }
        return r;
    }

    public RowsEventPostHeader getRowsEventPostHeader() {
        return rowsEventPostHeader;
    }

    protected long getNumberOfColumns() {
        return numberOfColumns;
    }

    protected BitSet getBeforePresentBitMap() {
        return beforePresentBitMap;
    }

    protected BitSet getAfterPresentBitMap() {
        return afterPresentBitMap;
    }

    protected List<Row> getRows() {
        return rows;
    }

    public Long getChecksum() {
        return checksum;
    }
}
