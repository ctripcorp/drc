package com.ctrip.framework.drc.core.driver.binlog.json;

/**
 * @ClassName BinaryJson
 * @Author haodongPan
 * @Date 2024/2/19 14:38
 * @Version: $
 */

import com.ctrip.framework.drc.core.driver.binlog.constant.MysqlFieldType;
import com.ctrip.framework.drc.core.driver.binlog.impl.AbstractRowsEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * <h2>Binary Format</h2>
 *
 * Each JSON value (scalar, object or array) has a one byte type identifier followed by the actual value.
 *
 * <h3>Scalar</h3>
 *
 * The binary value may contain a single scalar that is one of:
 * <ul>
 * <li>null</li>
 * <li>boolean</li>
 * <li>int16</li>
 * <li>int32</li>
 * <li>int64</li>
 * <li>uint16</li>
 * <li>uint32</li>
 * <li>uint64</li>
 * <li>double</li>
 * <li>string</li>
 * <li>{@code DATE} as a string of the form {@code YYYY-MM-DD} where {@code YYYY} can be positive or negative</li>
 * <li>{@code TIME} as a string of the form {@code HH-MM-SS} where {@code HH} can be positive or negative</li>
 * <li>{@code DATETIME} as a string of the form {@code YYYY-MM-DD HH-mm-SS.ssssss} where {@code YYYY} can be positive or
 * negative</li>
 * <li>{@code TIMESTAMP} as the number of microseconds past epoch (January 1, 1970), or if negative the number of
 * microseconds before epoch (January 1, 1970)</li>
 * <li>any other MySQL value encoded as an opaque binary value</li>
 * </ul>
 *
 * <h3>JSON Object</h3>
 *
 * If the value is a JSON object, its binary representation will have a header that contains:
 * <ul>
 * <li>the member count</li>
 * <li>the size of the binary value in bytes</li>
 * <li>a list of pointers to each key</li>
 * <li>a list of pointers to each value</li>
 * </ul>
 *
 * The actual keys and values will come after the header, in the same order as in the header.
 *
 * <h3>JSON Array</h3>
 *
 * If the value is a JSON array, the binary representation will have a header with
 * <ul>
 * <li>the element count</li>
 * <li>the size of the binary value in bytes</li>
 * <li>a list of pointers to each value</li>
 * </ul>
 * followed by the actual values, in the same order as in the header.
 *
 * <h2>Grammar</h2>
 * The grammar of the binary representation of JSON objects are defined in the MySQL codebase in the
 * <a href="https://github.com/mysql/mysql-server/blob/5.7/sql/json_binary.h">json_binary.h</a> file:
 * <p>
 *
 * <pre>
 *   doc ::= type value
 *   type ::=
 *       0x00 |  // small JSON object
 *       0x01 |  // large JSON object
 *       0x02 |  // small JSON array
 *       0x03 |  // large JSON array
 *       0x04 |  // literal (true/false/null)
 *       0x05 |  // int16
 *       0x06 |  // uint16
 *       0x07 |  // int32
 *       0x08 |  // uint32
 *       0x09 |  // int64
 *       0x0a |  // uint64
 *       0x0b |  // double
 *       0x0c |  // utf8mb4 string
 *       0x0f    // custom data (any MySQL data type)
 *   value ::=
 *       object  |
 *       array   |
 *       literal |
 *       number  |
 *       string  |
 *       custom-data
 *   object ::= element-count size key-entry* value-entry* key* value*
 *   array ::= element-count size value-entry* value*
 *   // number of members in object or number of elements in array
 *   element-count ::=
 *       uint16 |  // if used in small JSON object/array
 *       uint32    // if used in large JSON object/array
 *   // number of bytes in the binary representation of the object or array
 *   size ::=
 *       uint16 |  // if used in small JSON object/array
 *       uint32    // if used in large JSON object/array
 *   key-entry ::= key-offset key-length
 *   key-offset ::=
 *       uint16 |  // if used in small JSON object
 *       uint32    // if used in large JSON object
 *   key-length ::= uint16    // key length must be less than 64KB
 *   value-entry ::= type offset-or-inlined-value
 *   // This field holds either the offset to where the value is stored,
 *   // or the value itself if it is small enough to be inlined (that is,
 *   // if it is a JSON literal or a small enough [u]int).
 *   offset-or-inlined-value ::=
 *       uint16 |   // if used in small JSON object/array
 *       uint32     // if used in large JSON object/array
 *   key ::= utf8mb4-data
 *   literal ::=
 *       0x00 |   // JSON null literal
 *       0x01 |   // JSON true literal
 *       0x02 |   // JSON false literal
 *   number ::=  ....  // little-endian format for [u]int(16|32|64), whereas
 *                     // double is stored in a platform-independent, eight-byte
 *                     // format using float8store()
 *   string ::= data-length utf8mb4-data
 *   custom-data ::= throws IOException
 *   custom-type ::= uint8   // type identifier that matches the
 *                           // internal enum_field_types enum
 *   data-length ::= uint8*  // If the high bit of a byte is 1, the length
 *                           // field is continued in the next byte,
 *                           // otherwise it is the last byte of the length
 *                           // field. So we need 1 byte to represent
 *                           // lengths up to 127, 2 bytes to represent
 *                           // lengths up to 16383, and so on...
 * </pre>
 * @author <a href="mailto:rhauch@gmail.com">Randall Hauch</a>
 * https://github.com/mysql/mysql-server/blob/8.0/sql-common/json_binary.h
 */


public class BinaryJson {

    private static final Logger logger = LoggerFactory.getLogger(BinaryJson.class);
    
    private final java.io.ByteArrayInputStream reader;
    
    public static String parseAsString(byte[] binaryJson) throws IOException {
        return new BinaryJson(binaryJson).parseAsString();
    }
    
    public String parseAsString() throws IOException {
        BinaryJsonStringBuilder formatter = new BinaryJsonStringBuilder();
        parse(formatter);
        return formatter.toString();
    }
    
    public BinaryJson(byte[] binary) throws IOException {
        this.reader = new java.io.ByteArrayInputStream(binary);
    }
    
    public BinaryJson(java.io.ByteArrayInputStream binary) {
        this.reader = binary;
    }
    
    
    public void parse(JsonStringBuilder formatter) throws IOException {
        parse(readValueType(), formatter);
    }
    
    protected void parse(ValueType type, JsonStringBuilder formatter) throws IOException {
        switch (type) {
            case SMALL_OBJECT:
                parseObject(true,formatter);
                break;
            case LARGE_OBJECT:
                parseObject(false,formatter);
                break;
            case SMALL_ARRAY:
                parseArray(true,formatter);
                break;
            case LARGE_ARRAY:
                parseArray(false,formatter);
                break;
            case LITERAL:
                parseBoolean(formatter);
                break;
            case INT16:
                parseInt16(formatter);
                break;
            case UINT16:
                parseUInt16(formatter);
                break;
            case INT32:
                parseInt32(formatter);
                break;
            case UINT32:
                parseUInt32(formatter);
                break;
            case INT64:
                parseInt64(formatter);
                break;
            case UINT64:
                parseUInt64(formatter);
                break;
            case DOUBLE:
                parseDouble(formatter);
                break;
            case STRING:
                parseString(formatter);
                break;
            case OPAQUE:
                parseOpaque(formatter);
                break;
            default:
                throw new IOException("Unknown type value: " + asHex(type.getCode()));
        }
    }
    
    
    protected void parseBoolean(JsonStringBuilder formatter) throws IOException {
        Boolean booleanValue = readLiteral();
        if (booleanValue == null) {
            formatter.valueNull();
            return;
        }
        formatter.value(booleanValue);
    }

    protected void parseInt16(JsonStringBuilder formatter) throws IOException {
        formatter.value(readInt16());
    }

    protected void parseUInt16(JsonStringBuilder formatter) throws IOException {
        formatter.value(readUInt16());
    }

    protected void parseInt32(JsonStringBuilder formatter) throws IOException {
        formatter.value(readInt32());
    }

    protected void parseUInt32(JsonStringBuilder formatter) throws IOException {
        formatter.value(readUInt32());
    }

    protected void parseInt64(JsonStringBuilder formatter) throws IOException {
        formatter.value(readInt64());
    }

    protected void parseUInt64(JsonStringBuilder formatter) throws IOException {
        formatter.value(readUInt64BigEndian());
    }

    protected void parseString(JsonStringBuilder formatter) throws IOException {
        int dataLength = read_variable_length();
        byte[] value = readByteArray(dataLength);
        formatter.value(new String(value, StandardCharsets.UTF_8));
    }
    
    protected void parseDouble(JsonStringBuilder formatter) throws IOException {
        long bitsValue = readInt64();
        double value = Double.longBitsToDouble(bitsValue);
        formatter.value(value);
    }

    /**
     *   custom-data ::= throws IOException
     *   custom-type ::= uint8   // type identifier that matches the
     *                           // internal enum_field_types enum
     *   data-length ::= uint8*  // If the high bit of a byte is 1, the length
     *                           // field is continued in the next byte,
     *                           // otherwise it is the last byte of the length
     *                           // field. So we need 1 byte to represent
     *                           // lengths up to 127, 2 bytes to represent
     *                           // lengths up to 16383, and so on...
     * @param formatter
     * @throws IOException
     */
    // See 'Json_decimal::convert_from_binary'
    // https://github.com/mysql/mysql-server/blob/5.7/sql/json_dom.cc#L1625

    // All dates and times are in one of these types
    // See 'Json_datetime::to_packed' for details
    // https://github.com/mysql/mysql-server/blob/5.7/sql/json_dom.cc#L1681
    // which calls 'TIME_to_longlong_packed'
    // https://github.com/mysql/mysql-server/blob/5.7/sql-common/my_time.c#L2005
    // and 'Json_datetime::from_packed'
    // https://github.com/mysql/mysql-server/blob/5.7/sql/json_dom.cc#L1688
    // which calls 'TIME_from_longlong_packed'
    // https://github.com/mysql/mysql-server/blob/5.7/sql/sql_time.cc#L1624
    
    protected void parseOpaque(JsonStringBuilder formatter) throws IOException {
        int typeCode = reader.read();
        MysqlFieldType mysqlFieldType = MysqlFieldType.getMysqlFieldType(typeCode);
        int dataLength = read_variable_length();
        switch (mysqlFieldType) {
            case mysql_type_decimal:
            case mysql_type_newdecimal:
                parseDecimal(dataLength,formatter);
                break;
            case mysql_type_time:
            case mysql_type_time2:
                parseTime(formatter);
                break;
            case mysql_type_date:
            case mysql_type_newdate:
                parseDate(formatter);
                break;
            case mysql_type_datetime:
            case mysql_type_datetime2:
            case mysql_type_timestamp:
            case mysql_type_timestamp2:
                parseDatetime(formatter);
                break;
            default:
                parseOpaqueAsString(dataLength,formatter);
                break;
        }
    }

    private void parseOpaqueAsString(int dataLength, JsonStringBuilder formatter) throws IOException {
        formatter.value(new String(readByteArray(dataLength),StandardCharsets.UTF_8));
    }


    /*** TIME low-level memory and disk representation routines ***/

    /*
      In-memory format:
    
       1  bit sign          (Used for sign, when on disk)
       1  bit unused        (Reserved for wider hour range, e.g. for intervals)
       10 bit hour          (0-836)
       6  bit minute        (0-59)
       6  bit second        (0-59)
      24  bits microseconds (0-999999)
    */
    private void parseTime(JsonStringBuilder formatter) throws IOException {
        long raw = readInt64();
        long value = raw >> 24;
        boolean negative = value < 0L;
        int hour = (int) ((value >> 12) % (1 << 10)); // 10 bits starting at 12th
        int min = (int) ((value >> 6) % (1 << 6)); // 6 bits starting at 6th
        int sec = (int) (value % (1 << 6)); // 6 bits starting at 0th
        if (negative) {
            hour *= -1;
        }
        int microSeconds = (int) (raw % (1 << 24));
        formatter.valueTime(hour, min, sec, microSeconds);

    }

    protected void parseDate(JsonStringBuilder formatter) throws IOException {
        long raw = readInt64();
        long value = raw >> 24;
        int yearMonth = (int) ((value >> 22) % (1 << 17)); // 17 bits starting at 22nd
        int year = yearMonth / 13;
        int month = yearMonth % 13;
        int day = (int) ((value >> 17) % (1 << 5)); // 5 bits starting at 17th
        formatter.valueDate(year, month, day);
    }

    /**
     @page datetime_and_date_low_level_rep DATETIME and DATE

     | Bits  | Field         | Value |
     | ----: | :----         | :---- |
     |    1  | sign          |(used when on disk) |
     |   17  | year*13+month |(year 0-9999, month 0-12) |
     |    5  | day           |(0-31)|
     |    5  | hour          |(0-23)|
     |    6  | minute        |(0-59)|
     |    6  | second        |(0-59)|
     |   24  | microseconds  |(0-999999)|

     Total: 64 bits = 8 bytes

     @verbatim
     Format: SYYYYYYY.YYYYYYYY.YYdddddh.hhhhmmmm.mmssssss.ffffffff.ffffffff.ffffffff
     @endverbatim
     https://github.com/mysql/mysql-server/blob/trunk/mysys/my_time.cc#L1902
     */
    protected void parseDatetime(JsonStringBuilder formatter) throws IOException {
        long raw = readInt64();
        long ymdhms = raw >> 24;
        int yearMonth = (int) ((ymdhms >> 22) % (1 << 17)); // 17 bits starting at 22nd
        int year = yearMonth / 13;
        int month = yearMonth % 13;
        int day = (int) ((ymdhms >> 17) % (1 << 5)); // 5 bits starting at 17th
        int hour = (int) ((ymdhms >> 12) % (1 << 5)); // 5 bits starting at 12th
        int min = (int) ((ymdhms >> 6) % (1 << 6)); // 6 bits starting at 6th 
        int sec = (int) (ymdhms % (1 << 6)); // 6 bits starting at 0th
        int microSeconds = (int) (raw % (1 << 24));
        formatter.valueDatetime(year, month, day, hour, min, sec, microSeconds);
    }

    private void parseDecimal(int dataLength, JsonStringBuilder formatter) throws IOException {
        int precision = reader.read();
        int scale = reader.read();
        
        byte[] value = readByteArray(dataLength-2);
        formatter.value(AbstractRowsEvent.readAsBigDecimal(value, precision, scale));
    }
    
    /**
    https://github.com/mysql/mysql-server/blob/8.0/sql-common/json_binary.cc#L284
    It takes five bytes to represent UINT_MAX32, which is the largest
    supported length, so don't look any further.
    **/
    private int read_variable_length() throws IOException {
        int length = 0;
        byte b;
        int max_bytes = 5;
        for (int i=0;i < max_bytes;i++) {
            b = (byte) reader.read();
            length = ((b & 0x7F) << (i * 7)) | length;
            if ((b & 0x80) == 0) {
                return length;
            }
        }
        throw new IOException("read_variable_length, length " + asHex(length));
    }

    private ValueType readValueType() throws IOException {
        int type = reader.read();
        ValueType valueType = ValueType.byCode(type);
        if (valueType == null) {
            throw new IOException("Unknown value type: " + type);
        }
        return valueType;
    }
    
    protected void parseArray(boolean small, JsonStringBuilder formatter) throws IOException {
        int numElements = readAndCheckIndex(Integer.MAX_VALUE, small, "number of elements in");
        int numBytes = readAndCheckIndex(Integer.MAX_VALUE, small, "size of");
        int valueSize = small ? 2 : 4;
        // read each value-entry
        ValueEntry[] valueEntries = new ValueEntry[numElements];
        for (int i = 0; i != numElements; ++i) {
            ValueType type = readValueType(); //[0,-32768,65535,-2147483648,4294967295,-9223372036854775808,18446744073709551615]
            switch (type) {
                case LITERAL:
                    valueEntries[i] = new ValueEntry(type).setValue(readLiteral());
                    reader.skip(valueSize-1);
                    break;
                case INT16:
                    valueEntries[i] = new ValueEntry(type).setValue(readInt16());
                    reader.skip(valueSize-2);
                    break;
                case UINT16:
                    valueEntries[i] = new ValueEntry(type).setValue(readUInt16());
                    reader.skip(valueSize-2);
                    break;
                case INT32:
                    if (!small) {
                        valueEntries[i] = new ValueEntry(type).setValue(readInt32());
                        break;
                    }
                case UINT32:
                    if (!small) {
                        valueEntries[i] = new ValueEntry(type).setValue(readUInt32());
                        break;
                    }
                default:
                    int valueOffset = readAndCheckIndex(Integer.MAX_VALUE, small, "offset of value in");
                    if (valueOffset >= numBytes) {
                        throw new IOException("the valueOffset is" + valueOffset +
                                " and is too large for the binary json size (" + numBytes + ")");
                    }
                    valueEntries[i] = new ValueEntry(type, valueOffset);
            }
        }

        formatter.beginArray(numElements);
        // read each value in array
        for (int i = 0; i != numElements; ++i) {
            if (i != 0) {
                formatter.nextEntry();
            }
            ValueEntry entry = valueEntries[i];
            if (entry.resolved) {
                Object value = entry.value;
                if (value == null) {
                    formatter.valueNull();
                } else if (value instanceof Boolean) {
                    formatter.value((Boolean)value);
                } else if (value instanceof Integer) {
                    formatter.value((Integer) value);
                } else {
                    throw new IOException("Unexpected value type: " + entry.value.getClass());
                }
            } else {
                parse(entry.type,formatter);
            }
        }
        formatter.endArray();
    }

    protected void parseObject(boolean small, JsonStringBuilder formatter) throws IOException  {
        // Read the header 
        int numElements = readAndCheckIndex(Integer.MAX_VALUE, small, "number of elements in");
        int numBytes = readAndCheckIndex(Integer.MAX_VALUE, small, "size of");
        int valueSize = small ? 2 : 4;

        // Read each key-entry, consisting of the offset and length of each key
        int[] keyLengths = new int[numElements];
        for (int i = 0; i != numElements; ++i) {
            int offset = readAndCheckIndex(numBytes, small, "offset of key in");
            int length = readUInt16();
            keyLengths[i] = length;
        }
        
        // Read each key value value-entry
        ValueEntry[] valueEntries = new ValueEntry[numElements];
        for (int i = 0; i != numElements; ++i) {
            ValueType type = readValueType();
            switch (type) {
                case LITERAL:
                    valueEntries[i] = new ValueEntry(type).setValue(readLiteral());
                    reader.skip(valueSize - 1);
                    break;
                case INT16:
                    valueEntries[i] = new ValueEntry(type).setValue(readInt16());
                    reader.skip(valueSize - 2);
                    break;
                case UINT16:
                    valueEntries[i] = new ValueEntry(type).setValue(readUInt16());
                    reader.skip(valueSize - 2);
                    break;
                case INT32:
                    if (!small) {
                        valueEntries[i] = new ValueEntry(type).setValue(readInt32());
                        break;
                    }
                case UINT32:
                    if (!small) {
                        valueEntries[i] = new ValueEntry(type).setValue(readUInt32());
                        break;
                    }
                default:
                    // it's an offset, so read the offset and store it
                    int valueOffset = readAndCheckIndex(Integer.MAX_VALUE, small, "offset of value in");
                    if (valueOffset > numBytes) {
                        throw new IOException("The offset of the value in the JSON document is " + valueOffset +
                                " and is too big for the binary form of the document (" + numBytes + ")");
                    }
                    valueEntries[i] = new ValueEntry(type,valueOffset);
            }
        }
        
        // read key
        String[] keys = new String[numElements];
        for (int i = 0; i != numElements; ++i) {
            keys[i] = readString(keyLengths[i]);
        }
        
        // read value
        formatter.beginObject(numElements);
        for (int i = 0; i != numElements; ++i) {
            if (i != 0) {
                formatter.nextEntry();
            }
            formatter.name(keys[i]);
            ValueEntry valueEntry = valueEntries[i];
            if (valueEntry.resolved) {
                Object value = valueEntry.value;
                if (value == null) {
                    formatter.valueNull();
                } else if (value instanceof Boolean) {
                    formatter.value((Boolean) value);
                } else if (value instanceof Integer) {
                    formatter.value((Integer) value);
                } else {
                    throw new IOException("Unexpected value type: " + value.getClass());
                }
            } else {
                parse(valueEntry.type,formatter); // recursive call
            }
        }
        formatter.endObject();
    }
    
    

    protected int readAndCheckIndex(int maxValue, boolean isSmall, String desc) throws IOException {
        long result = isSmall ? readUInt16() : readUInt32();
        if (result > maxValue) {
            throw new IOException("The " + desc + " the JSON document is " + result +
                    " and is too big large the binary form of the document (" + maxValue + ")");
        }
        if (result > Integer.MAX_VALUE) {
            throw new IOException("The " + desc + " the JSON document is " + result + " and is too big to be used");
        }
        return (int) result;
    }

    protected byte[] readByteArray(int len) throws IOException {
        byte[] bytes = new byte[len];
        int remaining = len;
        while (remaining != 0) {
            int read = reader.read(bytes,len - remaining, remaining);
            if (read == -1) {
                throw new EOFException();
            }
            remaining -= read;
        }
        return bytes;
    }
    
    
    protected String readString(int len) throws IOException {
        return new String(readByteArray(len));
    }

    protected int readInt16() throws IOException {
        int b1 = reader.read() & 0xFF;
        int b2 = reader.read();
        return (short) (b2 << 8 | b1);
    }

    protected int readUInt16() throws IOException {
        int b1 = reader.read() & 0xFF;
        int b2 = reader.read() & 0xFF;
        return (b2 << 8 | b1) & 0xFFFF;
    }

    protected int readInt32() throws IOException {
        int b1 = reader.read() & 0xFF;
        int b2 = reader.read() & 0xFF;
        int b3 = reader.read() & 0xFF;
        int b4 = reader.read();
        return b4 << 24 | b3 << 16 | b2 << 8 | b1;
    }
    
    protected long readUInt32() throws IOException {
        int b1 = reader.read() & 0xFF;
        int b2 = reader.read() & 0xFF;
        int b3 = reader.read() & 0xFF;
        int b4 = reader.read() & 0xFF;
        return (long) ((b4 << 24) | (b3 << 16) | (b2 << 8) | b1);
    }

    protected long readInt64() throws IOException {
        int b1 = reader.read() & 0xFF;
        int b2 = reader.read() & 0xFF;
        int b3 = reader.read() & 0xFF;
        long b4 = reader.read() & 0xFF;
        long b5 = reader.read() & 0xFF;
        long b6 = reader.read() & 0xFF;
        long b7 = reader.read() & 0xFF;
        long b8 = reader.read();
        return b8 << 56 | (b7 << 48) | (b6 << 40) | (b5 << 32) |
                (b4 << 24) | (b3 << 16) | (b2 << 8) | b1;
    }

    protected BigInteger readUInt64BigEndian() throws IOException {
        byte[] bigEndian = new byte[8];
        for (int i = 8; i != 0; --i) {
            bigEndian[i - 1] = (byte) (reader.read() & 0xFF);
        }
        return new BigInteger(1, bigEndian);
    }

    protected Boolean readLiteral() throws IOException {
        byte b = (byte) reader.read();
        if (b == 0x00) {
            return null;
        } else if (b == 0x01) {
            return Boolean.TRUE;
        } else if (b == 0x02) {
            return Boolean.FALSE;
        }
        throw new IOException("Unexpected value '" + asHex(b) + "' for literal");
    }

    protected static String asHex(byte b) {
        return String.format("%02X ", b);
    }

    protected static String asHex(int value) {
        return Integer.toHexString(value);
    }

    /**
     * store value entry information.
     */
    protected static final class ValueEntry {

        protected final ValueType type;
        protected final int index;
        protected Object value;
        protected boolean resolved;

        public ValueEntry(ValueType type) {
            this.type = type;
            this.index = 0;
        }

        public ValueEntry(ValueType type, int index) {
            this.type = type;
            this.index = index;
        }

        public ValueEntry setValue(Object value) { // used for small value save in offset-line
            this.value = value;
            this.resolved = true;
            return this;
        }
    }
}
