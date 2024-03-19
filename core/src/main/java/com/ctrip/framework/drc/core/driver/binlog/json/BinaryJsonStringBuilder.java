package com.ctrip.framework.drc.core.driver.binlog.json;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * @ClassName BinaryJsonFormatter
 * @Author haodongPan
 * @Date 2024/2/5 18:07
 * @Version: $
 */
public class BinaryJsonStringBuilder implements JsonStringBuilder {

    private final StringBuilder sb = new StringBuilder();

    @Override
    public String toString() {
        return getString();
    }

    public String getString() {
        return sb.toString();
    }

    @Override
    public void beginObject(int numElements) {
        sb.append('{');
    }

    @Override
    public void beginArray(int numElements) {
        sb.append('[');
    }

    @Override
    public void endObject() {
        sb.append('}');
    }

    @Override
    public void endArray() {
        sb.append(']');
    }

    @Override
    public void name(String name) {
        sb.append('"');
        appendString(name);
        sb.append("\":");
    }

    @Override
    public void value(String value) {
        sb.append('"');
        appendString(value);
        sb.append('"');
    }

    @Override
    public void value(int value) {
        sb.append(Integer.toString(value));
    }

    @Override
    public void value(long value) {
        sb.append(Long.toString(value));
    }

    @Override
    public void value(double value) {
        String str = Double.toString(value);
        if (str.contains("E")) {
            value(new BigDecimal(value));
        } else {
            sb.append(str);
        }
    }

    @Override
    public void value(BigInteger value) {
        value(new BigDecimal(value));
    }

    @Override
    public void value(BigDecimal value) {
        sb.append(value.toPlainString());
    }

    @Override
    public void value(boolean value) {
        sb.append(Boolean.toString(value));
    }

    @Override
    public void valueNull() {
        sb.append("null");
    }

    @Override
    public void valueDate(int year, int month, int day) {
        sb.append('"');
        appendDate(year, month, day);
        sb.append('"');
    }

    @Override
    public void valueDatetime(int year, int month, int day, int hour, int min, int sec, int microSeconds) {
        sb.append('"');
        appendDate(year, month, day);
        sb.append(' ');
        appendTime(hour, min, sec, microSeconds);
        sb.append('"');
    }

    @Override
    public void valueTime(int hour, int min, int sec, int microSeconds) {
        sb.append('"');
        if (hour < 0) {
            sb.append('-');
            hour = Math.abs(hour);
        }
        appendTime(hour, min, sec, microSeconds);
        sb.append('"');
    }

    @Override
    public void nextEntry() {
        sb.append(',');
    }

    /**
     * Append a string by escaping any characters that must be escaped.
     *
     * @param original the string to be written; may not be null
     */
    protected void appendString(String original) {
        int endIndex = original.length();
        for (int i = 0; i < endIndex; ++i) {
            char c = original.charAt(i);
            if (c == '"') {
                sb.append("\\\"");
            } else if (c == '\n') {
                sb.append("\\n");
            } else if (c == '\r') {
                sb.append("\\r");
            } else if (c == '\\') {
                sb.append("\\\\");
            } else if (c == '\t') {
                sb.append("\\t");
            } else if (c < 16) {
                sb.append("\\u000");
                sb.append(Integer.toHexString(c));
            } else if (c < 32) {
                sb.append("\\u00");
                sb.append(Integer.toHexString(c));
            } else if (c >= 0x7f && c <= 0xA0) {
                sb.append("\\u00");
                sb.append(Integer.toHexString(c));
            } else {
                sb.append(c);
            }
        }
    }


    protected void appendTwoDigit(int value) {
        assert value >= 0;
        assert value < 100;
        if (value < 10) {
            sb.append("0").append(value);
        } else {
            sb.append(value);
        }
    }

    protected void appendFourDigit(int value) {
        if (value < 10) {
            sb.append("000").append(value);
        } else if (value < 100) {
            sb.append("00").append(value);
        } else if (value < 1000) {
            sb.append("0").append(value);
        } else {
            sb.append(value);
        }
    }

    protected void appendSixDigit(int value) {
        assert value > 0;
        assert value < 1000000;
        if (value < 10) {
            sb.append("00000");
        } else if (value < 100) {
            sb.append("0000");
        } else if (value < 1000) {
            sb.append("000");
        } else if (value < 10000) {
            sb.append("00");
        } else if (value < 100000) {
            sb.append("0");
        }
        sb.append(value);
    }

    protected void appendDate(int year, int month, int day) {
        if (year < 0) {
            sb.append('-');
            year = Math.abs(year);
        }
        appendFourDigit(year);
        sb.append('-');
        appendTwoDigit(month);
        sb.append('-');
        appendTwoDigit(day);
    }

    protected void appendTime(int hour, int min, int sec, int microSeconds) {
        appendTwoDigit(hour);
        sb.append(':');
        appendTwoDigit(min);
        sb.append(':');
        appendTwoDigit(sec);
        if (microSeconds != 0) {
            sb.append('.');
            appendSixDigit(microSeconds);
        }
    }
}
