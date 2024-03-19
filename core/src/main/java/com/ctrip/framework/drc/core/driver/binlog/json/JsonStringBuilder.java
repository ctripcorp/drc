package com.ctrip.framework.drc.core.driver.binlog.json;

import java.math.BigDecimal;
import java.math.BigInteger;

public interface JsonStringBuilder {
    
    void beginObject(int numElements);
    
    void beginArray(int numElements);
    
    void endObject();
    
    void endArray();

    /**
     * @param name the element's name; never null
     */
    void name(String name);

    /**
     * @param value the element's value; never null
     */
    void value(String value);
  
    void value(int value);
    
    void value(long value);
    
    void value(double value);
    
    void value(BigInteger value);
    
    void value(BigDecimal value);
    
    void value(boolean value);
    
    void valueNull();

    /**
     * Receive the date value of an element in a JSON object.
     * Range: 1000-01-01 - 9999-12-31
     * @param year the year in the element's date value
     * @param month the month (0-12) in the element's date value
     * @param day the day of the month (0-31) in the element's date value
     */
    void valueDate(int year, int month, int day);

    /**
     * Receive the date and time value of an element in a JSON object.
     * Range: 1000-01-01 00:00:00.000000 - 9999-12-31 23:59:59.499999
     * @param year the year in the element's date value
     * @param month the month (0-12) in the element's date value
     * @param day the day of the month (0-31) in the element's date value
     * @param hour the hour of the day (0-24) in the element's time value
     * @param min the minutes of the hour (0-60) in the element's time value
     * @param sec the seconds of the minute (0-60) in the element's time value
     * @param microSeconds the number of microseconds in the element's time value
     */
    void valueDatetime(int year, int month, int day, int hour, int min, int sec, int microSeconds);

    /**
     * Receive the time value of an element in a JSON object.
     * Range: -838:59:59.000000 - 838:59:59.000000
     * @param hour the positive or negative hour  in the element's time value
     * @param min the minutes of the hour (0-60) in the element's time value
     * @param sec the seconds of the minute (0-60) in the element's time value
     * @param microSeconds the number of microseconds in the element's time value
     */
    void valueTime(int hour, int min, int sec, int microSeconds);

    /**
     * Called after an entry signaling that another entry will be signaled.
     */
    void nextEntry();

}