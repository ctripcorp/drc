package com.ctrip.framework.drc.console.common;

import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.google.common.reflect.TypeToken;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

/**
 * Created by dengquanliang
 * 2023/11/20 17:00
 */
public class ObjectTest {

    @Test
    public void testEqual() {
        Map<String, Object> srcRecord = getSrcRecord();
        Map<String, Object> dstRecord = getDstRecord();
        List<String> columns = Lists.newArrayList(srcRecord.keySet());

        boolean equal = recordIsEqual(columns, srcRecord, dstRecord);
        System.out.println(equal);
        Assert.assertTrue(equal);
    }

    private Map<String, Object> getSrcRecord() {
        String record = "{\n" +
                "    \"id\": 11188360,\n" +
                "    \"uid\": \"_TIHKqlz1vs7xo9a\",\n" +
                "    \"source\": \"ANDROID\",\n" +
                "    \"page_type\": 0,\n" +
                "    \"head\": \"{\\\"source\\\":\\\"ANDROID\\\",\\\"currency\\\":\\\"HKD\\\",\\\"version\\\":\\\"789.002\\\",\\\"uid\\\":\\\"_TIHKqlz1vs7xo9a\\\",\\\"vid\\\":\\\"21BBBC40B42311EDF3DB1EE96FB247B5\\\",\\\"isQuickBooking\\\":0,\\\"clientID\\\":\\\"37002171311140226598\\\",\\\"deviceID\\\":\\\"477af10bc05c0c97\\\",\\\"aPIKey\\\":\\\"CtripAndriod\\\",\\\"serviceCode\\\":null,\\\"locale\\\":\\\"zh_HK\\\",\\\"rnversion\\\":null}\",\n" +
                "    \"page_id\": 10650092015,\n" +
                "    \"email\": \"\",\n" +
                "    \"biz_type\": \"HKAirportExpress\",\n" +
                "    \"itinerary_type\": 0,\n" +
                "    \"departure_station\": \"{\\\"stationName\\\":\\\"香港西九龍\\\",\\\"stationLocationCode\\\":\\\"香港西九龙\\\",\\\"countryCode\\\":\\\"HK\\\"}\",\n" +
                "    \"arrive_station_info\": \"{\\\"stationName\\\":\\\"潮州\\\",\\\"stationLocationCode\\\":\\\"潮州\\\",\\\"countryCode\\\":\\\"CN\\\"}\",\n" +
                "    \"out_time\": \"{\\\"departureDateTime\\\":\\\"2023-12-03\\\",\\\"arrivalDateTime\\\":null}\",\n" +
                "    \"return_time\": \"\",\n" +
                "    \"rail_card_info\": \"\",\n" +
                "    \"passenger_info\": \"\",\n" +
                "    \"create_time\": \"2023-11-20 13:33:06.64\",\n" +
                "    \"datachange_lasttime\": \"2023-11-20 17:03:52.693\",\n" +
                "    \"client_id\": \"37002171311140226598\",\n" +
                "    \"drc_row_log_id\": 163567512\n" +
                "}";

        Map<String, Object> records = JsonUtils.fromJson(record, new TypeToken<Map<String, Object>>() {
        }.getType());

        records.put("drc_row_log_id", 163567512L);
        records.put("id", 11188360L);
        records.put("page_id", 10650092015L);
        records.put("itinerary_type", 0);
        records.put("page_type", 0);
        Timestamp createTime = new Timestamp(1700458386000L);
        Timestamp updateTime = new Timestamp(1700442232000L);
        records.put("create_time", createTime);
        records.put("datachange_lasttime", updateTime);
        return records;
    }

    private Map<String, Object> getDstRecord() {
        String record = "{\n" +
                "    \"id\": 11188360,\n" +
                "    \"uid\": \"_TIHKqlz1vs7xo9a\",\n" +
                "    \"source\": \"ANDROID\",\n" +
                "    \"page_type\": 0,\n" +
                "    \"head\": \"{\\\"source\\\":\\\"ANDROID\\\",\\\"currency\\\":\\\"HKD\\\",\\\"version\\\":\\\"789.002\\\",\\\"uid\\\":\\\"_TIHKqlz1vs7xo9a\\\",\\\"vid\\\":\\\"21BBBC40B42311EDF3DB1EE96FB247B5\\\",\\\"isQuickBooking\\\":0,\\\"clientID\\\":\\\"37002171311140226598\\\",\\\"deviceID\\\":\\\"477af10bc05c0c97\\\",\\\"aPIKey\\\":\\\"CtripAndriod\\\",\\\"serviceCode\\\":null,\\\"locale\\\":\\\"zh_HK\\\",\\\"rnversion\\\":null}\",\n" +
                "    \"page_id\": 10650092015,\n" +
                "    \"email\": \"\",\n" +
                "    \"biz_type\": \"HKAirportExpress\",\n" +
                "    \"itinerary_type\": 0,\n" +
                "    \"departure_station\": \"{\\\"stationName\\\":\\\"香港西九龍\\\",\\\"stationLocationCode\\\":\\\"香港西九龙\\\",\\\"countryCode\\\":\\\"HK\\\"}\",\n" +
                "    \"arrive_station_info\": \"{\\\"stationName\\\":\\\"潮州\\\",\\\"stationLocationCode\\\":\\\"潮州\\\",\\\"countryCode\\\":\\\"CN\\\"}\",\n" +
                "    \"out_time\": \"{\\\"departureDateTime\\\":\\\"2023-12-03\\\",\\\"arrivalDateTime\\\":null}\",\n" +
                "    \"return_time\": \"\",\n" +
                "    \"rail_card_info\": \"\",\n" +
                "    \"passenger_info\": \"\",\n" +
                "    \"create_time\": \"2023-11-20 13:33:06.64\",\n" +
                "    \"datachange_lasttime\": \"2023-11-20 17:03:52.693\",\n" +
                "    \"client_id\": \"37002171311140226598\",\n" +
                "    \"drc_row_log_id\": 163567512\n" +
                "}";

        Map<String, Object> records = JsonUtils.fromJson(record, new TypeToken<Map<String, Object>>() {
        }.getType());

        records.put("drc_row_log_id", 163567512L);
        records.put("id", 11188360L);
        records.put("page_id", 10650092015L);
        records.put("itinerary_type", 0);
        records.put("page_type", 0);
        Timestamp createTime = new Timestamp(1700458386000L);
        Timestamp updateTime = new Timestamp(1700442232000L);
        records.put("create_time", createTime);
        records.put("datachange_lasttime", updateTime);
        return records;
    }

    private boolean recordIsEqual(List<String> columns, Map<String, Object> srcRecord, Map<String, Object> dstRecord) {
        for (String column : columns) {
            Object srcValue = srcRecord.get(column);
            Object dstValue = dstRecord.get(column);
            if (srcValue == null && dstValue == null) {
                continue;
            }
            if (srcValue == null || dstValue == null) {
                return false;
            }
            if (!srcValue.equals(dstValue)) {
                return false;
            }
        }
        return true;
    }
}
