package com.ctrip.framework.drc.core.service.statistics.traffic;

import com.ctrip.framework.drc.core.mq.MqType;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class HickWallMessengerDelayEntity {

    Metric metric;

    List<List<Object>> values;


    public static class Metric {
        String mhaName;
        String mqType;

        public String getMhaName() {
            return mhaName;
        }

        public void setMhaName(String mhaName) {
            this.mhaName = mhaName;
        }

        public String getMqType() {
            return mqType == null ? MqType.DEFAULT.name() : mqType;
        }

        public void setMqType(String mqType) {
            this.mqType = mqType;
        }

        @Override
        public String toString() {
            return "Metric{" +
                    "mhaName='" + mhaName + '\'' +
                    ", mqType='" + mqType + '\'' +
                    '}';
        }
    }

    // mha -> entity
    public static Map<String, HickWallMessengerDelayEntity> parseJson(String json, MqType mqType) {
        List<HickWallMessengerDelayEntity> entities = JsonUtils.fromJsonToList(json, HickWallMessengerDelayEntity.class);
        return entities.stream().filter(e -> mqType.name().equals(e.metric.getMqType()))
                .collect(Collectors.toMap(
                                HickWallMessengerDelayEntity::getMha,
                                Function.identity(),
                                (e1, e2) -> e1.getDelay() <= e2.getDelay() ? e1 : e2
                        )
                );
    }

    public String getMha() {
        if (metric == null) {
            return null;
        }
        return metric.mhaName;
    }

    public Long getDelay() {
        String delay = (String) values.get(values.size() - 1).get(1);
        BigDecimal bd = new BigDecimal(delay);
        return bd.setScale(0, RoundingMode.UP).longValue();
    }
}
