package com.ctrip.framework.drc.manager.config;

import com.ctrip.framework.drc.core.config.RegionConfig;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;

/**
 * Created by mingdongli
 * 2019/12/3 下午5:44.
 */
@Component
public class DataCenterService implements InitializingBean {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private String REGION;

    private Map<String, Set<String>> regionIdcMapping = Maps.newConcurrentMap();

    @Override
    public void afterPropertiesSet() {
        REGION = RegionConfig.getInstance().getRegion();
        logger.info("[REGION] is set {}", REGION);

        regionIdcMapping = RegionConfig.getInstance().getRegion2dcsMapping();
    }

    public String getRegion() {
        return REGION;
    }

    public String getRegion(String dcName) {
        for (Map.Entry<String, Set<String>> entry : regionIdcMapping.entrySet()) {
            if (entry.getValue().contains(dcName.toLowerCase())) {
                return entry.getKey();
            }
        }
        throw new IllegalArgumentException("can not find region with dcName: " + dcName);
    }

    public Map<String, Set<String>> getRegionIdcMapping() {
        return regionIdcMapping;
    }
}
