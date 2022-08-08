package com.ctrip.framework.drc.manager.config;

import com.ctrip.xpipe.api.codec.GenericTypeReference;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.config.AbstractConfigBean;
import com.ctrip.xpipe.foundation.DefaultFoundationService;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;

/**
 * Created by mingdongli
 * 2019/12/3 下午5:44.
 */
@Component
public class DataCenterService  extends AbstractConfigBean implements InitializingBean {

    public static final String REGION_KEY = "region";

    public static String KEY_REGION_IDC_MAPPING = "drc.region.idc.mapping";

    private String LOCAL_DC;

    private String REGION;

    private Map<String, Set<String>> regionIdcMapping = Maps.newConcurrentMap();

    @Override
    public void afterPropertiesSet() {
        LOCAL_DC = FoundationService.DEFAULT.getDataCenter();
        System.setProperty(DefaultFoundationService.DATA_CENTER_KEY, LOCAL_DC);
        logger.info("[LOCAL_DC] is set {}", LOCAL_DC);

        REGION = getProperty(REGION_KEY);
        logger.info("[REGION] is set {}", REGION);

        regionIdcMapping = getRegionIdcMapping(KEY_REGION_IDC_MAPPING);
    }

    public String getDc() {
        return LOCAL_DC;
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

    private Map<String, Set<String>> getRegionIdcMapping(String key) {

        String regionIdcMappingStr = getProperty(key, "{}");
        Map<String, Set<String>> migrationIdcs = JsonCodec.INSTANCE.decode(regionIdcMappingStr, new GenericTypeReference<>() {
        });

        Map<String, Set<String>> result = Maps.newConcurrentMap();
        for(Map.Entry<String, Set<String>> entry : migrationIdcs.entrySet()){
            Set<String> dcNames = Sets.newHashSet();
            entry.getValue().forEach((dcName) -> dcNames.add(dcName.toLowerCase()));
            result.put(entry.getKey().toLowerCase(), dcNames);
        }

        logger.debug("[getRegionIdcMapping]{}", result);
        return result;
    }
}
