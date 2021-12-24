package com.ctrip.framework.drc.console.utils;

import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.server.config.validation.dto.ValidationResultDto;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class HttpUtilLocalTest {
    private static final Logger logger = LoggerFactory.getLogger(HttpUtils.class);

    @Test
    public void testPut() {
        String uri = "http://drc.fat-1.qa.nt.ctripcorp.com/api/drc/v1/logs/unit";
        ValidationResultDto resultDto = new ValidationResultDto();
        resultDto.setMhaName("mhaName");
        resultDto.setGtid("gtid");
        resultDto.setSql("sql");
        resultDto.setExpectedDc("shaoy");
        resultDto.setActualDc("sharb");
        resultDto.setColumns(Arrays.asList("a", "b"));
        resultDto.setBeforeValues(Arrays.asList("a", "b"));
        resultDto.setAfterValues(Arrays.asList("a", "b"));
        resultDto.setUidName("uidname");
        resultDto.setUcsStrategyId(1);

        logger.info("report url: {}, dto: {}", uri, resultDto);
        ApiResult result = HttpUtils.put(uri, resultDto);
        Assert.assertNotNull(result);
        System.out.println(result.getStatus());
        Assert.assertTrue(result.getStatus() == 0 || result.getStatus() == 1);
    }
}
