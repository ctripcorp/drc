package com.ctrip.framework.drc.core.service.inquirer;

import com.ctrip.framework.drc.core.entity.Applier;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierInfoDto;
import com.ctrip.framework.drc.core.server.config.replicator.dto.ReplicatorInfoDto;
import com.ctrip.xpipe.tuple.Pair;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestOperations;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * @author yongnian
 * @create 2024/12/17 13:58
 */
public class BatchInfoInquirerTest {

    @Test
    public void testGetApplierInfo() {
        List<Applier> appliers = Lists.newArrayList(
                new Applier().setIp("127.0.0.1").setPort(8080)
        );
        ApplierInfoInquirer instance = ApplierInfoInquirer.getInstance();
        RestOperations mock = Mockito.mock(RestOperations.class);
        ApplierInfoDto dto = new ApplierInfoDto();
        dto.setIp("127.0.0.1");
        dto.setRegistryKey("test.test");
        dto.setPort(8080);
        dto.setReplicatorIp("127.1.0.1");
        ApiResult<List<ApplierInfoDto>> objectApiResult = ApiResult.getSuccessInstance(Lists.newArrayList(dto));
        ResponseEntity value = new ResponseEntity(objectApiResult, HttpStatus.ACCEPTED);
        when(mock.exchange(anyString(), any(), any(HttpEntity.class), any(Class.class))).thenReturn(value);
        instance.setRestTemplate(mock);

        Pair<List<String>, List<ApplierInfoDto>> applierInfo = BatchInfoInquirer.getInstance().getApplierInfo(appliers);
        System.out.println(applierInfo);
        Assert.assertEquals(1, applierInfo.getKey().size());
    }

    @Test
    public void testGetReplicatorInfo(){
        List<Applier> appliers = Lists.newArrayList(
                new Applier().setIp("127.0.0.1").setPort(8080)
        );
        ReplicatorInfoInquirer instance = ReplicatorInfoInquirer.getInstance();
        RestOperations mock = Mockito.mock(RestOperations.class);
        ReplicatorInfoDto dto = new ReplicatorInfoDto();
        dto.setIp("127.0.0.1");
        dto.setRegistryKey("test.test");
        dto.setPort(8080);
        dto.setUpstreamMasterIp("127.1.0.1");
        ApiResult<List<ReplicatorInfoDto>> objectApiResult = ApiResult.getSuccessInstance(Lists.newArrayList(dto));
        ResponseEntity value = new ResponseEntity(objectApiResult, HttpStatus.ACCEPTED);
        when(mock.exchange(anyString(),any(),any(HttpEntity.class),any(Class.class))).thenReturn(value);
        instance.setRestTemplate(mock);

        Pair<List<String>, List<ReplicatorInfoDto>> replicatorInfo = BatchInfoInquirer.getInstance().getReplicatorInfo(appliers);
        System.out.println(replicatorInfo);
        Assert.assertEquals(1,replicatorInfo.getKey().size());
    }
}

//Generated with love by TestMe :) Please raise issues & feature requests at: https://weirddev.com/forum#!/testme