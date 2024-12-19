package com.ctrip.framework.drc.console.controller.v2;

import com.ctrip.framework.drc.console.param.v2.resource.ApplierMigrateParam;
import com.ctrip.framework.drc.console.service.v2.resource.ResourceService;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.*;

/**
 * Created by shiruixin
 * 2024/12/18 20:53
 */
@RunWith(MockitoJUnitRunner.class)
public class ResourceControllerTest {

    @InjectMocks
    ResourceController resourceController;

    @Mock
    ResourceService resourceService;

    @Test
    public void partialMigrateMessenger() throws Exception{
        Mockito.when(resourceService.partialMigrateMessenger(Mockito.any(ApplierMigrateParam.class))).thenReturn(1);
        ApiResult<Integer> res = resourceController.partialMigrateMessenger(new ApplierMigrateParam());
        Assert.assertEquals(1, res.getData().intValue());

    }
}