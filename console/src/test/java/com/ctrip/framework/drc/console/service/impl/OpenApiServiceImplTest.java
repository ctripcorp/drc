package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.vo.MhaDbFiltersVo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.util.List;


public class OpenApiServiceImplTest {
    
    @Spy
    private MetaGenerator metaGenerator;
    
    @InjectMocks
    private OpenApiServiceImpl openApiService;

    

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }
    
    @Test
    public void testGetAllDrcMhaDbFilters() {
        List<MhaDbFiltersVo> allDrcMhaDbFilters = openApiService.getAllDrcMhaDbFilters();
        Assert.assertNull(allDrcMhaDbFilters);
        Assert.assertNotEquals(0,allDrcMhaDbFilters.size());
    }

}