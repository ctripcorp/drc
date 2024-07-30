package com.ctrip.framework.drc.monitor.function.cases.manager.suit;

import com.ctrip.framework.drc.core.server.config.applier.dto.ApplierConfigDto;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;

/**
 * @author: yongnian
 * @create: 2024/5/31 14:40
 */
public class MultiShardDbWriteOneComparePairCaseTest {
    @InjectMocks
    MultiShardDbWriteOneComparePairCase multiShardDbWriteOneComparePairCase;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testTest() throws Exception {
        String insertStatement = multiShardDbWriteOneComparePairCase.getInsertStatement(1, 1);
        String updateStatement = multiShardDbWriteOneComparePairCase.getUpdateStatement(1, 1);
        String deleteStatement = multiShardDbWriteOneComparePairCase.getDeleteStatement(1, 1);
        String selectStatement = multiShardDbWriteOneComparePairCase.getSelectStatement(1, 1);
        System.out.println(insertStatement);
        System.out.println(updateStatement);
        System.out.println(deleteStatement);
        System.out.println(selectStatement);
    }

    @Test
    public void testBody() {
        String applierRegistryKey = "zyn_test_2_dalcluster.zyn_test_2.zyn_test_1.drc_shard_1";
        ApplierConfigDto body = multiShardDbWriteOneComparePairCase.getBody(applierRegistryKey);
        String registryKey = body.getRegistryKey();
        Assert.assertEquals(applierRegistryKey, registryKey);
    }

}