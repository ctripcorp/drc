package com.ctrip.framework.drc.console.dto.v3;

import com.ctrip.framework.drc.console.enums.ReplicationTypeEnum;
import org.junit.Test;

/**
 * @author: yongnian
 * @create: 2024/7/3 16:53
 */
public class MhaDbReplicationCreateDtoTest {


    @Test
    public void testOK() {
        // for db - db
        MhaDbReplicationCreateDto mhaDbReplicationCreateDto = new MhaDbReplicationCreateDto();
        mhaDbReplicationCreateDto.setSrcRegionName("src region");
        mhaDbReplicationCreateDto.setDstRegionName("dst region");
        mhaDbReplicationCreateDto.setDbName("db1");
        mhaDbReplicationCreateDto.setBuName("BBZ");
        mhaDbReplicationCreateDto.setTag("COMMON");

        mhaDbReplicationCreateDto.validAndTrimForCreateReq();

        // for db - mq
        MhaDbReplicationCreateDto mhaDbReplicationCreateDto2 = new MhaDbReplicationCreateDto();
        mhaDbReplicationCreateDto2.setReplicationType(ReplicationTypeEnum.DB_TO_MQ.getType());
        mhaDbReplicationCreateDto2.setSrcRegionName("src region");
        mhaDbReplicationCreateDto2.setDbName("db1");
        mhaDbReplicationCreateDto2.setBuName("BBZ");
        mhaDbReplicationCreateDto2.setTag("COMMON");

        mhaDbReplicationCreateDto2.validAndTrimForCreateReq();
    }


    @Test(expected = IllegalArgumentException.class)
    public void testDBDBNoDst() {
        // for db - db
        MhaDbReplicationCreateDto mhaDbReplicationCreateDto = new MhaDbReplicationCreateDto();
        mhaDbReplicationCreateDto.setSrcRegionName("src region");
        mhaDbReplicationCreateDto.setDbName("db1");
        mhaDbReplicationCreateDto.setBuName("BBZ");
        mhaDbReplicationCreateDto.setTag("COMMON");

        mhaDbReplicationCreateDto.validAndTrimForCreateReq();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDBMqNoSrc() {
        // for db - db
        MhaDbReplicationCreateDto mhaDbReplicationCreateDto = new MhaDbReplicationCreateDto();
        mhaDbReplicationCreateDto.setDstRegionName("dst region");
        mhaDbReplicationCreateDto.setDbName("db1");
        mhaDbReplicationCreateDto.setBuName("BBZ");
        mhaDbReplicationCreateDto.setTag("COMMON");

        mhaDbReplicationCreateDto.validAndTrimForCreateReq();
    }
}

//Generated with love by TestMe :) Please raise issues & feature requests at: https://weirddev.com/forum#!/testme