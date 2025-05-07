package com.ctrip.framework.drc.console.vo.display.v2;

import com.ctrip.framework.drc.console.dto.v2.MhaDto;
import com.ctrip.framework.drc.core.meta.ReplicationTypeEnum;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author: yongnian
 * @create: 2024/7/3 17:13
 */
public class MhaReplicationPreviewDtoTest {
    @Test
    public void test() {
        MhaReplicationPreviewDto mhaReplicationPreviewDto = new MhaReplicationPreviewDto();
        mhaReplicationPreviewDto.setDstOptionalMha(Lists.newArrayList(getMhaDto("mha1")));
        mhaReplicationPreviewDto.setSrcOptionalMha(Lists.newArrayList(getMhaDto("mha2")));
        Assert.assertTrue(mhaReplicationPreviewDto.normalCase());
        mhaReplicationPreviewDto.setReplicationType(ReplicationTypeEnum.DB_TO_DB.getType());
        Assert.assertTrue(mhaReplicationPreviewDto.normalCase());
        mhaReplicationPreviewDto.setReplicationType(ReplicationTypeEnum.DB_TO_MQ.getType());
        Assert.assertFalse(mhaReplicationPreviewDto.normalCase());

        mhaReplicationPreviewDto.setDstOptionalMha(null);
        Assert.assertTrue(mhaReplicationPreviewDto.normalCase());

    }

    private static MhaDto getMhaDto(String mha1) {
        MhaDto mhaDto = new MhaDto();
        mhaDto.setName(mha1);
        return mhaDto;
    }
}
