package com.ctrip.framework.drc.console.param.v2;

import com.ctrip.framework.drc.console.enums.ReplicationTypeEnum;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.*;

/**
 * @author: yongnian
 * @create: 2024/7/3 17:00
 */
public class DrcAutoBuildReqTest {
    @Test
    public void testReq() {
        DrcAutoBuildReq drcAutoBuildReqForSingleDb = getDrcAutoBuildReqForSingleDb();
        Assert.assertEquals(ReplicationTypeEnum.DB_TO_DB, drcAutoBuildReqForSingleDb.getReplicationType());
        drcAutoBuildReqForSingleDb.validAndTrim();

        drcAutoBuildReqForSingleDb.setReplicationType(ReplicationTypeEnum.DB_TO_MQ.getType());
        drcAutoBuildReqForSingleDb.setDstRegionName(null);
        drcAutoBuildReqForSingleDb.validAndTrim();

    }

    @Test(expected = IllegalArgumentException.class)
    public void testReqDbToDbNoDstRegion() {
        DrcAutoBuildReq drcAutoBuildReqForSingleDb = getDrcAutoBuildReqForSingleDb();
        Assert.assertEquals(ReplicationTypeEnum.DB_TO_DB, drcAutoBuildReqForSingleDb.getReplicationType());
        drcAutoBuildReqForSingleDb.setDstRegionName(null);
        drcAutoBuildReqForSingleDb.validAndTrim();
    }

    private static DrcAutoBuildReq getDrcAutoBuildReqForSingleDb() {
        DrcAutoBuildReq req = new DrcAutoBuildReq();
        req.setMode(DrcAutoBuildReq.BuildMode.SINGLE_DB_NAME.getValue());
        req.setDbName("testdb");
        req.setSrcRegionName("ntgxh");
        req.setDstRegionName("sin");
        DrcAutoBuildReq.TblsFilterDetail tblsFilterDetail = new DrcAutoBuildReq.TblsFilterDetail();
        tblsFilterDetail.setTableNames("testTable1");
        req.setTblsFilterDetail(tblsFilterDetail);
        req.setBuName("bu");
        req.autoSetTag();

        return req;
    }
}

//Generated with love by TestMe :) Please raise issues & feature requests at: https://weirddev.com/forum#!/testme