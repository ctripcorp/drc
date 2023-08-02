package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.aop.forward.PossibleRemote;
import com.ctrip.framework.drc.console.aop.forward.response.MhaV2ListResponse;
import com.ctrip.framework.drc.console.dao.DcTblDao;
import com.ctrip.framework.drc.console.dao.entity.DcTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.enums.ForwardTypeEnum;
import com.ctrip.framework.drc.console.service.v2.ForwardService;
import com.ctrip.framework.drc.console.vo.response.MhaListApiResult;
import java.sql.SQLException;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @ClassName ForwardServiceImpl
 * @Author haodongPan
 * @Date 2023/7/28 17:47
 * @Version: $
 * @Description: forward Service through PossibleRemote aop
 */
@Service
public class ForwardServiceImpl implements ForwardService {
    
    @Autowired private MhaTblV2Dao mhaTblV2Dao;
    @Autowired private DcTblDao dcTblDao;
    
    

    @Override
    @PossibleRemote(path = "/api/drc/v2/meta/mhaTblV2s",forwardType = ForwardTypeEnum.TO_META_DB,responseType = MhaV2ListResponse.class)
    public List<MhaTblV2> getMhaTblV2s(String dcName) throws SQLException {
        Long dcId = getDcId(dcName);
        MhaTblV2 sample = new MhaTblV2();
        sample.setDcId(dcId);
        return mhaTblV2Dao.queryBy(sample);
    }

    private Long getDcId(String dcName) throws SQLException {
        if (StringUtils.isBlank(dcName)) {
            return null;
        }
        DcTbl dcTbl = new DcTbl();
        dcTbl.setDcName(dcName);
        List<DcTbl> dcTbls =  dcTblDao.queryBy(dcTbl);
        if (dcTbls.isEmpty()) {
            throw new IllegalStateException("dc name does not exist in meta db, dc name is: " + dcName);
        }
        return dcTbls.get(0).getId();
    }
}
