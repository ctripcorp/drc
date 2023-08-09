package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.core.entity.Messenger;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by dengquanliang
 * 2023/5/30 15:43
 */
public interface MessengerServiceV2 {

    List<MhaTblV2> getAllMessengerMhaTbls() ;
    List<Messenger> generateMessengers (Long mhaId) throws SQLException;
    
}
