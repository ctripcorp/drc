package com.ctrip.framework.drc.console.service;

import com.ctrip.framework.drc.console.dao.entity.DataMediaPairTbl;
import com.ctrip.framework.drc.console.dto.MqConfigDto;
import com.ctrip.framework.drc.core.mq.MessengerProperties;

import java.sql.SQLException;
import java.util.List;

public interface DataMediaPairService {

    // db -> mq
    MessengerProperties generateMessengerProperties(Long messengerGroupId) throws SQLException;
    
    String addMqConfig(MqConfigDto dto) throws SQLException;

    String updateMqConfig(MqConfigDto dto) throws SQLException;

    String deleteMqConfig(Long dataMediaPairId) throws SQLException;
    
    List<DataMediaPairTbl> getDataMediaPairs(Long messengerGroupId) throws SQLException;
}
