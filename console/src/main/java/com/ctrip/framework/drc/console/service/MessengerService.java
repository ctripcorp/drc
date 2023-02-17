package com.ctrip.framework.drc.console.service;

import com.ctrip.framework.drc.console.aop.PossibleRemote;
import com.ctrip.framework.drc.console.dto.MqConfigDto;
import com.ctrip.framework.drc.console.enums.ForwardTypeEnum;
import com.ctrip.framework.drc.console.vo.check.MqConfigCheckVo;
import com.ctrip.framework.drc.console.vo.check.MqConfigConflictTable;
import com.ctrip.framework.drc.console.vo.display.MessengerVo;
import com.ctrip.framework.drc.console.vo.display.MqConfigVo;
import com.ctrip.framework.drc.console.vo.api.MessengerInfo;
import com.ctrip.framework.drc.core.entity.Messenger;

import java.sql.SQLException;
import java.util.List;

public interface MessengerService {
    
    // meta
    List<Messenger> generateMessengers (Long mhaId) throws SQLException;

    
    // messenger
    List<MessengerVo> getAllMessengerVos() throws SQLException;

    List<String> getMessengerIps (Long mhaId) throws SQLException;
    
    String removeMessengerGroup(String mhaName) throws SQLException;
    
    
    // mqConfig
    List<MqConfigVo> getMqConfigVos(Long messengerGroupId) throws SQLException;
    
    String processAddMqConfig(MqConfigDto dto) throws SQLException;

    String processUpdateMqConfig(MqConfigDto dto) throws Exception;
    
    String processDeleteMqConfig(String dc, Long mqConfigId) throws Exception;

    MqConfigCheckVo checkMqConfig(MqConfigDto dto) throws SQLException;

    List<MqConfigConflictTable> checkMqConfig(Long messengerGroupId, Long mqConfigId, String mhaName,
            String namespace, String name, String tag) throws SQLException;

    List<String> getBusFromQmq() throws Exception;
    
    // openApi
    List<MessengerInfo> getAllMessengersInfo() throws SQLException;

    
    void addDalClusterMqConfigByDDL(String dc, String mhaName, String schema, String table) throws SQLException;
}
