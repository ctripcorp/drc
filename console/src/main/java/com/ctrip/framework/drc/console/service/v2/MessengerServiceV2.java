package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dto.v2.MhaDelayInfoDto;
import com.ctrip.framework.drc.console.dto.v2.MhaMessengerDto;
import com.ctrip.framework.drc.console.dto.v2.MqConfigDto;
import com.ctrip.framework.drc.console.vo.check.v2.MqConfigCheckVo;
import com.ctrip.framework.drc.console.vo.display.v2.MqConfigVo;
import com.ctrip.framework.drc.console.vo.request.MessengerQueryDto;
import com.ctrip.framework.drc.console.vo.request.MqConfigDeleteRequestDto;
import com.ctrip.framework.drc.core.entity.Messenger;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by dengquanliang
 * 2023/5/30 15:43
 */
public interface MessengerServiceV2 {

    List<MhaTblV2> getAllMessengerMhaTbls();
    List<MhaTblV2> getMessengerMhaTbls(MessengerQueryDto queryDto);

    void deleteDbReplicationForMq(String mhaName, List<Long> dbReplicationIds) throws Exception;

    List<String> getBusFromQmq() throws Exception;

    List<Messenger> generateMessengers(Long mhaId) throws SQLException;

    List<Messenger> generateDbMessengers(Long mhaId) throws SQLException;

    void removeMessengerGroup(String mhaName) throws Exception;

    List<MqConfigVo> queryMhaMessengerConfigs(String mhaName);

    MqConfigCheckVo checkMqConfig(MqConfigDto dto);

    void processAddMqConfig(MqConfigDto dto) throws Exception;

    void processUpdateMqConfig(MqConfigDto dto) throws Exception;

    void processDeleteMqConfig(MqConfigDeleteRequestDto requestDto) throws Exception;

    String getMessengerGtidExecuted(String mhaName);

    List<MhaMessengerDto> getRelatedMhaMessenger(List<String> mhas, List<String> dbs);

    List<MhaDelayInfoDto> getMhaMessengerDelays(List<MhaMessengerDto> messengerDtoList);

    Map<String, Set<String>> registerMessengerAppAsQMQProducer(boolean showOnly, boolean changeAll, String topic, String registDc) throws SQLException;
}
