package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dto.v2.MhaDelayInfoDto;
import com.ctrip.framework.drc.console.dto.v2.MhaMessengerDto;
import com.ctrip.framework.drc.console.dto.v2.MqConfigDto;
import com.ctrip.framework.drc.console.vo.check.v2.MqConfigCheckVo;
import com.ctrip.framework.drc.console.vo.display.MessengerVo;
import com.ctrip.framework.drc.console.vo.display.v2.MqConfigVo;
import com.ctrip.framework.drc.console.vo.request.MessengerQueryDto;
import com.ctrip.framework.drc.console.vo.request.MqConfigDeleteRequestDto;
import com.ctrip.framework.drc.core.entity.Messenger;
import com.ctrip.framework.drc.core.mq.MqType;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

/**
 * Created by dengquanliang
 * 2023/5/30 15:43
 */
public interface MessengerServiceV2 {

    List<MhaDelayInfoDto> getMhaMessengerDelaysWithoutSrcTime(List<MhaMessengerDto> messengerDtoList, MqType mqType);

    List<MessengerVo> getMessengerMhaTbls(MessengerQueryDto queryDto);

    void deleteDbReplicationForMq(String mhaName, MqType mqType, List<Long> dbReplicationIds) throws Exception;

    List<String> getBusFromQmq() throws Exception;

    List<Messenger> generateMessengers(Long mhaId, MqType mqType) throws SQLException;

    List<Messenger> generateDbMessengers(Long mhaId, MqType mqType) throws SQLException;

    void removeMessengerGroup(String mhaName, MqType parse) throws Exception;

    List<MqConfigVo> queryMhaMessengerConfigs(String mhaName, MqType mqType);

    MqConfigCheckVo checkMqConfig(MqConfigDto dto);

    void processAddMqConfig(MqConfigDto dto) throws Exception;

    void processUpdateMqConfig(MqConfigDto dto) throws Exception;

    void processDeleteMqConfig(MqConfigDeleteRequestDto requestDto) throws Exception;

    String getMessengerGtidExecuted(String mhaName, MqType mqType);

    List<MhaMessengerDto> getRelatedMhaMessenger(List<String> mhas, List<String> dbs);

    List<MhaMessengerDto> getRelatedMhaMessenger(List<String> mhas, List<String> dbs, MqType mqType);

    List<MhaDelayInfoDto> getMhaMessengerDelays(List<MhaMessengerDto> messengerDtoList, MqType mqType);

    List<MhaDelayInfoDto> getMhaMessengerDelays(List<MhaMessengerDto> messengerDtoList);

    void initMqConfigIfNeeded(MqConfigDto dto, Collection<String> dcNames) ;

}
