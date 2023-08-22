package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dto.v2.MqConfigDto;
import com.ctrip.framework.drc.console.vo.check.v2.MqConfigCheckVo;
import com.ctrip.framework.drc.console.vo.display.v2.MqConfigVo;
import com.ctrip.framework.drc.console.vo.request.MqConfigDeleteRequestDto;
import com.ctrip.framework.drc.core.entity.Messenger;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by dengquanliang
 * 2023/5/30 15:43
 */
public interface MessengerServiceV2 {

    List<MhaTblV2> getAllMessengerMhaTbls();

    void deleteDbReplicationForMq(String mhaName, List<Long> dbReplicationIds) throws Exception;

    List<String> getBusFromQmq() throws Exception;

    List<Messenger> generateMessengers(Long mhaId) throws SQLException;

    void removeMessengerGroup(String mhaName) throws Exception;

    List<MqConfigVo> queryMhaMessengerConfigs(String mhaName);

    MqConfigCheckVo checkMqConfig(MqConfigDto dto);

    void processAddMqConfig(MqConfigDto dto) throws Exception;

    void processUpdateMqConfig(MqConfigDto dto) throws Exception;

    void processDeleteMqConfig(MqConfigDeleteRequestDto requestDto) throws Exception;

    String getMessengerGtidExecuted(String mhaName);
}
