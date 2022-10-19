package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.dao.MessengerGroupTblDao;
import com.ctrip.framework.drc.console.dao.MessengerTblDao;
import com.ctrip.framework.drc.console.dao.ResourceTblDao;
import com.ctrip.framework.drc.console.dao.entity.MessengerGroupTbl;
import com.ctrip.framework.drc.console.dao.entity.MessengerTbl;
import com.ctrip.framework.drc.console.dao.entity.ResourceTbl;
import com.ctrip.framework.drc.console.service.DataMediaPairService;
import com.ctrip.framework.drc.console.service.MessengerService;
import com.ctrip.framework.drc.core.entity.Messenger;
import com.ctrip.framework.drc.core.mq.MessengerProperties;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.List;

/**
 * @ClassName MessengerServiceImpl
 * @Author haodongPan
 * @Date 2022/10/8 16:56
 * @Version: $
 */
@Service
public class MessengerServiceImpl implements MessengerService {

    @Autowired
    private MessengerGroupTblDao messengerGroupTblDao;

    @Autowired
    private MessengerTblDao messengerTblDao;

    @Autowired
    private ResourceTblDao resourceTblDao;

    @Autowired
    private DataMediaPairService dataMediaPairService;


    @Override
    public List<Messenger> generateMessengers(Long mhaId) throws SQLException {
        List<Messenger> messengers = Lists.newArrayList();
        MessengerGroupTbl messengerGroupTbl = messengerGroupTblDao.queryByMhaId(mhaId);
        MessengerProperties messengerProperties = dataMediaPairService.generateMessengerProperties(messengerGroupTbl.getId());
        String propertiesJson = JsonUtils.toJson(messengerProperties);
        List<MessengerTbl> messengerTbls = messengerTblDao.queryByGroupId(messengerGroupTbl.getId());
        for (MessengerTbl messengerTbl : messengerTbls) {
            Messenger messenger = new Messenger();
            ResourceTbl resourceTbl = resourceTblDao.queryByPk(messengerTbl.getResourceId());
            messenger.setIp(resourceTbl.getIp());
            messenger.setPort(messengerTbl.getPort());
            messenger.setNameFilter(messengerProperties.getNameFilter());
            messenger.setGtidExecuted(messengerGroupTbl.getGtidExecuted());
            messenger.setProperties(propertiesJson);
            messengers.add(messenger);
        }
        return messengers;
    }
}
