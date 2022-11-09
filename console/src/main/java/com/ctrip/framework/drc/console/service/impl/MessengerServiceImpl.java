package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.dao.MessengerGroupTblDao;
import com.ctrip.framework.drc.console.dao.MessengerTblDao;
import com.ctrip.framework.drc.console.dao.MhaTblDao;
import com.ctrip.framework.drc.console.dao.ResourceTblDao;
import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.dto.MhaDto;
import com.ctrip.framework.drc.console.dto.MqConfigDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.service.DataMediaPairService;
import com.ctrip.framework.drc.console.service.MessengerService;
import com.ctrip.framework.drc.console.service.MhaService;
import com.ctrip.framework.drc.console.vo.MessengerVo;
import com.ctrip.framework.drc.console.vo.MqConfigVo;
import com.ctrip.framework.drc.console.vo.response.QmqApiResponse;
import com.ctrip.framework.drc.console.vo.response.QmqBuEntity;
import com.ctrip.framework.drc.console.vo.response.QmqBuList;
import com.ctrip.framework.drc.core.entity.Messenger;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.mq.MessengerProperties;
import com.ctrip.framework.drc.core.mq.MqType;
import com.ctrip.xpipe.codec.JsonCodec;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @ClassName MessengerServiceImpl
 * @Author haodongPan
 * @Date 2022/10/8 16:56
 * @Version: $
 */
@Service
public class MessengerServiceImpl implements MessengerService {
    
    private static final Logger logger = LoggerFactory.getLogger(MessengerServiceImpl.class);

    @Autowired private DataMediaPairService dataMediaPairService;
    
    @Autowired private MhaService mhaService;
    
    @Autowired private DomainConfig domainConfig;

    @Autowired private MessengerGroupTblDao messengerGroupTblDao;

    @Autowired private MessengerTblDao messengerTblDao;

    @Autowired private ResourceTblDao resourceTblDao;
    
    @Autowired private MhaTblDao mhaTblDao;
    

    
    @Override
    public List<Messenger> generateMessengers(Long mhaId) throws SQLException {
        List<Messenger> messengers = Lists.newArrayList();
        MessengerGroupTbl messengerGroupTbl = messengerGroupTblDao.queryByMhaId(mhaId, BooleanEnum.FALSE.getCode());
        if (messengerGroupTbl == null) {
            return messengers;
        }
        
        MessengerProperties messengerProperties = dataMediaPairService.generateMessengerProperties(messengerGroupTbl.getId());
        if (CollectionUtils.isEmpty(messengerProperties.getMqConfigs())) {
            logger.info("no mqConfig, should not generate messenger");
            return messengers;
        }
        String propertiesJson = JsonCodec.INSTANCE.encode(messengerProperties);
        
        List<MessengerTbl> messengerTbls = messengerTblDao.queryByGroupId(messengerGroupTbl.getId());
        for (MessengerTbl messengerTbl : messengerTbls) {
            Messenger messenger = new Messenger();
            ResourceTbl resourceTbl = resourceTblDao.queryByPk(messengerTbl.getResourceId());
            messenger.setIp(resourceTbl.getIp());
            messenger.setPort(messengerTbl.getPort());
            messenger.setNameFilter(messengerProperties.getNameFilter()); // for compatible
            messenger.setGtidExecuted(messengerGroupTbl.getGtidExecuted());
            messenger.setProperties(propertiesJson);
            messengers.add(messenger);
        }
        return messengers;
    }

    @Override
    public List<String> getMessengerIps(Long mhaId) throws SQLException {
        List<String> res = Lists.newArrayList();
        MessengerGroupTbl messengerGroupTbl =
            messengerGroupTblDao.queryByMhaId(mhaId, BooleanEnum.FALSE.getCode());
        List<MessengerTbl> messengerTbls = messengerTblDao.queryByGroupId(messengerGroupTbl.getId());
        for (MessengerTbl messengerTbl : messengerTbls) {
            ResourceTbl resourceTbl = resourceTblDao.queryByPk(messengerTbl.getResourceId());
            res.add(resourceTbl.getIp());
        }
        return res;
    }

    @Override
    public List<MqConfigVo> getMqConfigVos(Long messengerGroupId) throws SQLException {
        List<MqConfigVo> vos = Lists.newArrayList();
        List<DataMediaPairTbl> dataMediaPairs = dataMediaPairService.getDataMediaPairs(messengerGroupId);
        for (DataMediaPairTbl dataMediaPair : dataMediaPairs) {
            vos.add(MqConfigVo.from(dataMediaPair));
        }
        return vos;
    }

    public List<String> getBusFromQmq() throws Exception {
        List<QmqBuEntity> buEntities = getBuEntitiesFromQmq();
        return buEntities.stream().map(buEntity -> buEntity.getEnName().toLowerCase()).collect(Collectors.toList());
    }
    
    private List<QmqBuEntity> getBuEntitiesFromQmq() throws Exception {
        String qmqBuListUrl = domainConfig.getQmqBuListUrl();
        QmqBuList response = HttpUtils.post(qmqBuListUrl, null, QmqBuList.class);
        return response.getData();
    }
    

    @Override
    public String processAddMqConfig(MqConfigDto dto) throws Exception {
        if (MqType.qmq.name().equalsIgnoreCase(dto.getMqType())) {
            if (!initTopic(dto)) {
                throw new IllegalArgumentException("init Topic error");
            }
            if (!initProducer(dto)) {
                throw new IllegalArgumentException("init producer error");
            }
        } else {
            // kafka todo
        }
        return dataMediaPairService.addMqConfig(dto);
    }

    @Override
    public String processUpdateMqConfig(MqConfigDto dto) throws Exception {
        if (MqType.qmq.name().equalsIgnoreCase(dto.getMqType())) {
            if (!initTopic(dto)) {
                throw new IllegalArgumentException("init Topic error");
            }
            if (!initProducer(dto)) {
                throw new IllegalArgumentException("init producer error");
            }
        } else {
            // kafka todo
        }
        return dataMediaPairService.updateMqConfig(dto);
    }

    @Override
    public String processDeleteMqConfig(Long mqConfigId) throws Exception {
        return dataMediaPairService.deleteMqConfig(mqConfigId);
    }

    @Override
    public List<MessengerVo> getAllMessengerVos() throws SQLException {
        List<MessengerVo> result = Lists.newArrayList();
        MessengerGroupTbl sample = new MessengerGroupTbl();
        sample.setDeleted(BooleanEnum.FALSE.getCode());
        List<MessengerGroupTbl> messengers = messengerGroupTblDao.queryBy(sample);
        for (MessengerGroupTbl messenger : messengers) {
            MhaDto mhaDto = mhaService.queryMhaInfo(messenger.getMhaId());
            MessengerVo messengerVo = new MessengerVo();
            messengerVo.setMhaName(mhaDto.getMhaName());
            messengerVo.setBu(mhaDto.getBuName());
            messengerVo.setMonitorSwitch(mhaDto.getMonitorSwitch());
            result.add(messengerVo);
        }
        return  result;
    }

    @Override
    public String removeMessengerGroup(String mhaName) throws SQLException {
        MhaTbl mhaTbl = mhaTblDao.queryByMhaName(mhaName, BooleanEnum.FALSE.getCode());
        MessengerGroupTbl mGroup = messengerGroupTblDao.queryByMhaId(mhaTbl.getId(), BooleanEnum.FALSE.getCode());
        mGroup.setDeleted(BooleanEnum.TRUE.getCode());
        List<MessengerTbl> messengers = messengerTblDao.queryByGroupId(mGroup.getId());
        for (MessengerTbl m : messengers) {
            m.setDeleted(BooleanEnum.TRUE.getCode());
        }
        messengerGroupTblDao.update(mGroup);
        messengerTblDao.batchUpdate(messengers);
        return "remove success";
    }

    private boolean initTopic(MqConfigDto dto) throws Exception {
        String topicApplicationUrl = domainConfig.getQmqTopicApplicationUrl();
        LinkedHashMap<String, String> requestBody = Maps.newLinkedHashMap();
        requestBody.put("subject",dto.getTopic());
        requestBody.put("cluster",dto.isOrder() ? "ordered" : "default");
        requestBody.put("bu",dto.getBu());
        requestBody.put("creator","drc");
        requestBody.put("emailGroup","rdkjdrc@Ctrip.com");
        QmqApiResponse response = HttpUtils.post(topicApplicationUrl, requestBody, QmqApiResponse.class);
        
        if (response.getStatus() == 0) {
            logger.info("[[tag=qmqInit]] init qmq topic success,topic:{}",dto.getTopic());
        } else if (StringUtils.isNotBlank(response.getStatusMsg())
                && response.getStatusMsg().contains("already existed")) {
            logger.info("[[tag=qmqInit]] init qmq topic success,topic:{} already existed",dto.getTopic());
        } else {
            logger.error("[[tag=qmqInit]] init qmq topic fail,MqConfigDto:{}",dto);
            return false;
        }
        return true;
    }
    
    private boolean initProducer(MqConfigDto dto) throws Exception {
        String producerApplicationUrl = domainConfig.getQmqProducerApplicationUrl();
        LinkedHashMap<String, Object> requestBody = Maps.newLinkedHashMap();
        requestBody.put("appCode","100023500");
        requestBody.put("subject",dto.getTopic());
        requestBody.put("durable",false);
        requestBody.put("tableStrategy",0);
        requestBody.put("qpsAvg",1000);
        requestBody.put("qpsMax",5000);
        requestBody.put("msgLength",1000);
        requestBody.put("platform",1);
        requestBody.put("creator","drc");
        requestBody.put("remark","binlog_dataChange_message");
        
        QmqApiResponse response = HttpUtils.post(producerApplicationUrl, requestBody, QmqApiResponse.class);
        if (response.getStatus() == 0) {
            logger.info("[[tag=qmqInit]] init qmq producer success,topic:{}",dto.getTopic());
        } else if (StringUtils.isNotBlank(response.getStatusMsg())
                && response.getStatusMsg().contains("already existed")) {
            logger.info("[[tag=qmqInit]] init success,qmq producer already existed,topic:{}",dto.getTopic());
        } else {
            logger.error("[[tag=qmqInit]] init qmq producer fail,MqConfigDto:{}",dto);
            return false;
        }
        return true;
    }
    
}
