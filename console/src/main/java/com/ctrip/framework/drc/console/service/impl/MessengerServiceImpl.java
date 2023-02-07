package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.dao.MessengerGroupTblDao;
import com.ctrip.framework.drc.console.dao.MessengerTblDao;
import com.ctrip.framework.drc.console.dao.MhaTblDao;
import com.ctrip.framework.drc.console.dao.ResourceTblDao;
import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.dto.MhaDto;
import com.ctrip.framework.drc.console.dto.MqConfigDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.DataMediaTypeEnum;
import com.ctrip.framework.drc.console.service.DataMediaPairService;
import com.ctrip.framework.drc.console.service.DrcBuildService;
import com.ctrip.framework.drc.console.service.MessengerService;
import com.ctrip.framework.drc.console.service.MhaService;
import com.ctrip.framework.drc.console.utils.MySqlUtils.TableSchemaName;
import com.ctrip.framework.drc.console.vo.check.MqConfigConflictVo;
import com.ctrip.framework.drc.console.vo.display.MessengerVo;
import com.ctrip.framework.drc.console.vo.display.MqConfigVo;
import com.ctrip.framework.drc.console.vo.api.MessengerInfo;
import com.ctrip.framework.drc.console.vo.response.QmqApiResponse;
import com.ctrip.framework.drc.console.vo.response.QmqBuEntity;
import com.ctrip.framework.drc.console.vo.response.QmqBuList;
import com.ctrip.framework.drc.core.entity.Messenger;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.mq.MessengerProperties;
import com.ctrip.framework.drc.core.mq.MqType;
import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
import com.ctrip.xpipe.codec.JsonCodec;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.ArrayList;
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
    
    @Autowired private DrcBuildService drcBuildService;
    
    @Autowired private MhaService mhaService;
    
    @Autowired private DomainConfig domainConfig;
    
    @Autowired private DefaultConsoleConfig consoleConfig;

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
    
    
    @Override
    public List<MqConfigConflictVo> checkMqConfig(MqConfigDto dto) throws SQLException {
        // common table in diff topic should add tag
        // 1.根据messengerGroupId 查询该mha 的相关配置
        //            List<DataMediaPairTbl>
        //            List<srcDataMediaName>
        //            
        //            
        //         2.查询匹配到的全部表
        //            List<FullTableName>
        //         3.表是否有注册
        //            db 是否有匹配？
        //                有： table 是否有匹配？ 
        //                    有： 需要添加 tag
        //                    无： 无需tag,正常
        //                无： 无需tag,正常
        //         3. 支持tag,元数据需要 增加tag字段
        //            1.
        //            2.
        List<MqConfigConflictVo> res = Lists.newArrayList();
        List<DataMediaPairTbl> dataMediaPairs = dataMediaPairService.getDataMediaPairs(dto.getMessengerGroupId());
        
        
        String[] dbAndTable = dto.getTable().split("\\.");
        if (dbAndTable.length != 2) {
            throw new IllegalArgumentException("illegal logical table in checkMqConfig");
        }
        List<TableSchemaName> matchTables = drcBuildService.getMatchTable(
                dbAndTable[0],
                dbAndTable[1], dto.getMhaName(), 0);
        for (TableSchemaName table: dataMediaPairs.)
        // 1.dto的 table 查出匹配的表 
        // 2.每个表 和 已经配置的 logicalTables 匹配
        // 3.是否有匹配？ 
        //      有：需要添加 tag 
        //      无：不需tag
        
        return res;
        
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
        /*
         todo overwrite binlog-topic-registry.properties
         
        
         4. 写入 qconfig 配置文件
            1.跨region写qconfig 问题？ answer: 无需跨region， 写 subEnv 即可
            2.update 不变 会怎么响应？  
         5. 延迟监控 收到 DDL,新增表查看是否匹配到原有正则表达式，写入配置文件
            1.根据库表名，找到匹配的 messenger,再执行第4步，
            2.如果 console 和 replicator 延迟监控期间有中断
                中间的 DDL 将丢失，可用会造成漏更新？
         6.解决方案 
            1.qconfig 中保存正则表达
                bbz.fx.drc.qmq.test.dbName=dbadalclustertest0[1-6]db
                bbz.fx.drc.qmq.test.tableName=test_.*
                bbz.fx.drc.qmq.test.tag=bbzDrcTest
            2.推拉结合 ，dal没从qconfig 中找到时，调用DRC console api 刷新qconfig
         */
        
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

    private boolean initTopic(MqConfigDto dto) throws SQLException {
        String dcNameForMha = mhaService.getDcNameForMha(dto.getMhaName());
        if (consoleConfig.getLocalConfigCloudDc().contains(dcNameForMha)) {
            logger.info("[[tag=qmqInit]] localConfigCloudDc init qmq topic:{}",dto.getTopic());
            return true;
        }
        String topicApplicationUrl = domainConfig.getQmqTopicApplicationUrl(dcNameForMha);
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
    
    private boolean initProducer(MqConfigDto dto) throws SQLException {
        String dcNameForMha = mhaService.getDcNameForMha(dto.getMhaName());
        if (consoleConfig.getLocalConfigCloudDc().contains(dcNameForMha)) {
            logger.info("[[tag=qmqInit]] localConfigCloudDc init qmq topic:{}",dto.getTopic());
            return true;
        }
        String producerApplicationUrl = domainConfig.getQmqProducerApplicationUrl(dcNameForMha);
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

    @Override
    public List<MessengerInfo> getAllMessengersInfo() throws SQLException {
        List<MessengerInfo> res = Lists.newArrayList();
        
        MessengerGroupTbl sample = new MessengerGroupTbl();
        sample.setDeleted(BooleanEnum.FALSE.getCode());
        List<MessengerGroupTbl> mGroups = messengerGroupTblDao.queryBy(sample);
        for (MessengerGroupTbl mGroup : mGroups) {
            List<MessengerTbl> messengers = messengerTblDao.queryByGroupId(mGroup.getId());
            if (CollectionUtils.isEmpty(messengers)) {
                continue;
            }
            MessengerInfo mInfo = new MessengerInfo();
            MhaTbl mhaTbl = mhaTblDao.queryByPk(mGroup.getMhaId());
            mInfo.setMhaName(mhaTbl.getMhaName());
            List<DataMediaPairTbl> dataMediaPairs = dataMediaPairService.getDataMediaPairs(mGroup.getId());
            if (CollectionUtils.isEmpty(dataMediaPairs)) {
                continue;
            } else {
                List<String> tables = dataMediaPairs.stream().map(DataMediaPairTbl::getSrcDataMediaName)
                        .collect(Collectors.toList());
                mInfo.setNameFilter(StringUtils.join(tables,","));
            }
            res.add(mInfo);
        }
        return res;
    }


}
