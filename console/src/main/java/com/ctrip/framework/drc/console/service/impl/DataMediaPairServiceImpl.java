package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.dao.DataMediaPairTblDao;
import com.ctrip.framework.drc.console.dao.entity.DataMediaPairTbl;
import com.ctrip.framework.drc.console.dto.MqConfigDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.ReplicationTypeEnum;
import com.ctrip.framework.drc.console.service.DataMediaPairService;
import com.ctrip.framework.drc.console.service.RowsFilterService;
import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.mq.MessengerProperties;
import com.ctrip.framework.drc.core.meta.MqConfig;
import com.ctrip.framework.drc.core.server.common.enums.ConsumeType;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.List;
import java.util.Set;

/**
 * @ClassName DataMediaPairServiceImpl
 * @Author haodongPan
 * @Date 2022/10/8 14:29
 * @Version: $
 */
@Service
public class DataMediaPairServiceImpl implements DataMediaPairService {

    @Autowired private DataMediaPairTblDao dataMediaPairTblDao;

    @Autowired private RowsFilterService rowsFilterService;
    
    
    @Override
    public MessengerProperties generateMessengerProperties(Long messengerGroupId) throws SQLException {
        List<MqConfig> mqConfigs = Lists.newArrayList();
        Set<String> tables = Sets.newHashSet();

        // mqConfigs
        List<DataMediaPairTbl> dataMediaPairTbls = dataMediaPairTblDao.queryByGroupId(messengerGroupId);
        for (DataMediaPairTbl dataMediaPairTbl : dataMediaPairTbls ) {
            tables.add(dataMediaPairTbl.getSrcDataMediaName());
            MqConfig mqConfig = JsonUtils.fromJson(dataMediaPairTbl.getProperties(), MqConfig.class);
            mqConfig.setTable(dataMediaPairTbl.getSrcDataMediaName());
            mqConfig.setTopic(dataMediaPairTbl.getDestDataMediaName());
            mqConfig.setProcessor(dataMediaPairTbl.getProcessor());
            mqConfigs.add(mqConfig);
        }
        MessengerProperties messengerProperties = new MessengerProperties();
        messengerProperties.setMqConfigs(mqConfigs);

        //nameFilter
        messengerProperties.setNameFilter(StringUtils.join(tables,","));

        //rowsFilters
        DataMediaConfig dataMediaConfig = new DataMediaConfig();
        dataMediaConfig.setRowsFilters(
                rowsFilterService.generateRowsFiltersConfig(messengerGroupId, ConsumeType.Applier.getCode())
        );
        messengerProperties.setDataMediaConfig(dataMediaConfig);

        return messengerProperties;
    }

    @Override
    public String addMqConfig(MqConfigDto dto) throws SQLException {
        DataMediaPairTbl dataMediaPairTbl = transfer(dto);
        int affectRows = dataMediaPairTblDao.insert(dataMediaPairTbl);
        return affectRows == 1 ?  "addMqConfig success" : "addMqConfig fail";
    }

    @Override
    public String updateMqConfig(MqConfigDto dto) throws SQLException {
        DataMediaPairTbl dataMediaPairTbl = transfer(dto);
        int affectRows = dataMediaPairTblDao.update(dataMediaPairTbl);
        return affectRows == 1 ?  "updateMqConfig success" : "updateMqConfig fail";
    }

    @Override
    public String deleteMqConfig(Long dataMediaPairId) throws SQLException {
        DataMediaPairTbl sample = new DataMediaPairTbl();
        sample.setId(dataMediaPairId);
        sample.setDeleted(BooleanEnum.TRUE.getCode());
        int affectRows = dataMediaPairTblDao.update(sample);
        return affectRows == 1 ?  "deleteMqConfig success" : "deleteMqConfig fail";
    }

    @Override
    public List<DataMediaPairTbl> getPairsByTopic(Long dataMediaPairId) throws SQLException {
        DataMediaPairTbl dataMediaPairTbl = dataMediaPairTblDao.queryByPk(dataMediaPairId);
        return getPairsByTopic(dataMediaPairTbl.getDestDataMediaName());
    }

    @Override
    public List<DataMediaPairTbl> getPairsByTopic(String topic) throws SQLException {
        DataMediaPairTbl sample = new DataMediaPairTbl();
        sample.setDestDataMediaName(topic);
        sample.setDeleted(BooleanEnum.FALSE.getCode());
        return dataMediaPairTblDao.queryBy(sample);
    }

    @Override
    public List<DataMediaPairTbl> getPairsByMGroupId(Long messengerGroupId) throws SQLException {
       return dataMediaPairTblDao.queryByGroupId(messengerGroupId);
    }
    
    private DataMediaPairTbl transfer(MqConfigDto dto) throws IllegalArgumentException {
        DataMediaPairTbl dataMediaPair = new DataMediaPairTbl();
        
        dataMediaPair.setId(dto.getId() == 0 ? null : dto.getId());
        dataMediaPair.setType(ReplicationTypeEnum.DB_TO_MQ.getType());
        dataMediaPair.setSrcDataMediaName(dto.getTable());
        dataMediaPair.setDestDataMediaName(dto.getTopic());
        dataMediaPair.setGroupId(dto.getMessengerGroupId());
        dataMediaPair.setProcessor(dto.getProcessor());
        dataMediaPair.setTag(StringUtils.isEmpty(dto.getTag()) ? null : dto.getTag());
        
        MqConfig mqConfig = new MqConfig();
        mqConfig.setMqType(dto.getMqType());
        mqConfig.setSerialization(dto.getSerialization());
        mqConfig.setOrder(dto.isOrder());
        mqConfig.setOrderKey(dto.getOrderKey());
        mqConfig.setPersistent(dto.isPersistent());
        mqConfig.setPersistentDb(dto.getPersistentDb());
        mqConfig.setDelayTime(dto.getDelayTime());
        String mqJsonString = JsonUtils.toJson(mqConfig);

        dataMediaPair.setProperties(mqJsonString);
        return dataMediaPair;
    }
}
