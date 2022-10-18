package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.dao.DataMediaPairTblDao;
import com.ctrip.framework.drc.console.dao.entity.DataMediaPairTbl;
import com.ctrip.framework.drc.console.enums.ApplierTypeEnum;
import com.ctrip.framework.drc.console.service.DataMediaPairService;
import com.ctrip.framework.drc.console.service.RowsFilterService;
import com.ctrip.framework.drc.core.meta.DataMediaConfig;
import com.ctrip.framework.drc.core.mq.MessengerProperties;
import com.ctrip.framework.drc.core.meta.MqConfig;
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

    @Autowired
    private DataMediaPairTblDao dataMediaPairTblDao;

    @Autowired
    private RowsFilterService rowsFilterService;


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
                rowsFilterService.generateRowsFiltersConfig(messengerGroupId, ApplierTypeEnum.MESSENGER.getCode())
        );
        messengerProperties.setDataMediaConfig(dataMediaConfig);

        return messengerProperties;
    }
}
