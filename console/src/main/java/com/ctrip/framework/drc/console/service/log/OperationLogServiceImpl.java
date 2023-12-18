package com.ctrip.framework.drc.console.service.log;

import com.ctrip.framework.drc.console.dao.log.OperationLogTblDao;
import com.ctrip.framework.drc.console.dao.log.entity.OperationLogTbl;
import com.ctrip.framework.drc.console.param.log.OperationLogQueryParam;
import com.ctrip.framework.drc.console.vo.log.OperationLogView;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

/**
 * @ClassName OperationLogServiceImpl
 * @Author haodongPan
 * @Date 2023/12/8 16:03
 * @Version: $
 */
@Service
public class OperationLogServiceImpl implements OperationLogService {

    @Autowired
    private OperationLogTblDao operationLogTblDao;
    
    @Override
    public List<OperationLogView> getOperationLogView(OperationLogQueryParam param) throws SQLException {
        List<OperationLogTbl> operationLogTbls =  operationLogTblDao.queryByParam(param);
        if (CollectionUtils.isEmpty(operationLogTbls)) {
            return new ArrayList<>();
        }

        List<OperationLogView> views = operationLogTbls.stream().map(source -> {
            OperationLogView target = new OperationLogView();
            BeanUtils.copyProperties(source, target, "createTime");
            target.setCreateTime(source.getCreateTime().toString());
            return target;
        }).collect(Collectors.toList());
        return views;
    }

    @Override
    public Long gerOperationLogCount(OperationLogQueryParam param) throws SQLException {
        return operationLogTblDao.countBy(param);
    }
    
}
