package com.ctrip.framework.drc.console.service;
import com.ctrip.framework.drc.console.vo.TableCheckVo;
import java.util.List;
import java.util.Map;

public interface LocalService {

    Map<String, Object> preCheckMySqlConfig(String mha);

    List<TableCheckVo> preCheckMySqlTables(String mha, String nameFilter);
    
}
