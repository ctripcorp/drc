package com.ctrip.framework.drc.console.service;

import com.ctrip.framework.drc.console.dao.entity.MhaTbl;
import com.ctrip.framework.drc.console.dto.MhaInstanceGroupDto;


public interface DrcMaintenanceService {
    void mhaInstancesChange(MhaInstanceGroupDto mhaInstanceGroupDto, MhaTbl mhaTbl) throws Exception;
}
