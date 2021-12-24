package com.ctrip.framework.drc.console.dto;

import junit.framework.TestCase;
import org.junit.Test;

public class MhaInstanceGroupDtoTest extends TestCase {

    @Test
    public void testForMhaInstanceGroupDtoMySQLInstance() {
        MhaInstanceGroupDto mhaInstanceGroupDto = new MhaInstanceGroupDto();
        MhaInstanceGroupDto.MySQLInstance mySQLInstance = new MhaInstanceGroupDto.MySQLInstance();
        mySQLInstance.setServerType("SERVER_TYPE_1");
        String serverType = mySQLInstance.getServerType();
        assertEquals("SERVER_TYPE_1", serverType);
        MhaInstanceGroupDto.MySQLInstance sample = new MhaInstanceGroupDto.MySQLInstance();
        sample.setServerType("SERVER_TYPE_1");
        assertEquals(true, mySQLInstance.equals(sample));
    }


}