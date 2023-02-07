package com.ctrip.framework.drc.console.vo;

import com.ctrip.framework.drc.console.vo.display.SimplexDrcBuildVo;
import org.junit.Test;

public class SimplexDrcBuildVoTest {

    @Test
    public void testTestToString() {
        SimplexDrcBuildVo simplexDrcBuildVo = new SimplexDrcBuildVo(
                "srcMha",
                "destMha",
                "srcDc",
                "destDc",
                0L,
                0L,
                0L);
        
        System.out.println(simplexDrcBuildVo.toString());

    }
}