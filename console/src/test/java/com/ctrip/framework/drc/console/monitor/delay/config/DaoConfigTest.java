package com.ctrip.framework.drc.console.monitor.delay.config;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.service.impl.MetaGenerator;
import com.ctrip.framework.drc.console.utils.XmlUtil;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.transform.DefaultSaxParser;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.HashSet;

import static com.ctrip.framework.drc.console.AllTests.DRC_XML2;

public class DaoConfigTest extends AbstractConfigTest {
    @InjectMocks
    private DaoConfig daoConfig = new DaoConfig();

    @Mock
    private DataCenterService dataCenterService;

    @Mock
    private DefaultConsoleConfig consoleConfig;

    @Mock
    private MetaGenerator metaGenerator = new MetaGenerator();

    private static final String IDC = "shaxx";

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        Mockito.doReturn(IDC).when(dataCenterService).getDc();
        Mockito.doReturn(new HashSet<>()).when(consoleConfig).getPublicCloudDc();
    }

    @Test
    public void testUpdateConfig() throws Exception {
        daoConfig.updateConfig();
        Assert.assertNull(daoConfig.xml);

        Mockito.doReturn(DefaultSaxParser.parse(DRC_XML2)).when(metaGenerator).getDrc();
        daoConfig.updateConfig();
        Drc expectedDrc = DefaultSaxParser.parse(DRC_XML2);
        Drc actualDrc = DefaultSaxParser.parse(daoConfig.xml);
        Assert.assertEquals(XmlUtil.extractDrc(expectedDrc).toString(), XmlUtil.extractDrc(actualDrc).toString());
    }
}
