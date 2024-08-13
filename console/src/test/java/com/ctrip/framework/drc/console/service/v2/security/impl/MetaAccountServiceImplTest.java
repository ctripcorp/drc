package com.ctrip.framework.drc.console.service.v2.security.impl;

import static org.mockito.Mockito.when;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.v2.MetaProviderV2;
import com.ctrip.framework.drc.console.param.v2.security.Account;
import com.ctrip.framework.drc.console.param.v2.security.MhaAccounts;
import com.ctrip.framework.drc.console.service.v2.CentralService;
import com.ctrip.framework.drc.core.entity.Drc;
import com.ctrip.framework.drc.core.transform.DefaultSaxParser;
import com.ctrip.xpipe.utils.FileUtils;
import java.io.IOException;
import java.sql.SQLException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.xml.sax.SAXException;


public class MetaAccountServiceImplTest {

    @Mock
    private MetaProviderV2 metaProviderV2;
    @Mock
    private CentralService centralService;
    @Mock
    private DefaultConsoleConfig consoleConfig;
    @Mock
    private MonitorTableSourceProvider monitorTableSourceProvider;
    
    @InjectMocks
    MetaAccountServiceImpl metaAccountService;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testGetMhaAccounts() throws IOException, SAXException, SQLException {
        when(consoleConfig.getAccountFromMetaSwitch()).thenReturn(false);
        when(monitorTableSourceProvider.getMonitorUserVal()).thenReturn("mockUser");
        when(monitorTableSourceProvider.getReadUserVal()).thenReturn("mockUser");
        when(monitorTableSourceProvider.getWriteUserVal()).thenReturn("mockUser");
        when(monitorTableSourceProvider.getMonitorPasswordVal()).thenReturn("mockPwd");
        when(monitorTableSourceProvider.getReadPasswordVal()).thenReturn("mockPwd");
        when(monitorTableSourceProvider.getWritePasswordVal()).thenReturn("mockPwd");
        
        when(consoleConfig.getAccountFromMetaSwitch()).thenReturn(true);
        Drc drc = DefaultSaxParser.parse(FileUtils.getFileInputStream("api/open_api_meta.xml"));
        when(metaProviderV2.getDrc()).thenReturn(drc);
        when(centralService.getMhaAccounts(Mockito.eq("newMha"))).thenReturn(new MhaAccounts("newMha",new Account(),new Account(),new Account()));
        MhaAccounts mhaAccounts1 = metaAccountService.getMhaAccounts("fat-fx-drc1");
        Assert.assertEquals("root",mhaAccounts1.getMonitorAcc().getUser());
        Assert.assertEquals("root",mhaAccounts1.getMonitorAcc().getPassword());

        MhaAccounts mhaAccounts2 = metaAccountService.getMhaAccounts("newMha");
        Assert.assertNull(mhaAccounts2.getMonitorAcc().getUser());
        Assert.assertNull(mhaAccounts2.getMonitorAcc().getPassword());


        MhaAccounts mhaAccounts3 = metaAccountService.loadCache("fat-fx-drc1");
        Assert.assertEquals("root",mhaAccounts3.getMonitorAcc().getUser());
        Assert.assertEquals("root",mhaAccounts3.getMonitorAcc().getPassword());

        MhaAccounts mhaAccounts4 = metaAccountService.loadCache("newMha");
        Assert.assertNull(mhaAccounts4.getMonitorAcc().getUser());
        Assert.assertNull(mhaAccounts4.getMonitorAcc().getPassword());
        


    }
}