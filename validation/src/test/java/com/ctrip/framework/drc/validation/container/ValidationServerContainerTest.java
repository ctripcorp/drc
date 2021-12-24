package com.ctrip.framework.drc.validation.container;

import com.ctrip.framework.drc.core.server.config.validation.dto.ValidationConfigDto;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;

import static com.ctrip.framework.drc.validation.AllTests.*;

public class ValidationServerContainerTest {

    @InjectMocks
    private ValidationServerContainer validationServerContainer = new ValidationServerContainer();

    @Before
    public void setUp() { MockitoAnnotations.initMocks(this); }

    @Test
    public void testAddServer() throws Exception {
        ValidationConfigDto configDto = getValidationConfigDto();
        boolean added = validationServerContainer.addServer(configDto);
        Assert.assertTrue(added);

        added = validationServerContainer.addServer(configDto);
        Assert.assertFalse(added);

        ValidationConfigDto configDto1 = getValidationConfigDto();
        configDto1.setReplicator(getReplicator2());
        added = validationServerContainer.addServer(configDto1);
        Assert.assertTrue(added);

        added = validationServerContainer.addServer(configDto);
        Assert.assertTrue(added);

        configDto1 = getValidationConfigDto();
        configDto1.setGtidExecuted(GTID_EXECUTED2);
        added = validationServerContainer.addServer(configDto);
        Assert.assertFalse(added);
    }
}
