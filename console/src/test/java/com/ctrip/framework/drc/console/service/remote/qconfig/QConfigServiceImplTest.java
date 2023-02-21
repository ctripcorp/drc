package com.ctrip.framework.drc.console.service.remote.qconfig;



import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.service.remote.qconfig.request.BatchUpdateRequestBody;
import com.ctrip.framework.drc.console.service.remote.qconfig.request.CreateFileRequestBody;
import com.ctrip.framework.drc.console.service.remote.qconfig.response.BatchUpdateResponse;
import com.ctrip.framework.drc.console.service.remote.qconfig.response.CreateFileResponse;
import com.ctrip.framework.drc.console.service.remote.qconfig.response.FileDetailData;
import com.ctrip.framework.drc.console.service.remote.qconfig.response.FileDetailResponse;
import com.ctrip.framework.drc.console.utils.MySqlUtils.TableSchemaName;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.monitor.reporter.EventMonitor;
import com.ctrip.framework.drc.core.service.dal.DbClusterApiService;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.ctrip.framework.foundation.Foundation;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class QConfigServiceImplTest {
    
    @InjectMocks private QConfigServiceImpl qConfigService;
    
    @Mock private DomainConfig domainConfig;
    
    @Mock private DbClusterApiService dbClusterService;
    
    @Mock private EventMonitor eventMonitor;
    
    
    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        Mockito.doReturn("db1_dalcluster").when(dbClusterService).getDalClusterName(Mockito.anyString(),Mockito.anyString());
        Mockito.doNothing().when(eventMonitor).logEvent(Mockito.anyString(),Mockito.anyString());
        Mockito.when(domainConfig.getDalClusterUrl()).thenReturn("dalclusterUrl");
        Mockito.when(domainConfig.getQConfigRestApiUrl()).thenReturn("url");
        Mockito.when(domainConfig.getQConfigAPIToken()).thenReturn("token");
        Mockito.when(domainConfig.getDc2QConfigSubEnvMap()).thenReturn(new HashMap<>() {{
            put("sinaws","SIN-AWS");
            put("shaxy","SHAXY");
        }});
        Mockito.when(domainConfig.getDcsInSameRegion("shaxy")).thenReturn(Sets.newHashSet("shaxy"));
    }

    @Test
    public void testAddOrUpdateDalClusterMqConfig() throws SQLException {
        String envName = Foundation.server().getEnv().getName();
        // create file
        try(MockedStatic<HttpUtils> theMock = Mockito.mockStatic(HttpUtils.class)) {
            theMock.when(() -> {
                HttpUtils.get(
                        Mockito.eq("url/configs" 
                                + "?token={token}" 
                                + "&groupid={groupid}" 
                                + "&dataid={dataid}" 
                                + "&env={env}" 
                                + "&subenv={subenv}" 
                                + "&targetgroupid={targetgroupid}"),
                        Mockito.any(),
                        Mockito.any(Map.class));
            }).thenReturn(JsonUtils.toJson(mockInExistentFileDetailResponse()));
            theMock.when(() -> {
                HttpUtils.post(
                        Mockito.eq("url" + "/configs" + "?token={token}"),
                        Mockito.any(CreateFileRequestBody.class),
                        Mockito.any(),
                        Mockito.any(Map.class));
            }).thenReturn(new CreateFileResponse() {{setStatus(0);}});
            boolean res = qConfigService.addOrUpdateDalClusterMqConfig(
                    "shaxy",
                    "topicName",
                    "db1\\.t3",
                    null,
                    new ArrayList<>() {{
                        add(new TableSchemaName("db1", "t3"));
                    }}
            );
            Assert.assertTrue(res);
        }
        
        
        // update file
        try(MockedStatic<HttpUtils> theMock = Mockito.mockStatic(HttpUtils.class)) {
            theMock.when(() -> {
                HttpUtils.get(
                        Mockito.eq("url/configs"
                                + "?token={token}"
                                + "&groupid={groupid}"
                                + "&dataid={dataid}"
                                + "&env={env}"
                                + "&subenv={subenv}"
                                + "&targetgroupid={targetgroupid}"),
                        Mockito.any(),
                        Mockito.any(Map.class));
            }).thenReturn(JsonUtils.toJson(mockExistingFileDetailResponse()));
            
            theMock.when(() -> {
                HttpUtils.post(Mockito.eq("url" 
                                + "/properties/binlog-topic-registry/envs/" 
                                + envName 
                                + "/subenvs/SHAXY" 
                                + "?token={token}&operator={operator}&serverenv={serverenv}&groupid={groupid}"),
                        Mockito.anyString(),
                        Mockito.any(),
                        Mockito.any(Map.class));
            }).thenReturn(mockBatchUpdateResponse());
            boolean res = qConfigService.addOrUpdateDalClusterMqConfig(
                    "shaxy",
                    "topicName",
                    "db1\\.t3",
                    null,
                    new ArrayList<>() {{
                        add(new TableSchemaName("db1", "t3"));
                    }}
            );
            Assert.assertTrue(res);
        }
    }



    @Test
    public void testDisableDalClusterMqConfigIfNecessary() throws SQLException {
        String envName = Foundation.server().getEnv().getName();
        try(MockedStatic<HttpUtils> theMock = Mockito.mockStatic(HttpUtils.class)) {
            theMock.when(() -> {
                HttpUtils.get(
                        Mockito.eq("url/configs"
                                + "?token={token}"
                                + "&groupid={groupid}"
                                + "&dataid={dataid}"
                                + "&env={env}"
                                + "&subenv={subenv}"
                                + "&targetgroupid={targetgroupid}"),
                        Mockito.any(),
                        Mockito.any(Map.class));
            }).thenReturn(JsonUtils.toJson(mockExistingFileDetailResponse()));


            theMock.when(() -> {
                HttpUtils.post(Mockito.eq("url"
                                + "/properties/binlog-topic-registry/envs/" 
                                + envName
                                + "/subenvs/SHAXY"
                                + "?token={token}&operator={operator}&serverenv={serverenv}&groupid={groupid}"),
                        Mockito.anyString(),
                        Mockito.any(),
                        Mockito.any(Map.class));
            }).thenReturn(mockBatchUpdateResponse());
            boolean b = qConfigService.removeDalClusterMqConfigIfNecessary(
                    "shaxy",
                    "topicName",
                    "db1\\.t1",
                    null,
                    new ArrayList<>() {{
                        add(new TableSchemaName("db1", "t1"));
                    }},
                    null
            );
            
            boolean b1 = qConfigService.removeDalClusterMqConfigIfNecessary(
                    "shaxy",
                    "topicName",
                    "db1\\.t1",
                    null,
                    new ArrayList<>() {{
                        add(new TableSchemaName("db1", "t1"));
                    }},
                    Lists.newArrayList("db1\\.t3")
            );

            boolean b2 = qConfigService.removeDalClusterMqConfigIfNecessary(
                    "shaxy",
                    "topicName",
                    "db1\\.t1",
                    null,
                    new ArrayList<>() {{
                        add(new TableSchemaName("db1", "t1"));
                    }},
                    Lists.newArrayList("db1\\..*")
            );
            Assert.assertTrue(b & b1 & b2);
        }
    }




    private BatchUpdateResponse mockBatchUpdateResponse() {
        BatchUpdateResponse batchUpdateResponse = new BatchUpdateResponse();
        batchUpdateResponse.setStatus(0);
        return batchUpdateResponse;
    }
    
    private FileDetailResponse mockInExistentFileDetailResponse() {
        FileDetailResponse fileDetail = new FileDetailResponse();
        fileDetail.setStatus(-1);
        return fileDetail;
    }
    
    private FileDetailResponse mockExistingFileDetailResponse() {
        FileDetailResponse fileDetail = new FileDetailResponse();
        fileDetail.setStatus(0);
        fileDetail.setMessage("message");
        
        FileDetailData fileDetailData = new FileDetailData();
        fileDetailData.setEditVersion(0);
        fileDetailData.setData("topicName.status=off\ntopicName.dbName=db1,db2\ntopicName.tableName=t1,t2");
        fileDetail.setData(fileDetailData);
        
        return fileDetail;
    }
    
}