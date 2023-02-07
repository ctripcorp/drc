package com.ctrip.framework.drc.console.controller;

import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.service.LocalService;
import com.ctrip.framework.drc.console.service.RowsFilterService;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.MySqlEndpoint;
import com.ctrip.framework.drc.core.server.common.filter.table.aviator.AviatorRegexFilter;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultHandlers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.sql.SQLException;


public class LocalControllerTest extends AbstractControllerTest {
    
    @InjectMocks
    private LocalController localController;

    @Mock
    private DbClusterSourceProvider dbClusterSourceProvider;

    @Mock
    private RowsFilterService rowsFilterService;
    
    @Mock
    private LocalService localService;
    
    @Mock
    private DalUtils dalUtils;

    @Before
    public void setUp() throws SQLException {
        /** initialization */
        MockitoAnnotations.openMocks(this);
        /** build mvc env */
        mvc = MockMvcBuilders.standaloneSetup(localController).build();
        Mockito.when(dbClusterSourceProvider.getMasterEndpoint(Mockito.anyString())).
                thenReturn(new MySqlEndpoint("ip",0,"user","psw",true));
    }
    
    
    @Test
    public void testQuerySqlReturnInteger() {
        try(MockedStatic<MySqlUtils> theMock = Mockito.mockStatic(MySqlUtils.class)) {
            
            theMock.when(() -> MySqlUtils.getSqlResultInteger(Mockito.any(Endpoint.class),Mockito.anyString(),Mockito.anyInt())).
                    thenReturn(1);
            MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/local/sql/integer/query?" +
                                    "mha=" + "mha1" +
                                    "&sql=" + "show global variables like 'auto_increment_increment';" +
                                    "&index=" + 2 )
                            .accept(MediaType.APPLICATION_JSON))
                    .andDo(MockMvcResultHandlers.print())
                    .andReturn();
            int status = mvcResult.getResponse().getStatus();
            String response = mvcResult.getResponse().getContentAsString();
            Assert.assertEquals(200, status);
            System.out.println(response);

            theMock.when(() -> MySqlUtils.getSqlResultInteger(Mockito.any(Endpoint.class),Mockito.anyString(),Mockito.anyInt())).
                    thenThrow(new SQLException("sql query error"));
            mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/local/sql/integer/query?" +
                                    "mha=" + "mha1" +
                                    "&sql=" + "show global variables like 'auto_increment_increment';" +
                                    "&index=" + 2 )
                            .accept(MediaType.APPLICATION_JSON))
                    .andDo(MockMvcResultHandlers.print())
                    .andReturn();
            status = mvcResult.getResponse().getStatus();
            response = mvcResult.getResponse().getContentAsString();
            Assert.assertEquals(200, status);
            System.out.println(response);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetCreateTableStatements() {
        try(MockedStatic<MySqlUtils> theMock = Mockito.mockStatic(MySqlUtils.class)) {
            theMock.when(() -> MySqlUtils.getDefaultCreateTblStmts(Mockito.any(Endpoint.class),Mockito.any(AviatorRegexFilter.class))).
                    thenReturn(null);
            MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/local/createTblStmts/query?" +
                                    "mha=" + "mha1" +
                                    "&unionFilter=" + "db1\\..*")
                            .accept(MediaType.APPLICATION_JSON))
                    .andDo(MockMvcResultHandlers.print())
                    .andReturn();
            int status = mvcResult.getResponse().getStatus();
            String response = mvcResult.getResponse().getContentAsString();
            Assert.assertEquals(200, status);
            System.out.println(response);

            theMock.when(() -> MySqlUtils.getDefaultCreateTblStmts(Mockito.any(Endpoint.class),Mockito.any(AviatorRegexFilter.class))).
                    thenThrow(new SQLException("sql query error"));
            mvcResult = mvc.perform(MockMvcRequestBuilders.get("/api/drc/v1/local/createTblStmts/query?" +
                                    "mha=" + "mha1" +
                                    "&unionFilter=" + "db1\\..*")
                            .accept(MediaType.APPLICATION_JSON))
                    .andDo(MockMvcResultHandlers.print())
                    .andReturn();
            status = mvcResult.getResponse().getStatus();
            response = mvcResult.getResponse().getContentAsString();
            Assert.assertEquals(200, status);
            System.out.println(response);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    @Test
    public void testInsertDdlHistory()  {

        try {
            Mockito.when(
                    dalUtils.insertDdlHistory(Mockito.anyString(),Mockito.anyString(),Mockito.anyInt(),Mockito.anyString(),Mockito.anyString())
            ).thenReturn(1);
            MvcResult mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/local/ddlHistory?" +
                                    "mhaName=" + "mha1" +
                                    "&ddl=" + "ddl_sql" + 
                                    "&queryType=" + 2 + 
                                    "&schemaName=" + "db1" + 
                                    "&tableName=" + "table1"
                            )
                            .accept(MediaType.APPLICATION_JSON))
                    .andDo(MockMvcResultHandlers.print())
                    .andReturn();
            int status = mvcResult.getResponse().getStatus();
            String response = mvcResult.getResponse().getContentAsString();
            Assert.assertEquals(200, status);
            System.out.println(response);

            Mockito.when(
                    dalUtils.insertDdlHistory(Mockito.anyString(),Mockito.anyString(),Mockito.anyInt(),Mockito.anyString(),Mockito.anyString())
            ).thenThrow(new SQLException("sql query error"));
            mvcResult = mvc.perform(MockMvcRequestBuilders.post("/api/drc/v1/local/ddlHistory?" +
                                    "mhaName=" + "mha1" +
                                    "&ddl=" + "ddl_sql" +
                                    "&queryType=" + 2 +
                                    "&schemaName=" + "db1" +
                                    "&tableName=" + "table1"
                            )
                            .accept(MediaType.APPLICATION_JSON))
                    .andDo(MockMvcResultHandlers.print())
                    .andReturn();
            status = mvcResult.getResponse().getStatus();
            response = mvcResult.getResponse().getContentAsString();
            Assert.assertEquals(200, status);
            System.out.println(response);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}