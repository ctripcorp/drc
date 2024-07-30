package com.ctrip.framework.drc.service.soa;

import com.ctrip.framework.drc.core.driver.binlog.manager.task.NamedCallable;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.RetryTask;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterResult.Status;
import com.ctrip.framework.drc.core.server.common.filter.row.soa.CustomSoaRowFilterContext;
import com.ctrip.framework.drc.core.server.common.filter.service.CustomSoaService;
import com.ctriposs.baiji.rpc.client.GenericServiceClient;
import com.ctriposs.baiji.rpc.client.ServiceClientBase;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import javax.validation.groups.Default;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ClassName TripCustomSoaService
 * @Author haodongPan
 * @Date 2024/7/15 17:50
 * @Version: $
 */
public class TripCustomSoaService implements CustomSoaService {
    
    public static final Logger CUSTOM_SOA_LOGGER = LoggerFactory.getLogger("CUSTOM SOA");
    private static final String OPERATION = "FilterRow";
    private static final String SERVICE_URL = "http://soa.ctrip.com/%s";
    private static final int RETRY_TIME = 2;
    
    private final Map<String,ServiceClientBase> clientMap = new ConcurrentHashMap<>();
    

    @Override
    public int getOrder() {
        return 0;
    }

    @Override
    public Status filter(int serviceCode, String serviceName, CustomSoaRowFilterContext soaRowFilterContext) throws Exception {
        Status rowsFilterStatus = new RetryTask<>(new CustomSoaFilterTask(serviceCode,serviceName, soaRowFilterContext), RETRY_TIME).call();
        if (rowsFilterStatus == null) {
            throw new RuntimeException( "[CustomSoaService] serviceCode:" +serviceCode+ ",call soa with request error" + soaRowFilterContext);
        }
        if (rowsFilterStatus == Status.No_Filtered) {
            // todo hdpan pass is minority or not? 
            CUSTOM_SOA_LOGGER.info("serviceCode:{} call soa with request {} pass", serviceCode, soaRowFilterContext);
        }
        return rowsFilterStatus;
    }
    
    private class CustomSoaFilterTask implements NamedCallable<Status>  {
        
        private int serviceCode;
        private String serviceName;
        private CustomSoaRowFilterContext rowFilterContext;
        
        public CustomSoaFilterTask(int serviceCode, String serviceName, CustomSoaRowFilterContext rowFilterContext) {
            this.serviceCode = serviceCode;
            this.serviceName = serviceName;
            this.rowFilterContext = rowFilterContext;
        }

        @Override
        public Status call() throws Exception {
            ServiceClientBase client = clientMap.computeIfAbsent(
                    String.format(SERVICE_URL, serviceCode),
                    url -> GenericServiceClient.getInstance().getClient(url, serviceName)
            );
            FilterRowRequestType filterRowRequestType = new FilterRowRequestType(
                    rowFilterContext.getDbName(),
                    rowFilterContext.getTableName(),
                    rowFilterContext.getColumnName(),
                    rowFilterContext.getColumnValue(),
                    rowFilterContext.getSrcRegion(),
                    rowFilterContext.getDstRegion(),
                    null
            );
            FilterRowResponseType response = (FilterRowResponseType) client.invoke(OPERATION, filterRowRequestType, FilterRowResponseType.class);
            if (!Objects.equals(0, response.getResultCode())) {
                throw new RuntimeException("FilterRowResponseType error, result: " + response);
            }
            if (response.isFilterPass() == null) {
                CUSTOM_SOA_LOGGER.warn("[CustomSoaService] serviceCode:{} call soa with request ({}) return null", serviceCode,rowFilterContext);
                DefaultEventMonitorHolder.getInstance().logEvent("DRC.replicator.rows.filter.soa."+serviceCode, "badRequest",1);
                return Status.Filtered;
            }
            return Status.from(response.isFilterPass());
        }

        @Override
        public void afterFail() {
            CUSTOM_SOA_LOGGER.error("[CustomSoaService] serviceCode:{} call soa with request ({}) fail", serviceCode,rowFilterContext);
        }
        
        @Override
        public void afterException(Throwable t) {
            CUSTOM_SOA_LOGGER.error("[CustomSoaService] serviceCode:{} call soa with request ({}) error", serviceCode,rowFilterContext, t);
        }

        @Override
        public void afterSuccess(int retryTime) {
        }
    }
    
}
