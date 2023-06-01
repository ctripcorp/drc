package com.ctrip.framework.drc.manager.healthcheck.notifier;

import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.entity.Applier;
import com.ctrip.framework.drc.core.entity.DbCluster;
import com.ctrip.framework.drc.core.entity.Instance;
import com.ctrip.framework.drc.core.entity.Messenger;
import com.ctrip.framework.drc.core.exception.DrcServerException;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.manager.concurrent.DrcKeyedOneThreadTaskExecutor;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.concurrent.KeyedOneThreadTaskExecutor;
import com.ctrip.xpipe.retry.RestOperationsRetryPolicyFactory;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.http.conn.ConnectTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.*;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestOperations;

import java.net.ConnectException;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.*;

/**
 * Created by mingdongli
 * 2019/11/22 上午12:34.
 */
public abstract class AbstractNotifier implements Notifier {

    public static final Logger logger = LoggerFactory.getLogger(AbstractNotifier.class);

    public static int RETRY_INTERVAL = 2000;

    protected RestOperations restTemplate = RestTemplateFactory.createCommonsHttpRestTemplate(4, 40, CONNECTION_TIMEOUT, 5000, 0, new RestOperationsRetryPolicyFactory(RETRY_INTERVAL)); //retry by Throwable

    private KeyedOneThreadTaskExecutor<String> notifyExecutor;

    private static final String NOTIFY_URL = "http://%s/%s";

    protected AbstractNotifier() {
        ExecutorService notifyExecutorService = ThreadUtils.newFixedThreadPool(PROCESSORS_SIZE, "CM-Notifier-Service");
        notifyExecutor = new DrcKeyedOneThreadTaskExecutor(notifyExecutorService);
    }

    @Override
    public void notify(String clusterId, DbCluster dbCluster) {  //retry when failed
        for (String ipAndPort : getDomains(dbCluster)) {
            String url = String.format(NOTIFY_URL, ipAndPort, getUrlPath());
            PostSend postSend = new PostSend(ipAndPort, url, dbCluster);
            notifyExecutor.execute(clusterId, new SendTask(clusterId, postSend));
        }
    }

    @Override
    public void notifyAdd(String clusterId, DbCluster dbCluster) {
        for (String ipAndPort : getDomains(dbCluster)) {
            String url = String.format(NOTIFY_URL, ipAndPort, getUrlPath());
            PutSend putSend = new PutSend(ipAndPort, url, dbCluster, false);
            notifyExecutor.execute(clusterId, new SendTask(clusterId, putSend));
        }
    }

    @Override
    public void notifyRegister(String clusterId, DbCluster dbCluster) {
        for (String ipAndPort : getDomains(dbCluster)) {
            String url = String.format(NOTIFY_URL, ipAndPort, getRegisterPath());
            PutSend putSend = new PutSend(ipAndPort, url, dbCluster, true);
            notifyExecutor.execute(clusterId, new SendTask(clusterId, putSend));
        }
    }

    @Override
    public void notifyRemove(String clusterId, Instance instance, boolean deleted) {
        String ip = instance.getIp();
        int port = instance.getPort();
        String url = String.format(NOTIFY_URL, ip + ":" + port, getUrlPath());
        String deleteUrl = url + "/" + clusterId + "/";
        if ((instance instanceof Applier || instance instanceof Messenger) && !deleted) {
            deleteUrl += deleted;
        }
        DeleteSend deleteSend = new DeleteSend(deleteUrl);
        notifyExecutor.execute(clusterId, new SendTask(clusterId, deleteSend));
    }

    private void doNotify(HttpSend httpSend) {
        String url = httpSend.getUrl();
        try {
            ApiResult<Boolean> apiResult = httpSend.sendHttp();
            boolean success = apiResult.getData();
            NOTIFY_LOGGER.info("[Notify] {} by http with result {} and message {}", url, success, apiResult.getMessage());
            if (!success) {
                success = checkStatus(apiResult.getStatus());
                if (!success) {
                    String errMsg = String.format("[Invoke] %s error", url);
                    throw new DrcServerException(errMsg);
                } else {
                    NOTIFY_LOGGER.info("[Success] set to true for status {}", apiResult.getStatus());
                }
            }
        } catch (Exception e) {
            String errMsg = String.format("[Invoke] %s throw exception", url);
            NOTIFY_LOGGER.error("{}", errMsg, e);
            throw new DrcServerException(errMsg, e);
        }
    }

    private boolean checkStatus(int status) {
        ResultCode resultCode = ResultCode.getResultCode(status);
        switch (resultCode) {
            case SERVER_ALREADY_EXIST:
            case PORT_ALREADY_EXIST:
                return true;
            default:
                return false;
        }
    }

    protected abstract String getUrlPath();

    protected abstract Object getBody(String ipAndPort, DbCluster dbCluster, boolean register);

    protected String getRegisterPath() {
        return getUrlPath() + "/register";
    }

    protected abstract List<String> getDomains(DbCluster dbCluster);

    interface HttpSend {
        ApiResult<Boolean> sendHttp() throws Exception;

        String getUrl();
    }

    abstract class AbstractHttpSend implements HttpSend {

        protected String ipAndPort;

        protected String url;

        protected DbCluster dbCluster;

        public AbstractHttpSend(String ipAndPort, String url, DbCluster dbCluster) {
            this.ipAndPort = ipAndPort;
            this.url = url;
            this.dbCluster = dbCluster;
        }

        @Override
        public String getUrl() {
            return url;
        }
    }

    class PostSend extends AbstractHttpSend implements HttpSend {

        public PostSend(String ipAndPort, String url, DbCluster dbCluster) {
            super(ipAndPort, url, dbCluster);
        }

        @Override
        public ApiResult<Boolean> sendHttp() {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.setAccept(Lists.newArrayList(MediaType.APPLICATION_JSON));
            HttpEntity<Object> entity = new HttpEntity<Object>(getBody(ipAndPort, dbCluster, false), headers);
            ResponseEntity<ApiResult> response = restTemplate.exchange(url, HttpMethod.POST, entity, ApiResult.class);
            return response.getBody();
            //return restTemplate.postForObject(url, getBody(ipAndPort, dbCluster, false), ApiResult.class);
        }
    }

    class PutSend extends AbstractHttpSend implements HttpSend {

        private boolean register;

        public PutSend(String ipAndPort, String url, DbCluster dbCluster, boolean register) {
            super(ipAndPort, url, dbCluster);
            this.register = register;
        }

        @Override
        public ApiResult<Boolean> sendHttp() {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.setAccept(Lists.newArrayList(MediaType.APPLICATION_JSON));
            HttpEntity<Object> entity = new HttpEntity<Object>(getBody(ipAndPort, dbCluster, register), headers);
            ResponseEntity<ApiResult> response = restTemplate.exchange(url, HttpMethod.PUT, entity, ApiResult.class);
            return response.getBody();
        }
    }

    class DeleteSend extends AbstractHttpSend implements HttpSend {

        public DeleteSend(String url) {
            super(null, url, null);
        }

        @Override
        public ApiResult<Boolean> sendHttp() {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.setAccept(Lists.newArrayList(MediaType.APPLICATION_JSON));
            restTemplate.delete(url);
            return ApiResult.getSuccessInstance(Boolean.TRUE);
        }
    }

    class SendTask extends AbstractCommand {

        private String clusterId;

        private AbstractHttpSend httpSend;

        public SendTask(String clusterId, AbstractHttpSend httpSend) {
            this.clusterId = clusterId;
            this.httpSend = httpSend;
        }

        @Override
        protected void doExecute() {
            try {
                doNotify(httpSend);
                future().setSuccess();
            } catch(Throwable t){
                if (isConnectException(t)) {
                    future().setSuccess();
                    NOTIFY_LOGGER.info("[Future] set success for {} on {}", httpSend.getUrl(), t.getCause());
                } else {
                    future().setFailure(t);
                }
            }
        }

        private boolean isConnectException(Throwable t) {
            DrcServerException serverException = (DrcServerException) t;
            Throwable throwable = serverException.getCause();
            if (throwable instanceof ResourceAccessException) {
                Throwable cause = throwable.getCause();
                return cause instanceof ConnectException || cause instanceof ConnectTimeoutException;
            }
            return false;
        }

        @Override
        protected void doReset() {

        }

        @Override
        public String getName() {
            return clusterId;
        }
    }

    @VisibleForTesting
    public void setRestTemplate(RestOperations restTemplate) {
        this.restTemplate = restTemplate;
    }
}
