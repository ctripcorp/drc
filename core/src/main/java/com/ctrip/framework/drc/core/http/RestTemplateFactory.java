package com.ctrip.framework.drc.core.http;

import com.ctrip.xpipe.api.retry.RetryPolicy;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.retry.RetryNTimes;
import com.ctrip.xpipe.retry.RetryPolicyFactory;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.InputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.Objects;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.client.RestOperations;
import org.springframework.web.client.RestTemplate;

/**
 * @ClassName HttpsUtils
 * @Author haodongPan
 * @Date 2024/7/8 19:02
 * @Version: $
 */
public class RestTemplateFactory {

    private static final Logger logger = LoggerFactory.getLogger(RestTemplateFactory.class);
    public static String trustKeyType = "PKCS12";
    public static String trustPath = "ssl/trustKeys.p12";
    public static String trustPass = "drcdrc";
    public static Integer SSL_PORT = 8081;

    public static RestOperations createRestTemplateWithSSLContext(int maxConnPerRoute, int maxConnTotal,
            int connectTimeout, int soTimeout, int retryTimes, RetryPolicyFactory retryPolicyFactory){
     
        RestOperations restTemplateProxy = null;
        try (InputStream trustStream = RestTemplateFactory.class.getClassLoader().getResourceAsStream(trustPath)){
            if (trustStream == null) {
                logger.error("trustStore file not found: {}", Objects.requireNonNull(
                        RestTemplateFactory.class.getClassLoader().getResource("")).getPath() + trustPath);
            }
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            KeyStore trustStore = KeyStore.getInstance(trustKeyType);
            trustStore.load(trustStream, trustPass.toCharArray());
            
            trustManagerFactory.init(trustStore);
            TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, trustManagers, new SecureRandom());
            SSLConnectionSocketFactory sslConnectionSocketFactory = new SSLConnectionSocketFactory(sslContext, NoopHostnameVerifier.INSTANCE);
            
            CloseableHttpClient httpclient = HttpClients
                    .custom()
                    .setSSLSocketFactory(sslConnectionSocketFactory)
                    .setSSLHostnameVerifier(new NoopHostnameVerifier()) 
                    .setMaxConnPerRoute(maxConnPerRoute)
                    .setMaxConnTotal(maxConnTotal)
                    .setDefaultSocketConfig(SocketConfig.custom().setSoTimeout(soTimeout).build())
                    .setDefaultRequestConfig(RequestConfig.custom().setConnectTimeout(connectTimeout).build())
                    .build();
            HttpComponentsClientHttpRequestFactory requestFactory = new HttpComponentsClientHttpRequestFactory();
            requestFactory.setHttpClient(httpclient);
            RestTemplate restTemplate = new RestTemplate(requestFactory);
            //set jackson mapper
            for (HttpMessageConverter<?> hmc : restTemplate.getMessageConverters()) {
                if (hmc instanceof MappingJackson2HttpMessageConverter) {
                    ObjectMapper objectMapper = createObjectMapper();
                    MappingJackson2HttpMessageConverter mj2hmc = (MappingJackson2HttpMessageConverter) hmc;
                    mj2hmc.setObjectMapper(objectMapper);
                }
            }

            restTemplateProxy =  (RestOperations) Proxy.newProxyInstance(RestOperations.class.getClassLoader(),
                    new Class[]{RestOperations.class},
                    new RetryableRestOperationsHandler(restTemplate, retryTimes, retryPolicyFactory));
        }catch (Exception e){
            logger.error("createRestTemplate error", e);
        }
        return restTemplateProxy;
    }

    private static ObjectMapper createObjectMapper() {

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
        return objectMapper;

    }
    
    private static class RetryableRestOperationsHandler implements InvocationHandler {

        private RestTemplate restTemplate;

        private int retryTimes;

        private RetryPolicyFactory retryPolicyFactory;

        public RetryableRestOperationsHandler(RestTemplate restTemplate, int retryTimes,
                RetryPolicyFactory retryPolicyFactory) {
            this.restTemplate = restTemplate;
            this.retryTimes = retryTimes;
            this.retryPolicyFactory = retryPolicyFactory;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Exception {
            return retryableInvoke(restTemplate, method, args);
        }

        public Object retryableInvoke(final Object proxy, final Method method, final Object[] args) throws Exception {
            final RetryPolicy retryPolicy = retryPolicyFactory.create();

            return new RetryNTimes<Object>(retryTimes, retryPolicy, false).execute(new AbstractCommand<Object>() {

                @Override
                public String getName() {
                    return String.format("[retryable-invoke]%s(%s)", method.getName(), (args.length >= 1 ? args[0] : ""));
                }

                @Override
                protected void doExecute() throws Exception {
                    future().setSuccess(method.invoke(proxy, args));
                }

                @Override
                protected void doReset() {

                }

            });
        }

    }
    

}
