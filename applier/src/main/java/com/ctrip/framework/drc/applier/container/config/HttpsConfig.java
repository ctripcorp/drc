package com.ctrip.framework.drc.applier.container.config;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.HTTPS_PORT;

import java.nio.charset.StandardCharsets;
import org.apache.catalina.connector.Connector;
import org.apache.coyote.http11.Http11NioProtocol;
import org.springframework.boot.context.embedded.ConfigurableEmbeddedServletContainer;
import org.springframework.boot.context.embedded.EmbeddedServletContainerCustomizer;
import org.springframework.boot.context.embedded.tomcat.TomcatEmbeddedServletContainerFactory;
import org.springframework.context.annotation.Configuration;

/**
 * @ClassName HttpsConfig
 * @Author haodongPan
 * @Date 2024/7/8 19:58
 * @Version: $
 */
@Configuration
public class HttpsConfig implements EmbeddedServletContainerCustomizer {

    private static final String KEYSTORE_NAME = "classpath:ssl/applier.p12";
    private static final String KEYSTORE_PASSWORD = "drcdrc";
    private static final String KEYSTORE_TYPE = "PKCS12";
    private static final String KEYSTORE_ALIAS = "applier";

    private Connector createHttpsConnector() {
        Connector httpsConnector = new Connector("org.apache.coyote.http11.Http11NioProtocol");
        httpsConnector.setScheme("https");
        httpsConnector.setSecure(true);
        httpsConnector.setPort(HTTPS_PORT);
        httpsConnector.setURIEncoding(StandardCharsets.UTF_8.name());

        if (httpsConnector.getProtocolHandler() instanceof Http11NioProtocol) {
            Http11NioProtocol httpProtocol = (Http11NioProtocol) httpsConnector.getProtocolHandler();
            httpProtocol.setSslProtocol("TLS");
            httpProtocol.setSSLEnabled(true);
            httpProtocol.setClientAuth("false");
            httpProtocol.setKeystoreType(KEYSTORE_TYPE);
            httpProtocol.setKeyAlias(KEYSTORE_ALIAS);
            httpProtocol.setKeystorePass(KEYSTORE_PASSWORD);
            httpProtocol.setKeystoreFile(KEYSTORE_NAME);
        }
        return httpsConnector;
    }

    @Override
    public void customize(ConfigurableEmbeddedServletContainer container) {
        if (container instanceof TomcatEmbeddedServletContainerFactory) {
            customizeTomcat((TomcatEmbeddedServletContainerFactory) container);
        }
    }

    private void customizeTomcat(TomcatEmbeddedServletContainerFactory tomcat) {
        tomcat.addAdditionalTomcatConnectors(createHttpsConnector());
    }
}
