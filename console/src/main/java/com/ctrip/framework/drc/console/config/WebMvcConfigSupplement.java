package com.ctrip.framework.drc.console.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.format.FormatterRegistry;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;
import org.springframework.http.converter.json.Jackson2ObjectMapperFactoryBean;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.http.converter.xml.MappingJackson2XmlHttpMessageConverter;
import org.springframework.web.servlet.config.annotation.*;


import java.text.SimpleDateFormat;
import java.util.List;
import java.util.logging.Logger;

/**
 * @ClassName WebMvcConfigSupplement
 * @Author haodongPan
 * @Date 2022/1/27 17:24
 * @Version: $
 */
@Configuration
public class WebMvcConfigSupplement extends WebMvcConfigurerAdapter {
    private final Logger logger = Logger.getLogger(getClass().getSimpleName());

    @Override
    public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
        Jackson2ObjectMapperBuilder builder = new Jackson2ObjectMapperBuilder();
        builder.indentOutput(true).dateFormat(new SimpleDateFormat("yyyy-MM-dd"));
        converters.add(new MappingJackson2HttpMessageConverter(builder.build()));
    }

    @Override
    public void extendMessageConverters(List<HttpMessageConverter<?>> converters) {
        super.extendMessageConverters(converters);
        MappingJackson2HttpMessageConverter defaultConverter = null;
        for (HttpMessageConverter converter : converters) {
            if (converter instanceof MappingJackson2HttpMessageConverter) {
                defaultConverter = (MappingJackson2HttpMessageConverter) converter;
                break;
            }
        }
        if (defaultConverter != null) {
            converters.remove(defaultConverter);
            converters.set(0, defaultConverter);
            logger.info("!!! default converter use MappingJackson2HttpMessageConverter");
        }
        logger.info("!!! default converter no use MappingJackson2HttpMessageConverter");
    }

}
