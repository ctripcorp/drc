package com.ctrip.framework.drc.console.config;

import java.util.Collections;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.http.converter.xml.MappingJackson2XmlHttpMessageConverter;
import org.springframework.util.CollectionUtils;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import java.util.List;

/**
 * @ClassName WebConfig
 * @Author haodongPan
 * @Date 2022/3/15 14:50
 * @Version: $
 */
@Configuration
public class WebResponseTypeAdapter extends WebMvcConfigurerAdapter {

    @Override
    public void extendMessageConverters(List<HttpMessageConverter<?>> converters) {
        // 从converters中移除xml的converter
        converters.removeIf(converter -> converter instanceof MappingJackson2XmlHttpMessageConverter);
    }
}
