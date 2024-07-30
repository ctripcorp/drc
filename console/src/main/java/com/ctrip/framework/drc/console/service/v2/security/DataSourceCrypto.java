package com.ctrip.framework.drc.console.service.v2.security;

public interface DataSourceCrypto {
    
    String encrypt(String content, String secretKey) ;
    String decrypt(String content, String secretKey) ;

}
