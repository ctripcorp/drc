package com.ctrip.framework.drc.console.service.v2.security.impl;

import com.ctrip.framework.drc.console.service.v2.security.DataSourceCrypto;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.google.common.collect.Maps;
import java.security.SecureRandom;
import java.util.Map;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * @ClassName DataSourceCryptoImpl
 * @Author haodongPan
 * @Date 2024/5/29 14:59
 * @Version: $
 */
@Service
public class DataSourceCryptoImpl implements DataSourceCrypto {
    
    private Logger logger = LoggerFactory.getLogger(DataSourceCryptoImpl.class);
    
    private Map<String, Cipher> encryptCipherMap = Maps.newConcurrentMap();
    private Map<String, Cipher> decryptCipherMap = Maps.newConcurrentMap();
    

    @Override
    public String encrypt(String content, String secretKey) {
        if (StringUtils.isEmpty(content)) {
            throw ConsoleExceptionUtils.message("content  is empty");
        }
        if (StringUtils.isEmpty(secretKey)) {
            throw ConsoleExceptionUtils.message("secretKey is empty");
        }
        
        if (!encryptCipherMap.containsKey(secretKey)) {
            initialize(secretKey);
        }
        try {
            byte[] bytes = content.getBytes("UTF-8");
            byte[] result = encryptCipherMap.get(secretKey).doFinal(bytes);
            
            return parseByte2HexStr(result);
        } catch (Exception e) {
            logger.error("encrypt error", e);
            throw ConsoleExceptionUtils.message("encrypt error");
        }
    }

    
    
    @Override
    public String decrypt(String content, String secretKey) {
        if (StringUtils.isEmpty(content)) {
            throw ConsoleExceptionUtils.message("content  is empty");
        }
        if (StringUtils.isEmpty(secretKey)) {
            throw ConsoleExceptionUtils.message("secretKey is empty");
        }
        
        if (!decryptCipherMap.containsKey(secretKey)) {
            initialize(secretKey);
        }
        try {
            byte[] bytes = parseHexStr2Byte(content);
            byte[] result = decryptCipherMap.get(secretKey).doFinal(bytes);
            
            return new String(result, "UTF-8");
        } catch (Exception e) {
            logger.error("decrypt error", e);
            throw ConsoleExceptionUtils.message("decrypt error");
        }
    }
    
    private void initialize(String secretKey) {
        try {
            KeyGenerator kgen = KeyGenerator.getInstance("AES");
            SecureRandom random = SecureRandom.getInstance("SHA1PRNG");
            random.setSeed(secretKey.getBytes());
            kgen.init(128,random);
            SecretKey realSecretKey = kgen.generateKey();
            byte[] enCodeFormat = realSecretKey.getEncoded();
            SecretKeySpec key = new SecretKeySpec(enCodeFormat, "AES");

            Cipher encryptCipher = Cipher.getInstance("AES");
            encryptCipher.init(Cipher.ENCRYPT_MODE, key);
            encryptCipherMap.putIfAbsent(secretKey, encryptCipher);

            Cipher decryptCipher = Cipher.getInstance("AES");
            decryptCipher.init(Cipher.DECRYPT_MODE, key);
            decryptCipherMap.putIfAbsent(secretKey, decryptCipher);
        } catch (Exception e) {
            logger.error("initialize cipher error",e);
            throw ConsoleExceptionUtils.message("initialize cipher error");
        }
        
    }

    static String parseByte2HexStr(byte buf[]) {
        return DatatypeConverter.printHexBinary(buf);
    }

    static byte[] parseHexStr2Byte(String hexStr) {
        return DatatypeConverter.parseHexBinary(hexStr);
    }

}
