package com.ctrip.framework.drc.service.mq;

import com.ctrip.framework.ckafka.client.KafkaClientFactory;
import com.ctrip.framework.drc.service.config.TripServiceDynamicConfig;
import org.apache.kafka.clients.producer.Producer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * Created by dengquanliang
 * 2025/1/17 17:51
 */
public class KafkaProducerFactoryTest {
    MockedStatic<TripServiceDynamicConfig> dynamicConfigMockedStatic;
    MockedStatic<KafkaClientFactory> theMock;
    @Before
    public void setUp() throws Exception {
        TripServiceDynamicConfig mockConfig = Mockito.mock(TripServiceDynamicConfig.class);
        Mockito.when(mockConfig.getKafkaAppidToken()).thenReturn("client.id");
        Mockito.when(mockConfig.getKafkaLingerMs(Mockito.anyString())).thenReturn("");
        Mockito.when(mockConfig.getKafkaBatchSize(Mockito.anyString())).thenReturn("");
        Mockito.when(mockConfig.getKafkaMaxRequestSize(Mockito.anyString())).thenReturn("");
        Mockito.when(mockConfig.getKafkaBufferMemory(Mockito.anyString())).thenReturn("");
        Mockito.when(mockConfig.getCompressionType(Mockito.anyString())).thenReturn("");
        Mockito.when(mockConfig.getAcks(Mockito.anyString())).thenReturn("");
        dynamicConfigMockedStatic = Mockito.mockStatic(TripServiceDynamicConfig.class);
        dynamicConfigMockedStatic.when(() -> TripServiceDynamicConfig.getInstance()).thenReturn(mockConfig);

        Producer<String, String> producer = Mockito.mock(Producer.class);
        theMock = Mockito.mockStatic(KafkaClientFactory.class);
        theMock.when(() -> KafkaClientFactory.newProducer(Mockito.anyString(), Mockito.any(Properties.class))).thenReturn(producer);

    }

    @After
    public void tearDown() throws Exception {
        dynamicConfigMockedStatic.close();
        theMock.close();
    }

    @Test
    public void testCreateProvider() throws Exception {
        Producer<String, String> provider1 = KafkaProducerFactory.createProducer("topic");
        Assert.assertEquals(1, KafkaProducerFactory.getRefCount("topic"));

        Producer<String, String> provider2 = KafkaProducerFactory.createProducer("topic");
        Assert.assertEquals(2, KafkaProducerFactory.getRefCount("topic"));

        Assert.assertEquals(provider1, provider2);

        KafkaProducerFactory.destroy("topic");
        Assert.assertEquals(1, KafkaProducerFactory.getRefCount("topic"));
        KafkaProducerFactory.destroy("topic");


        Producer<String, String> provider3 = KafkaProducerFactory.createProducer("topic");
        Assert.assertEquals(provider1, provider3);
        Assert.assertEquals(1, KafkaProducerFactory.getRefCount("topic"));
    }

    @Test
    public void name() {
        String str = "{\n" +
                "  \"orderKeyInfo\": {\n" +
                "    \"pks\": [\n" +
                "      \"22618622\"\n" +
                "    ],\n" +
                "    \"schemaName\": \"globalsearchdb\",\n" +
                "    \"tableName\": \"gl_activitysearch_languages_new\"\n" +
                "  },\n" +
                "  \"otterParseTime\": 1742525246313,\n" +
                "  \"otterSendTime\": 1742525246313,\n" +
                "  \"beforeColumnList\": [\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"productid\",\n" +
                "      \"isKey\": true,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"22618622\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"FirstTagIds\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"firsttagcode\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"DistributionChannelIds\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"CommentScore\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.00\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"CommentCount\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"PoiIds\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"SalesVolumeLast30days\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"ProductSortScore\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0000\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"LocationCityIds\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"DestinationCityIds\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"LocationDistrictIds\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"DestinationDistrictIds\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"IsActive\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"F\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_en_us\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_en_au\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_en_gb\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_en_my\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_en_sg\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_en_hk\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_zh_hk\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_zh_tw\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_ko_kr\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_ja_jp\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_th_th\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_vi_vn\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_fr_fr\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_ru_ru\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_en_us\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_en_au\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_en_gb\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_en_my\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_en_sg\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_en_hk\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_zh_hk\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_zh_tw\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_ko_kr\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_ja_jp\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_th_th\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_vi_vn\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_fr_fr\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_ru_ru\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_en_us\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_en_au\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_en_gb\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_en_my\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_en_sg\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_en_hk\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_zh_hk\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_zh_tw\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_ko_kr\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_ja_jp\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_th_th\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_vi_vn\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_fr_fr\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_ru_ru\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"app_locales\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"en-th,id-id,en-au,en-ca,es-us,en-in,ms-my,fr-be,nl-be,ru-ru,en-my,de-de,en-sa,en-sg,en-ie,en-il,en-pk,de-ch,es-es,de-at,zh-tw,en-id,fr-ch,th-th,en-gb,it-it,vi-vn,fr-fr,en-ae,en-hk,es-mx,en-nz,nl-nl,en-ph,en-be,en-us,ko-kr,ja-jp,zh-hk\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"online_locales\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"id-id,en-th,en-au,en-ca,es-us,en-in,ms-my,fr-be,nl-be,ru-ru,en-my,de-de,en-sa,en-sg,en-ie,en-il,en-pk,de-ch,es-es,de-at,en-id,en-gb,fr-ch,th-th,zh-tw,it-it,es-mx,vi-vn,en-ae,en-hk,fr-fr,en-nz,nl-nl,en-ph,en-be,en-us,ko-kr,zh-hk,ja-jp\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"h5_locales\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"id-id,en-th,en-au,en-ca,es-us,en-in,ms-my,fr-be,ru-ru,nl-be,en-my,de-de,en-sa,en-sg,en-ie,en-il,de-ch,en-pk,es-es,de-at,zh-tw,en-id,th-th,en-gb,fr-ch,it-it,es-mx,vi-vn,en-ae,en-hk,fr-fr,en-nz,nl-nl,en-ph,en-be,en-us,ja-jp,zh-hk,ko-kr\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"2025-03-21 14:08:03.323\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"2025-03-21 09:42:44.563\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"is_ticket\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_en_ca\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_en_ae\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_en_il\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_en_nz\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_en_sa\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_en_be\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_en_ie\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_en_ca\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_en_ae\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_en_il\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_en_nz\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_en_sa\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_en_be\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_en_ie\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_en_ca\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_en_ae\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_en_il\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_en_nz\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_en_sa\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_en_be\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_en_ie\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_en_th\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_en_id\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_en_ph\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_en_th\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_en_id\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_en_ph\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_en_th\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_en_id\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_en_ph\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_id_id\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_ms_my\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tag_name_id_id\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tag_name_ms_my\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_id_id\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_ms_my\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    }\n" +
                "  ],\n" +
                "  \"eventType\": \"UPDATE\",\n" +
                "  \"schemaName\": \"globalsearchdb\",\n" +
                "  \"afterColumnList\": [\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"productid\",\n" +
                "      \"isKey\": true,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"22618622\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"FirstTagIds\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"firsttagcode\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"DistributionChannelIds\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"CommentScore\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.00\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"CommentCount\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"PoiIds\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"SalesVolumeLast30days\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"ProductSortScore\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0000\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"LocationCityIds\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"DestinationCityIds\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"LocationDistrictIds\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"DestinationDistrictIds\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"IsActive\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"F\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_en_us\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_en_au\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_en_gb\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_en_my\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_en_sg\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_en_hk\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_zh_hk\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_zh_tw\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_ko_kr\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_ja_jp\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_th_th\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_vi_vn\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_fr_fr\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_ru_ru\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_en_us\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_en_au\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_en_gb\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_en_my\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_en_sg\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_en_hk\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_zh_hk\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_zh_tw\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_ko_kr\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_ja_jp\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_th_th\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_vi_vn\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_fr_fr\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_ru_ru\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_en_us\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_en_au\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_en_gb\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_en_my\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_en_sg\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_en_hk\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_zh_hk\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_zh_tw\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_ko_kr\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_ja_jp\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_th_th\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_vi_vn\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_fr_fr\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_ru_ru\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"app_locales\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": true,\n" +
                "      \"value\": \"en-th,id-id,en-au,en-ca,es-us,ms-my,en-in,fr-be,nl-be,ru-ru,en-my,de-de,en-sa,en-sg,en-ie,en-il,de-ch,en-pk,es-es,de-at,zh-tw,th-th,fr-ch,en-id,en-gb,it-it,vi-vn,fr-fr,en-ae,en-hk,es-mx,en-nz,nl-nl,en-ph,en-be,en-us,ko-kr,ja-jp,zh-hk\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"online_locales\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": true,\n" +
                "      \"value\": \"id-id,en-th,en-au,en-ca,es-us,en-in,ms-my,fr-be,nl-be,ru-ru,en-my,de-de,en-sa,en-sg,en-ie,en-il,en-pk,de-ch,es-es,de-at,zh-tw,en-gb,fr-ch,en-id,th-th,it-it,vi-vn,fr-fr,en-ae,en-hk,es-mx,en-nz,nl-nl,en-ph,en-be,en-us,ko-kr,ja-jp,zh-hk\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"h5_locales\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": true,\n" +
                "      \"value\": \"en-th,id-id,en-au,en-ca,es-us,en-in,ms-my,fr-be,ru-ru,nl-be,en-my,de-de,en-sa,en-sg,en-ie,en-il,de-ch,en-pk,es-es,de-at,zh-tw,en-id,th-th,en-gb,fr-ch,it-it,es-mx,fr-fr,en-ae,en-hk,vi-vn,en-nz,nl-nl,en-ph,en-be,en-us,ja-jp,zh-hk,ko-kr\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"datachange_lasttime\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": true,\n" +
                "      \"value\": \"2025-03-21 10:37:01.178\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"is_ticket\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_en_ca\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_en_ae\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_en_il\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_en_nz\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_en_sa\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_en_be\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_en_ie\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_en_ca\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_en_ae\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_en_il\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_en_nz\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_en_sa\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_en_be\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_en_ie\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_en_ca\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_en_ae\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_en_il\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_en_nz\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_en_sa\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_en_be\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_en_ie\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_en_th\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_en_id\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_en_ph\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_en_th\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_en_id\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tagname_en_ph\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_en_th\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_en_id\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_en_ph\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_id_id\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"name_ms_my\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tag_name_id_id\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": true,\n" +
                "      \"name\": \"tag_name_ms_my\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_id_id\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"isNull\": false,\n" +
                "      \"name\": \"score_ms_my\",\n" +
                "      \"isKey\": false,\n" +
                "      \"isUpdated\": false,\n" +
                "      \"value\": \"0.0\"\n" +
                "    }\n" +
                "  ],\n" +
                "  \"tableName\": \"gl_activitysearch_languages_new\"\n" +
                "}";

        // 获取字符串的字节数
        byte[] byteArray = str.getBytes(StandardCharsets.UTF_8);
        int byteCount = byteArray.length;

        // 转换为 MB
        double sizeInMB = byteCount / (1024.0 * 1024.0);

        System.out.println("字符串大小: " + sizeInMB + " MB");
    }
}
