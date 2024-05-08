package com.ctrip.framework.drc.applier.confirmed.mysql;

import com.ctrip.framework.drc.core.driver.pool.DrcTomcatDataSource;
import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.apache.tomcat.jdbc.pool.PoolProperties;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

import static org.junit.Assert.*;

/**
 * @Author Slight
 * Dec 08, 2019
 */
public class ConflictTest {

    private Logger logger = LoggerFactory.getLogger(getClass());

    protected DataSource dataSource;
    protected Connection connection;

    @Before
    public void setUp() throws Exception {
        connect();
        resetTable();
    }

    @After
    public void tearDown() throws Exception {
        disconnect();
    }

    public void connect() throws SQLException {
        PoolProperties properties = new PoolProperties();
        properties.setUrl("jdbc:mysql://127.0.0.1:3306?allowMultiQueries=true&useLocalSessionState=true&useSSL=false&useUnicode=true&characterEncoding=UTF-8&useAffectedRows=true");
        properties.setDriverClassName("com.mysql.jdbc.Driver");
        properties.setUsername("root");
        properties.setPassword("123456");
        properties.setDefaultAutoCommit(false);
        properties.setConnectionProperties("connectTimeout=1000;socketTimeout=2000");
        dataSource = new DrcTomcatDataSource(properties);
        connection = dataSource.getConnection();
    }

    public void disconnect() {
        dataSource.close(true);
    }

    String tablePRI = "CREATE TABLE `prod`.`hello1` (\n" +
            "  `id` int(11) NOT NULL AUTO_INCREMENT,\n" +
            "  `user` varchar(11) DEFAULT NULL,\n" +
            "  `lt` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),\n" +
            "  PRIMARY KEY (`id`)\n" +
            ")";

    String tableUNI = "CREATE TABLE `prod`.`hello2` (\n" +
            "  `id` int(11) NOT NULL,\n" +
            "  `user` varchar(11) DEFAULT NULL,\n" +
            "  `lt` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),\n" +
            "  UNIQUE (`id`)\n" +
            ")";

    String tableMonitor = "CREATE TABLE `prod`.`monitor` (\n" +
            "  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n" +
            "  `src_ip` varchar(15) NOT NULL,\n" +
            "  `dest_ip` varchar(15) NOT NULL,\n" +
            "  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',\n" +
            "  PRIMARY KEY (`id`)\n" +
            ") ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8mb4";

    String tableOrderCategory = "CREATE TABLE `prod`.`order_category` (\n" +
            "  `ID` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键ID',\n" +
            "  `BasicOrderID` bigint(20) DEFAULT NULL COMMENT '关联表basicorder主键ID',\n" +
            "  `OrderID` bigint(20) DEFAULT NULL COMMENT '单编号',\n" +
            "  `BizType` smallint(6) DEFAULT NULL COMMENT '业务类型（OrderDbType）',\n" +
            "  `UID` varchar(20) DEFAULT NULL COMMENT '用户名',\n" +
            "  `Category` smallint(6) DEFAULT NULL COMMENT '订单分类（1-点评，2-出行，3-支付）',\n" +
            "  `StatusCode` smallint(6) DEFAULT NULL COMMENT '状态码（0-未发生，1-已发生）',\n" +
            "  `StartDate` datetime(3) DEFAULT NULL COMMENT '开始时间（点评-可点评开始时间，出行-无，支付-无）',\n" +
            "  `EndDate` datetime(3) DEFAULT NULL COMMENT '截止时间（点评-点评截止时间，出行-出行时间，支付-无）',\n" +
            "  `DataChange_CreateTime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间戳（精确到毫秒）',\n" +
            "  `DataChange_LastTime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '时间戳（精确到毫秒）',\n" +
            "  PRIMARY KEY (`ID`),\n" +
            "  UNIQUE KEY `UNI_BasicOrderId_Category` (`BasicOrderID`,`Category`),\n" +
            "  KEY `IDX_UID` (`UID`),\n" +
            "  KEY `idx_DataChange_LastTime` (`DataChange_LastTime`),\n" +
            "  KEY `idx_uid_orderid` (`UID`,`OrderID`)\n" +
            ") ENGINE=InnoDB AUTO_INCREMENT=1215662138 DEFAULT CHARSET=utf8 COMMENT='订单分类'";

    public void resetTable() throws SQLException {
        executeIf("drop table prod.hello1");
        executeIf("drop table prod.hello2");
        executeIf("drop table prod.monitor");
        executeIf("drop table prod.order_category");
        executeIf("create database prod");
        execute(tablePRI);
        execute(tableUNI);
        execute(tableMonitor);
        execute(tableOrderCategory);
    }

    public void execute(String SQL) throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute(SQL);
        statement.close();
    }

    public void executeIf(String SQL) {
        try {
            Statement statement = connection.createStatement();
            statement.execute(SQL);
            statement.close();
        } catch (SQLException e) {
            logger.error("executeIf error", e);
        }
    }

    @Test
    public void insertConflictForPRI() throws SQLException {
        PreparedStatement statement =
                connection.prepareStatement("insert into prod.hello1 (id, user, lt) values (?,?,?),(?,?,?)");
        statement.setInt(1, 1);
        statement.setString(2, "Blades");
        statement.setString(3, "2019-12-09 16:00:00.000");
        statement.setInt(4, 2);
        statement.setString(5, "_Torn");
        statement.setString(6, "2019-12-09 16:00:00.000");
        assertFalse(statement.execute());
        assertEquals(2, statement.getUpdateCount());
        statement.close();
        try {
            statement =
                    connection.prepareStatement("insert into prod.hello1 (id, user, lt) values (?,?,?)");
            statement.setInt(1, 1);
            statement.setString(2, "Phy");
            statement.setString(3, "2019-12-09 16:00:00.001");
            statement.execute();
            assert 1 == 2;
        } catch (MySQLIntegrityConstraintViolationException e) {
            assertEquals("Duplicate entry '1' for key 'PRIMARY'", e.getMessage());
            statement.close();
            statement = connection.prepareStatement("select `id`,`lt` from prod.hello1 where `id` = 1");
            assertTrue(statement.execute());
            ResultSet rs = statement.getResultSet();
            assertTrue(rs.last());
            assertEquals(1, rs.getRow());
            assertTrue(rs.absolute(1));
            assertEquals(1, rs.getInt(1));
            if (rs.getString(2).compareTo("2019-12-09 16:00:00.001") > 0) {
                //conflict(y) override(n)
            } else {
                //conflict(y) override(y)
                statement.close();
                statement = connection.prepareStatement("UPDATE `prod`.`hello1` SET `user`=?,`lt`=?  WHERE `id`=? AND`lt`<=?");
                statement.setString(1, "Phy");
                statement.setString(2, "2019-12-09 16:00:00.001");
                statement.setInt(3, 1);
                statement.setString(4, "2019-12-09 16:00:00.001");
                assertFalse(statement.execute());
                assertEquals(1, statement.getUpdateCount());
                statement.close();
            }
        }
    }

    @Test
    public void insertConflictForUNI() throws SQLException {
        PreparedStatement statement =
                connection.prepareStatement("insert into prod.hello2 (id, user, lt) values (?,?,?),(?,?,?)");
        statement.setInt(1, 1);
        statement.setString(2, "Blades");
        statement.setString(3, "2019-12-09 16:00:00.000");
        statement.setInt(4, 2);
        statement.setString(5, "_Torn");
        statement.setString(6, "2019-12-09 16:00:00.000");
        assertFalse(statement.execute());
        assertEquals(2, statement.getUpdateCount());
        statement.close();
        try {
            statement =
                    connection.prepareStatement("insert into prod.hello2 (id, user, lt) values (?,?,?)");
            statement.setInt(1, 1);
            statement.setString(2, "Phy");
            statement.setString(3, "2019-12-09 15:59:59.999");
            statement.execute();
            assert 1 == 2;
        } catch (MySQLIntegrityConstraintViolationException e) {
            assertEquals("Duplicate entry '1' for key 'id'", e.getMessage());
            statement.close();
        }
    }
}
