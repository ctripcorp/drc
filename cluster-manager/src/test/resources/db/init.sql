-- 延时监控表
CREATE DATABASE IF NOT EXISTS drcmonitordb;

CREATE TABLE `drcmonitordb`.`delaymonitor` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `src_ip` varchar(15) NOT NULL,
  `dest_ip` varchar(15) NOT NULL,
  `delay` BIGINT(20) default -1,
  `datachange_lasttime` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

INSERT INTO `drcmonitordb`.`delaymonitor`(`id`,`src_ip`, `dest_ip`, `datachange_lasttime`) VALUES(1,'shaoy', 'shaoy', DATE_SUB(NOW(), INTERVAL 75 SECOND));
INSERT INTO `drcmonitordb`.`delaymonitor`(`id`,`src_ip`, `dest_ip`, `datachange_lasttime`) VALUES(2,'sharb', 'sharb', DATE_SUB(NOW(), INTERVAL 76 SECOND));