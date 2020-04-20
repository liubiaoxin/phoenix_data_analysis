/*
 Navicat Premium Data Transfer

 Source Server         : mysql
 Source Server Type    : MySQL
 Source Server Version : 50646
 Source Host           : 192.168.1.103:3306
 Source Schema         : test

 Target Server Type    : MySQL
 Target Server Version : 50646
 File Encoding         : 65001

 Date: 09/04/2020 16:54:43
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for orders
-- ----------------------------
DROP TABLE IF EXISTS `orders`;
CREATE TABLE `orders` (
  `orderId` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `orderNo` varchar(50) NOT NULL COMMENT '订单号',
  `userId` int(11) NOT NULL COMMENT '用户ID',
  `goodId` int(11) NOT NULL COMMENT '商品ID',
  `goodsMoney` decimal(11,2) NOT NULL DEFAULT '0.00' COMMENT '商品总金额',
  `realTotalMoney` decimal(11,2) NOT NULL DEFAULT '0.00' COMMENT '实际订单总金额',
  `payFrom` int(11) NOT NULL DEFAULT '0' COMMENT '支付来源(1:支付宝，2：微信)',
  `province` varchar(50) NOT NULL COMMENT '省份',
  `createTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`orderId`)
) ENGINE=InnoDB AUTO_INCREMENT=34344 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of orders
-- ----------------------------
BEGIN;
INSERT INTO `orders` VALUES (3, '1', 3223, 4432, 9999.00, 7878787.00, 0, '湖北', '2020-04-06 14:31:48');
INSERT INTO `orders` VALUES (66, '33232', 3223, 4432, 112.60, 3232.99, 1, '湖北', '2020-04-06 14:31:48');
INSERT INTO `orders` VALUES (69, '33232', 3223, 4432, 112.60, 3232.99, 1, '湖北', '2020-04-06 14:31:48');
INSERT INTO `orders` VALUES (90, '88', 3223, 4432, 112.60, 3232.99, 1, '湖北', '2020-04-06 14:31:48');
INSERT INTO `orders` VALUES (3333, '33232', 3223, 4432, 444.00, 3232.99, 1, '湖北', '2020-04-06 14:31:48');
INSERT INTO `orders` VALUES (5555, '6666', 3223, 4432, 112.60, 3232.99, 1, '湖北', '2020-04-06 14:31:48');


update orders  set orderId=888888   where orderId=5555;
delete from `orders` where orderId=3;




SELECT USERID,PROVINCE, count(distinct ORDERID),sum(REALTOTALMONEY) FROM flink_dwd_orders group by USERID,PROVINCE
