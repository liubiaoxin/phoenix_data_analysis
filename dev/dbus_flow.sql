/*
 Navicat Premium Data Transfer

 Source Server         : mysql
 Source Server Type    : MySQL
 Source Server Version : 50646
 Source Host           : 192.168.1.103:3306
 Source Schema         : dbus

 Target Server Type    : MySQL
 Target Server Version : 50646
 File Encoding         : 65001

 Date: 09/04/2020 16:52:48
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for dbus_flow
-- ----------------------------
DROP TABLE IF EXISTS `dbus_flow`;
CREATE TABLE `dbus_flow` (
  `flowId` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `mode` int(11) NOT NULL COMMENT '存储类型(#PHOENIX  #NATIVE   #STRING,默认STRING)',
  `databaseName` varchar(50) NOT NULL COMMENT 'database',
  `tableName` varchar(50) NOT NULL COMMENT 'table',
  `hbaseTable` varchar(50) NOT NULL COMMENT 'hbaseTable',
  `family` varchar(50) NOT NULL COMMENT 'family',
  `uppercaseQualifier` tinyint(1) NOT NULL COMMENT '字段名转大写, 默认为true',
  `commitBatch` int(11) NOT NULL COMMENT '一次提交条数',
  `rowKey` varchar(100) NOT NULL COMMENT '组成rowkey的字段名，必须用逗号分隔',
  `status` int(11) NOT NULL COMMENT '状态:1-初始,2:就绪,3:运行',
  PRIMARY KEY (`flowId`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of dbus_flow
-- ----------------------------
BEGIN;
INSERT INTO `dbus_flow` VALUES (1, 1, 'test', 'orders', 'dwd:orders', 'CF', 1, 10, 'orderId,orderNo', 2);
COMMIT;

SET FOREIGN_KEY_CHECKS = 1;
