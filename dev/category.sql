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
DROP TABLE IF EXISTS `category`;
CREATE TABLE `category` (
  `sub_category_id` int(11) NOT NULL AUTO_INCREMENT COMMENT '子品类ID',
  `parent_category_id` int(11) NOT NULL COMMENT '父品类ID',
  `parent_category_name` varchar(50) NOT NULL COMMENT '父品类名称',
  PRIMARY KEY (`sub_category_id`)
) ENGINE=InnoDB AUTO_INCREMENT=34344 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of orders
-- ----------------------------
INSERT INTO `category` VALUES (4580530, 1,'服饰鞋包');
INSERT INTO `category` VALUES (4580531, 2,'家装家饰');
INSERT INTO `category` VALUES (4580532, 3,'家电');
INSERT INTO `category` VALUES (4580533, 4,'美妆');
INSERT INTO `category` VALUES (4580534, 5,'母婴');
INSERT INTO `category` VALUES (4580535, 6,'3C数码');
INSERT INTO `category` VALUES (4580536, 7,'运动户外');
INSERT INTO `category` VALUES (4580537, 8,'食品');
