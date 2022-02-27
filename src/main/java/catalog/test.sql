-- MySQL dump 10.13  Distrib 8.0.19, for Win64 (x86_64)
--
-- Host: 127.0.0.1    Database: test
-- ------------------------------------------------------
-- Server version	5.7.33-log

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `catalog_database`
--

DROP TABLE IF EXISTS `catalog_database`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `catalog_database` (
  `table_id` int(11) NOT NULL AUTO_INCREMENT COMMENT '表唯一标识，自增',
  `db_name` varchar(512) DEFAULT NULL COMMENT '源属于的库',
  `table_name` varchar(512) DEFAULT NULL COMMENT '源属于的表',
  `comment` text COMMENT '源表级别注释',
  `create_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`table_id`),
  KEY `uni_table_ind` (`db_name`,`table_name`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `catalog_database`
--

LOCK TABLES `catalog_database` WRITE;
/*!40000 ALTER TABLE `catalog_database` DISABLE KEYS */;
INSERT INTO `catalog_database` VALUES (1,'test','datagen_sou',NULL,NULL,NULL),(2,'test','ka_test',NULL,'2022-02-27 07:56:22','2022-02-27 08:10:31');
/*!40000 ALTER TABLE `catalog_database` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `catalog_table_conn_info`
--

DROP TABLE IF EXISTS `catalog_table_conn_info`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `catalog_table_conn_info` (
  `table_id` int(11) NOT NULL COMMENT '表唯一标识',
  `key` varchar(256) NOT NULL COMMENT '表连接属性名',
  `value` varchar(20480) DEFAULT NULL COMMENT '表连接属性变量',
  `create_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`table_id`,`key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `catalog_table_conn_info`
--

LOCK TABLES `catalog_table_conn_info` WRITE;
/*!40000 ALTER TABLE `catalog_table_conn_info` DISABLE KEYS */;
INSERT INTO `catalog_table_conn_info` VALUES (1,'connector','datagen',NULL,NULL),(2,'connector','kafka','2022-02-27 07:57:35','2022-02-27 07:57:35'),(2,'format','json','2022-02-27 07:59:28','2022-02-27 07:59:28'),(2,'properties.bootstrap.servers','test:9092','2022-02-27 07:59:28','2022-02-27 07:59:28'),(2,'properties.group.id','test_group1','2022-02-27 07:59:28','2022-02-27 07:59:41'),(2,'scan.startup.mode','earliest-offset','2022-02-27 07:59:28','2022-02-27 07:59:28'),(2,'topic','test','2022-02-27 07:57:35','2022-02-27 07:57:35');
/*!40000 ALTER TABLE `catalog_table_conn_info` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `catalog_table_schema`
--

DROP TABLE IF EXISTS `catalog_table_schema`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `catalog_table_schema` (
  `table_id` int(11) NOT NULL COMMENT '表唯一标识',
  `schema_root` tinyint(4) NOT NULL DEFAULT '0' COMMENT '0为简单类型.1为特殊类型(不能简单的表达都为1)',
  `schema_name` varchar(256) NOT NULL COMMENT 'schema名字',
  `schema_type` varchar(20480) NOT NULL COMMENT '属性类型,特殊类型会存入json方便用户二次解析,如: flink row 表达',
  `comment` text COMMENT '列属性注释',
  `create_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`table_id`,`schema_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `catalog_table_schema`
--

LOCK TABLES `catalog_table_schema` WRITE;
/*!40000 ALTER TABLE `catalog_table_schema` DISABLE KEYS */;
INSERT INTO `catalog_table_schema` VALUES (1,1,'array_array','{\"type\":\"ARRAY\",\"next\":{\"type\":\"ARRAY\",\"next\":{\"type\":\"STRING\"}}}',NULL,'2022-02-27 14:16:19','2022-02-27 14:16:19'),(1,1,'map_map_nested','{\"type\":\"MAP\",\"key\":{\"type\":\"STRING\"},\"value\":{\"type\":\"MAP\",\"key\":{\"type\":\"STRING\"},\"value\":{\"type\":\"INTEGER\"}}}',NULL,NULL,NULL),(1,1,'nested_struct','{\"type\":\"STRUCT\",\"field\":[{\"type\":\"TIME\",\"name\":\"start_time\"},{\"type\":\"STRUCT\",\"name\":\"inside_row\",\"field\":[{\"type\":\"STRING\",\"name\":\"ins\"}]}]}',NULL,NULL,NULL),(1,0,'order_number','BIGINT',NULL,NULL,NULL),(1,0,'order_time','TIMESTAMP(3)',NULL,NULL,NULL),(1,0,'price','DECIMAL(32,2)',NULL,NULL,NULL),(1,1,'simple_struct','{\"type\":\"STRUCT\",\"field\":[{\"type\":\"TIME\",\"name\":\"start_time\"}]}',NULL,NULL,NULL),(1,1,'tags','{\"type\":\"ARRAY\",\"next\":{\"type\":\"STRING\"}}',NULL,NULL,NULL),(2,0,'str_test','STRING',NULL,'2022-02-27 08:04:50','2022-02-27 08:17:42');
/*!40000 ALTER TABLE `catalog_table_schema` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2022-02-27 22:36:55
