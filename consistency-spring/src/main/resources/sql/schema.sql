
CREATE TABLE `listener_event` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `status` varchar(10) NOT NULL DEFAULT 'new' COMMENT '监听事件类型NEW SENDED',
  `content` mediumtext COMMENT '消息内容',
  `topic` varchar(100) DEFAULT '',
  `tag` varchar(32) DEFAULT '',
  `insert_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP COMMENT '生成时间',
  `update_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;