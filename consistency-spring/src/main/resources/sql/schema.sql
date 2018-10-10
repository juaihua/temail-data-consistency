
CREATE TABLE `listener_event` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `status` varchar(10) NOT NULL DEFAULT 'new' COMMENT '监听事件类型new pending complete',
  `content_id` bigint(20) NOT NULL COMMENT '监听同步消息id',
  `content` varchar(1000) DEFAULT NULL COMMENT '同步内容',
  `insert_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP COMMENT '生成时间',
  `from_addr` varchar(100) DEFAULT NULL,
  `to_addr` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8mb4;