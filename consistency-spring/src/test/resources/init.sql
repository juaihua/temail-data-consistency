CREATE TABLE `listener_event` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `status` varchar(10) NOT NULL DEFAULT 'new' COMMENT '监听事件类型new pending complete',
  `content_id` bigint(20) NOT NULL COMMENT '监听同步消息id',
  `content` varchar(1000) DEFAULT NULL COMMENT '同步内容',
  `insert_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP COMMENT '生成时间',
  `from_addr` varchar(100) DEFAULT NULL COMMENT '发件人',
  `to_addr` varchar(100) DEFAULT NULL COMMENT '收件人',
  PRIMARY KEY (`id`)
);
insert into `listener_event` ( `status`, `content_id`, `content`, `insert_time`, `from_addr`, `to_addr`) values ( 'new', '1', 'test1', '2018-08-17 10:21:31', 'bob', 'alice');
insert into `listener_event` ( `status`, `content_id`, `content`, `insert_time`, `from_addr`, `to_addr`) values ( 'new', '2', 'test2', '2018-08-17 10:22:01', 'jack', 'alice');
insert into `listener_event` ( `status`, `content_id`, `content`, `insert_time`, `from_addr`, `to_addr`) values ( 'new', '3', 'test3', '2018-08-17 10:22:24', 'bob', 'jack');
insert into `listener_event` ( `status`, `content_id`, `content`, `insert_time`, `from_addr`, `to_addr`) values ( 'new', '4', 'test4', '2018-08-17 10:22:54', 'john', 'bob');
insert into `listener_event` ( `status`, `content_id`, `content`, `insert_time`, `from_addr`, `to_addr`) values ( 'new', '5', 'test5', '2018-08-17 10:23:25', 'lucy', 'john');