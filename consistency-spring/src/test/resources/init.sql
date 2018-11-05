
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

insert into `listener_event` ( `status`, `content`, `topic`, `tag`, `insert_time`, `update_time`) values ( 'NEW', 'test1', 'bob', 'alice', '2018-08-17 10:21:31', '2018-08-17 10:23:45');
insert into `listener_event` ( `status`, `content`, `topic`, `tag`, `insert_time`, `update_time`) values ( 'NEW', 'test2', 'jack', 'alice', '2018-08-17 10:22:01', '2018-08-17 10:24:50');
insert into `listener_event` ( `status`, `content`, `topic`, `tag`, `insert_time`, `update_time`) values ( 'NEW', 'test3', 'bob', 'jack', '2018-08-17 10:22:24', '2018-08-17 10:30:12');
insert into `listener_event` ( `status`, `content`, `topic`, `tag`, `insert_time`, `update_time`) values ( 'NEW', 'test4', 'john', 'bob', '2018-08-17 10:22:54', '2018-08-17 10:45:21');
insert into `listener_event` ( `status`, `content`, `topic`, `tag`, `insert_time`, `update_time`) values ( 'NEW', 'test5', 'lucy', 'john', '2018-08-17 10:23:25', '2018-08-17 10:50:23');