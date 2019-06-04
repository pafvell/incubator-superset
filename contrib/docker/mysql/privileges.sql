-- 创建数据库
create database IF NOT EXISTS `superset` default character set utf8mb4 collate utf8mb4_unicode_ci;
use mysql;
-- 允许root远程登陆
update user set host = '%' where user = 'root';
select host, user from user;
-- 因为mysql版本是5.7，因此新建用户为如下命令：
create user superset identified by 'superset';
-- 将docker_mysql数据库的权限授权给创建的superset用户，密码为superset：
grant all on *.* to superset@'%' identified by 'superset' with grant option;
-- 这一条命令一定要有：
flush privileges;
