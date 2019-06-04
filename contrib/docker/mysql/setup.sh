#!/bin/bashset -e
#查看mysql服务的状态，方便调试，这条语句可以删除
echo `service mysql status`
echo '1.启动mysql....'
#启动
#mysqlservice mysql start
service mysql start        #mysql docker ok '/var/run/mysqld/mysqld.sock'
#mysqld_safe --user=mysql    #mysql docker error
#mysqld_safe --user=mysql #--skip-grant-tables &
# The MySQL server is running with the --skip-grant-tables option so it cannot execute this statement
sleep 3
echo `service mysql status`
#重新设置mysql密码
echo '2.开始修改密码....'
mysql < /mysql/privileges.sql
echo '3.修改密码完毕....'#
sleep 3
echo `service mysql status`
echo 'mysql容器启动完毕'
tail -f /dev/null
