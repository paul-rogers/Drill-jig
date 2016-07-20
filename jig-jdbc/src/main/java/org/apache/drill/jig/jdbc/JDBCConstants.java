package org.apache.drill.jig.jdbc;

public interface JDBCConstants
{
  String JIG_PREFIX = "jdbc:jig:";
  
  String DRILL_PRESS_METHOD = "jig";
  String EMBEDDED_METHOD = "embedded";
  String DRILL_DIRECT_METHOD = "drill";
  String ZK_DIRECT_METHOD = "zk";
  String CONFIG_DIRECT_METHOD = "config";
  
  String METHOD_PROPERTY = "method";
  String USER_PROPERTY = "user";
  String PASSWORD_PROPERTY = "password";
  String HOST_PROPERTY = "host";
  String PORT_PROPERTY = "port";
  String SCHEMA_PROPERTY = "schema";
}
