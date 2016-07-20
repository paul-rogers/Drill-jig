package org.apache.drill.jig.protocol;

public interface MessageConstants
{
  int OK_STATUS = 0;
  
  int HELLO_REQ_LEN = 4;
  int HELLO_RESP_LEN = 4;

  int REQ_HEADER_LEN = 6;

  int RESP_HEADER_LEN = 6;

  int DEFAULT_PORT = 32859;
  
  int DEFAULT_RESULTS_BUFFER_SIZE_K = 1024;
  
  int DEFAULT_RESULTS_WAIT_SEC = 30;
  
  // Request types

  int HELLO_REQ = 0;
  
  int LIST_LOGINS_REQ = 1;

  int LOGIN_PROPS_REQ = 2;

  int LOGIN_REQ = 3;

  int EXEC_STMT_REQ = 4;

  int EXEC_QUERY_REQ = 5;

  int RESULTS_REQ = 6;

  int CANCEL_QUERY_REQ = 7;

  int GOODBYE_REQ = 10;
  
  int REQ_COUNT = 20;
  
  // Response types

  int HELLO_RESP = 0;
  
  int OK_RESP = 1;

  int SUCCESS_RESP = 2;

  int ERROR_RESP = 3;

  int INFO_RESP = 4;

  int LIST_LOGIN_METHODS_RESP = 5;

  int LOGIN_PROPS_RESP = 6;

  int SCHEMA_RESP = 7;

  int RESULTS_RESP = 8;

  int EOF_RESP = 9;

  int GOODBYE_RESP = 10;

  int RESP_COUNT = 20;
  
  // Error codes

  int INTERNAL_ERROR = 999;

  String OPEN_LOGIN = "open";

  String USER_PWD_LOGIN = "basic";
  
  String USER_NAME_PROPERTY = "user";
  String PASSWORD_PROPERTY = "password";

  // Error codes
  
  int INCOMPATIBLE_VERSIONS_ERROR = 1;

  int INVALID_REQUEST_ERROR = 2;

  int DRILL_CONNECT_ERROR = 3;

  int EXECUTE_ERROR = 4;

  int BUFFER_TOO_SMALL_ERROR = 5;

  int ROW_TOO_BIG_ERROR = 6;

  String DEFAULT_HOST = "localhost";

  int DEFAULT_QUERY_TIMEOUT_SECS = 60;

  int DEFAULT_QUERY_POLL_PERIOD_MS = 15_000;
}
