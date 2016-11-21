package org.apache.drill.jig.server.net;

import org.apache.drill.jig.exception.JigException;
import org.apache.drill.jig.proto.ExecuteRequest;
import org.apache.drill.jig.proto.ListLoginsResponse;
import org.apache.drill.jig.proto.LoginPropertiesRequest;
import org.apache.drill.jig.proto.LoginPropertiesResponse;
import org.apache.drill.jig.proto.LoginRequest;
import org.apache.drill.jig.proto.QueryRequest;
import org.apache.drill.jig.proto.SuccessResponse;
import org.apache.drill.jig.protocol.DataResponse;
import org.apache.drill.jig.protocol.HelloRequest;
import org.apache.drill.jig.protocol.HelloResponse;

public interface RequestProcessor
{
  HelloResponse hello( HelloRequest request ) throws JigException;
  ListLoginsResponse listLogins( ) throws JigException;
  LoginPropertiesResponse loginProperties( LoginPropertiesRequest request ) throws JigException;
  void login( LoginRequest request ) throws JigException;
  
  SuccessResponse executeStmt( ExecuteRequest request ) throws JigException;
  void executeQuery( QueryRequest request ) throws JigException;
  DataResponse requestData( ) throws JigException;
  void cancelQuery( ) throws JigException;
  
  void goodbye( ) throws JigException;
}