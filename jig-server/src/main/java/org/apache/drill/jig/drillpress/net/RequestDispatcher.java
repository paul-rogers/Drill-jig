package org.apache.drill.jig.drillpress.net;

import org.apache.drill.jig.protocol.MessageConstants;
import org.apache.drill.jig.proto.ErrorResponse;

public class RequestDispatcher implements Dispatcher
{
  RequestHandler handler;
  
  public RequestDispatcher( RequestHandler handler ) {
    this.handler = handler;
  }
  
  @Override
  public void dispatch(RequestHandler.RequestContext request) {
    try {
      switch( request.opCode ) {
      case MessageConstants.HELLO_REQ:
        handler.hello(request);
        break;
      case MessageConstants.LIST_LOGINS_REQ:
        handler.listLogins(request);
        break;
      case MessageConstants.LOGIN_PROPS_REQ:
        handler.loginProperties(request);
        break;
      case MessageConstants.LOGIN_REQ:
        handler.login(request);
        break;
      case MessageConstants.EXEC_STMT_REQ:
        handler.executeStmt(request);
        break;
      case MessageConstants.EXEC_QUERY_REQ:
        handler.executeQuery(request);
        break;
      case MessageConstants.RESULTS_REQ:
        handler.requestData(request);
        break;
      case MessageConstants.CANCEL_QUERY_REQ:
        handler.cancelQuery(request);
        break;
      case MessageConstants.GOODBYE_REQ:
        handler.goodbye(request);
        break;
       default:
         // TODO: Log an error and return an error code.
         throw new IllegalStateException( "Unsupported client op code: " + request.opCode );
      }
    }
    catch ( Exception e ) {
      // Log.error( e.getMessage( ), e );
      ErrorResponse resp = new ErrorResponse( );
      resp.setCode( MessageConstants.INTERNAL_ERROR );
      resp.setMessage( e.getMessage() );
      MessageUtils.write( request.ctx, MessageConstants.ERROR_RESP, resp, ErrorResponse.getSchema() );
    }   
  }
}
