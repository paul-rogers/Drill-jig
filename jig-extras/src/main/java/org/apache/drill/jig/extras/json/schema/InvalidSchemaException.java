package org.apache.drill.jig.extras.json.schema;

@SuppressWarnings("serial")
public class InvalidSchemaException extends RuntimeException
{

  public InvalidSchemaException(String msg) {
    super( msg );
  }
  
}