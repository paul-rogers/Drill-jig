package org.apache.drill.jig.extras.json;

import java.io.Reader;

public class JsonResultCollectionBuilder {

  protected Reader reader;
  protected String input;
  protected boolean flatten;

  public JsonResultCollectionBuilder forReader( Reader reader ) {
    this.reader = reader;
    return this;
  }
  
  public JsonResultCollectionBuilder forString( String input ) {
    this.input = input;
    return this;
  }
  
  public JsonResultCollectionBuilder flatten( ) {
    flatten = true;
    return this;
  }
  
  public JsonResultCollection build( ) {
    return new JsonResultCollection( this );
  }
}
