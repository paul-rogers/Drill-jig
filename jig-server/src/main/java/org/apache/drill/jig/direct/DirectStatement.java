package org.apache.drill.jig.direct;

import org.apache.drill.jig.api.ResultCollection;
import org.apache.drill.jig.api.Statement;

public class DirectStatement implements Statement
{
  DrillResultCollection results;
  private String stmt;
  private DrillSession session;

  public DirectStatement( DrillSession session, String stmt ) {
    this.session = session;
    this.stmt = stmt;
  }
  
  @Override
  public ResultCollection execute() {
    VectorRecordReader reader = new VectorRecordReader( session, stmt );
    return new DrillResultCollection( reader );
  }
  
  @Override
  public void close() {
    if ( results != null ) {
      results.close( );
      results = null;
    }
  }
}
