package org.apache.drill.jig.direct;

import static org.junit.Assert.*;

import org.apache.drill.jig.api.AlterSessionKeys;
import org.apache.drill.jig.direct.DrillClientContext;
import org.apache.drill.jig.direct.DrillConnectionFactory;
import org.apache.drill.jig.direct.DrillContextFactory;
import org.apache.drill.jig.direct.DirectConnection;
import org.junit.Test;

public class TestConnect
{

  @Test
  public void testEmbedded() throws Exception {
    new DrillContextFactory( )
        .withEmbeddedDrillbit( )
        .build( )
        .startEmbedded( );
    DirectConnection session = new DrillConnectionFactory( )
        .embedded( )
        .connect( );
    
    session.alterSession( AlterSessionKeys.MAX_WIDTH_PER_NODE, 2 );
    
    session.close();
    DrillClientContext.instance( ).stopEmbeddedDrillbit( );
    
    // If we get this far, things worked well enough. If they didn't,
    // we'd have encountered an exception.
    
    assertTrue( true );
  }

}
