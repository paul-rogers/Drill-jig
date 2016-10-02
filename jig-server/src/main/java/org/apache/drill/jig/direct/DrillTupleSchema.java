package org.apache.drill.jig.direct;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.jig.api.Cardinality;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.impl.FieldSchemaImpl;
import org.apache.drill.jig.api.impl.TupleSchemaImpl;

public class DrillTupleSchema extends TupleSchemaImpl
{
  public static class DrillFieldSchema extends FieldSchemaImpl
  {
    protected final MinorType drillType;
    
    public DrillFieldSchema(String name, DataType type, Cardinality cardinality, MinorType drillType) {
      super(name, type, cardinality);
      this.drillType = drillType;
    }
  }
  
  public DrillFieldAccessor accessors[];
  
  protected DrillFieldSchema getDrillField( int i ) {
    return (DrillFieldSchema) field( i );
  }
}