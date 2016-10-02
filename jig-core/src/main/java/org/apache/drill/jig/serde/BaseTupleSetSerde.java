package org.apache.drill.jig.serde;

import org.apache.drill.jig.api.Cardinality;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldSchema;
import org.apache.drill.jig.api.TupleSchema;

public abstract class BaseTupleSetSerde
{
  protected TupleSchema schema;
  protected int fieldCount;
  protected boolean optional[];
  protected boolean isArray[];
  protected boolean isNull[];
  protected boolean isRepeated[];

  protected void prepare( int fieldCount ) {
    this.fieldCount = fieldCount;
    optional = new boolean[fieldCount];
    isArray = new boolean[fieldCount];
    for ( int i = 0;  i < fieldCount;  i++ ) {
      FieldSchema field = schema.field( i );
      optional[i] = field.nullable();
      isArray[i] = field.type() == DataType.LIST;
    }
    
    int padCount = (fieldCount + 3) & ~0x3;
    isNull = new boolean[padCount];
    isRepeated = new boolean[padCount];
  }
  
}
