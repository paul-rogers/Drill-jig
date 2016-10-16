package org.apache.drill.jig.serde;

import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldSchema;
import org.apache.drill.jig.api.TupleSchema;

public abstract class BaseTupleSetSerde
{
  protected final int fieldCount;
  protected final boolean optional[];
  protected final boolean isArray[];
  protected final boolean isNull[];
  protected final boolean isRepeated[];

  protected BaseTupleSetSerde( TupleSchema schema ) {
    this.fieldCount = schema.count();
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
