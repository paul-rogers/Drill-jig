package org.apache.drill.jig.types;

import org.apache.drill.jig.api.FieldValue;

public interface AbstractFieldValue extends FieldValue {
	
	public void bind( FieldAccessor accessor );
}
