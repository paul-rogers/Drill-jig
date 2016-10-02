package org.apache.drill.jig.types;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;
import java.util.Collection;

import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldValue;
import org.apache.drill.jig.exception.ValueConversionError;

public abstract class AbstractScalarFieldValue implements AbstractFieldValue {

	@Override
	public byte[] getBlob() {
		throw typeError( "blob" );
	}

	@Override
	public LocalDate getDate() {
		throw typeError( "date" );
	}

	@Override
	public LocalDateTime getDateTime() {
		throw typeError( "local Date/Time" );
	}

	@Override
	public Period getUTCTime() {
		throw typeError( "UTC Time" );
	}

	@Override
	public int size() {
		throw typeError( "collection" );
	}

	@Override
	public DataType memberType() {
		throw typeError( "array" );
	}

	@Override
	public FieldValue get(int i) {
		throw typeError( "array" );
	}

	@Override
	public Collection<String> keys() {
		throw typeError( "map" );
	}

	@Override
	public FieldValue get(String key) {
		throw typeError( "map" );
	}
	
	private ValueConversionError typeError( String dest ) {
		return new ValueConversionError( "Can't convert scalar to " + dest );
	}
}
