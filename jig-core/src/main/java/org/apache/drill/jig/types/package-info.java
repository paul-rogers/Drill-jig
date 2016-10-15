/**
 * Defines Jig's type system, and methods for accessing value of the Jig types.
 * The field value system is built using composition using several components:
 * <ul>
 * <li>{@link TupleSchema} - API component to describes the schema for tuples
 * within a tuple
 * set. (Recall that a tuple set is defined as a collection of tuples that
 * share a common schema.)</li>
 * <li>{@link FieldSchema} - API component to describes the field
 * (name, type, nullable).</li>
 * <li>{@link TupleValue} - Client-visible API to access the values of fields
 * within a tuple.</li>
 * <li>{@link FieldValueContainer} - An internal structure that selects the
 * proper field value for each field. Non-nullale, non-variant fields all have
 * the same field value. Nullable fields may have a null or non-null value.
 * Variant fields can take on a variety of field values.</li>
 * <li>{@link FieldValue} - The API presentation of a field value. Provides methods
 * to access the field as a wide range of data types. Many types are automatically
 * converted to other types on demand. For example, an {@link DataType#INT32} field
 * will be converted on demand to a string or a float, say. A separate field value
 * implementation exists for each type. For structured fields (arrays, maps) each
 * data source may provide a custom implementation.</li>
 * <li><i>Something</i>Conversions - Utility methods to convert a given type to
 * other types. Trivial conversions (from short to int, say) are done inline.
 * Non-trivial conversions are done in the Conversions classes.<li>
 * <li>{@link FieldValueAccessor} - Acts as a bridge between the generic
 * {@link FieldValue} and the source-specific implementation of the value. This
 * package provides a number of "generic" implementations for values stored in
 * Java objects.</li>
 * <li>{@link DataDef} - A utility to create the field value, field container
 * and other plumbing given information about a field and the field value
 * accessor.</li>
 * </ul>
 * <p>
 * The composition system avoids messy switch statements otherwise required
 * to handle the null/non-null, variant/non-variant, scalar/array cases.
 * The design also ensures that no garbage is created in tuple and field
 * access.
 * <p>
 * The bulk of the code required to wire Jig up to a data source comes from four
 * tasks:
 * <ul>
 * <li>Parse out a Jig schema from the schema information available from the data
 * source. The "extras" sources (used for testing) infer schema from Java objects
 * or from a JSON file. The Direct implementation infers schema from a Drill
 * record batch schema. The client implementation reads the schema from the
 * wire protocol.<li>
 * <li>Build accessors to access data. Simple fields need only scalar accessors
 * (int, String, etc.). Structured objects often require extra work, such as
 * converting the underlying structure to a Java object, which is then accessed
 * using the "generic" Java object accessors. In some cases (such as the test
 * Java array data source), the implementation may use a chain of accessors to
 * convert data. The chain approach enables reuse: differences (between, say,
 * a direct value and a value in a map) can be abstracted out into a lower-level
 * accessor.</li>
 * <li>Construct the tuple using a {@link DataDef} set, schema information and
 * the implementation-specific accessors.</li>
 * <li>Access the implementation-specific tuples using the Jig API to iterate over
 * tuple sets, and to access each field in the tuple. For the most part, tuple-level
 * field access is automatic based on the accessors and field values created
 * above.</li>
 * </ul>
 */
package org.apache.drill.jig.types;