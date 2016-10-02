/**
 * Provides a set of classes that work with field values of multiple types.
 * The field value system is built using composition using several components:
 * <ul>
 * <li>Field Schema: Describes the field (name, type, nullable).</li>
 * <li>Field Value: Client-visible API to access field values. Handles
 * conversions between storage types and retrieval types. (Lets the client
 * access an short as a long, or a long as a decimal, say.</li>
 * <li>Field Value Container: Handles cases where the field value returned
 * for a field varies depending on attributes of a specific field value.
 * For example null or non-null, a variant field type, or an indexed
 * field.</li>
 * <li>Field Accessor: Handles the direct interface to the backing data.
 * Determines if that value is null, the type of the data (when needed),
 * and retrieves the data using the matching primitive type.</li>
 * </ul>
 * <p>
 * The composition system avoids messy switch statements otherwise required
 * to handle the null/non-null, variant/non-variant, scalar/array cases.
 * The design also ensures that no garbage is created in tuple and field
 * access.
 */

package org.apache.drill.jig.types;