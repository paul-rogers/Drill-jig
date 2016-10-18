/**
 * Defines the Jig deserialization mechanism. The Jig server sends batches of
 * tuples to the client, which receives them in the form of a byte buffer.
 * The deserializer reads tuple values directly from the byte buffer using
 * a set of deserializer accessors. Maps and lists are materialized as Java
 * Map and array objects. Java object accessors then present these values
 * to the Jig field value API.
 */

package org.apache.drill.jig.serde.deserializer;

