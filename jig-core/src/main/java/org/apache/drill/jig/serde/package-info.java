/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Serializes & deserializes a tuple set that represents a run of tuples with
 * identical schema.
 * <p>
 * The implementation here allows serializing tuples via the tuple and field
 * API. Deserialization happens in place, presented though the same API.
 * 
 * <h4>Serialization Format</h4>
 * The serialization format is the following:
 * <pre>
 * Message 1: [schema] (as a protobuf)
 * Message 2: [msg length][tuple 1][tuple 2]...
 * Message 3: [msg length][tuple i][tuple i+1] ...</pre>
 * <p>
 * The first message provides the schema (only) so that the client can prepare its
 * deserializer. Subsequent messages include n tuples in a message limited by size.
 * The message field lets the client know how many bytes to read for each message.
 * <h4>Tuple</h4>
 * Each tuple is serialized as:
 * <pre>
 * [length][header][fields]</pre>
 * <p>
 * A length value of 0 means the end of the tuple set (EOF).
 * <h4>Tuple Header</h4>
 * The header comprises an array of two-bit markers for each field:
 * <pre>
 * aabb ccdd | eeff gghh | ... | zz00 0000</pre>
 * <p>
 * Unused bit fields are set to zero. For each field the two bit
 * fields are:
 * <pre>
 * [null flag][repeat flag]</pre>
 * <p>
 * The null flag indicates that the field is null (and so no value appears
 * in the serialized tuple.) The repeat flag means that the value is the same
 * as the previous tuple and so the field is again omitted. Because of the repeat
 * flag, there is no advantage of omitting bits for non-nullable fields.
 * <h4>Field Data</h4>
 * Fields are either fixed-length or variable length. Numeric fields Big Endian.
 * <ul>
 * <li>BOOLEAN: 1-byte (true=1, false=0)</li>
 * <li>INT, LONG: variable length, encoded as below.</li>
 * <li>DOUBLE: 8-bytes</li>
 * <li>BIG_DECIMAL: converted to String, then written as STRING.</li>
 * <li>STRING: (variable-length) length, followed by characters encoded as UTF-8.</li>
 * <li>NULL: Never written, encoded in the null bit for the field.</li>
 * <li>VARIANT: Encoded as a 1-byte type, followed by the data for that type as described
 * above.</li>
 * </ul>
 * <p>
 * Numbers are encoded as:
 * The length is a compressed integer defined in this format:
 * <pre>
 * 0xxx xxxx (1 byte)
 * 10xx xxxx | B (2 bytes)
 * 110x xxxx | BBB (4 bytes)
 * 1110 xxxx | BBB BBBB (8 bytes)
 * 1111 0000 | BBBB BBBB (9 bytes)</pre>
 * <p>
 * Where 0, 1 and x are bits, B is a byte. Negative numbers are encoded by rotating
 * the bits upward by one, wrapping the sign bit into the low-order bit. This ensues
 * efficient encoding of both negative and positive values.
 * <p>
 * String fields are variable, encoded as:
 * <pre>
 * [length][bytes, encoded as UTF-8]</pre>
 * Size is encoded in the compressed integer format described above.
 * <h4>Lists</h4>
 * Arrays are encoded as:
 * <pre>
 * [size][count][item 0][item 1]...</pre>
 * Size is the entire field length, in bytes, encoded as a 4-byte integer.
 * <p>
 * Count is encoded in compressed integer format.
 * <p>Items are encoded as described for fields. If an array is of a fixed
 * type, then just the field value is written. If the array allows variant
 * members, then the type is written, then the value. This format handles
 * lists of lists.
 * <h4>Maps</h4>
 * Maps are written as:<br>
 * <pre>[size][count][key 0][type 0][value 0]...</pre>
 * <p>
 * The size and count are the same as for a List. The key is always a string.
 * The value is a variant and is encoded as a type/value pair.
 * 
 */

package org.apache.drill.jig.serde;