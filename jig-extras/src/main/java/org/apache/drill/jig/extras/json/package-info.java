/**
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
 * Simple, JSON-based result set implementation. Allows
 * testing of Jig components without connecting to Drill.
 * <p>
 * Translates a set of JSON objects to a Jig schema and implementation
 * in a number of steps:
 * <ul>
 * <li>{@link ObjectParser} - scan a set of JSON objects to extract the
 * common schemas. Detect unsupported structures, such as the same field
 * being both a scalar and a map/array.</li>
 * <li>{@link SchemaBuilder} - translate the parsed object schema into a jig
 * schema, optionally flattening child objects.</li>
 * <li>{@link DefinitionBuilder} - translate the above intermediate schema
 * into a set of accessors that read actual JSON values at runtime.</li>
 * <li>Runtime builder - Create Jig field values and associated objects
 * to present the accessors to the application.</li>
 * </ul>
 */

package org.apache.drill.jig.extras.json;