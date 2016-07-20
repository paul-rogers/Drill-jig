/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright (c) 2013 Oracle and/or its affiliates. All rights reserved.
 *
 * The contents of this file are subject to the terms of either the GNU
 * General Public License Version 2 only ("GPL") or the Common Development
 * and Distribution License("CDDL") (collectively, the "License").  You
 * may not use this file except in compliance with the License.  You can
 * obtain a copy of the License at
 * https://glassfish.dev.java.net/public/CDDL+GPL_1_1.html
 * or packager/legal/LICENSE.txt.  See the License for the specific
 * language governing permissions and limitations under the License.
 *
 * When distributing the software, include this License Header Notice in each
 * file and include the License file at packager/legal/LICENSE.txt.
 *
 * GPL Classpath Exception:
 * Oracle designates this particular file as subject to the "Classpath"
 * exception as provided by Oracle in the GPL Version 2 section of the License
 * file that accompanied this code.
 *
 * Modifications:
 * If applicable, add the following below the License Header, with the fields
 * enclosed by brackets [] replaced by your own identifying information:
 * "Portions Copyright [year] [name of copyright owner]"
 *
 * Contributor(s):
 * If you wish your version of this file to be governed by only the CDDL or
 * only the GPL Version 2, indicate your decision by adding "[Contributor]
 * elects to include this software in this distribution under the [CDDL or GPL
 * Version 2] license."  If you don't indicate a single choice of license, a
 * recipient has the option to distribute your version of this file under
 * either the CDDL, the GPL Version 2 or to extend the choice of license to
 * its licensees as provided above.  However, if you add GPL Version 2 code
 * and therefore, elected the GPL Version 2 license, then the option applies
 * only if the new code is made subject to such option by the copyright
 * holder.
 */

package org.glassfish.json;

import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.Charset;

import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonException;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;
import javax.json.JsonStructure;
import javax.json.JsonValue;
import javax.json.stream.JsonParser;

import org.glassfish.json.api.BufferPool;

/**
 * JsonReader impl using parser and builders.
 * <p>
 * Modified to allow reading multiple JSON objects per input file.
 *
 * @author Jitendra Kotamraju
 */

public class CustomJsonReader implements JsonReader {
  private final CustomJsonParser parser;
  private boolean readDone;
    private final BufferPool bufferPool;

    public CustomJsonReader(Reader reader) {
        this.bufferPool = new BufferPoolImpl();
        parser = new CustomJsonParser(reader, bufferPool);
    }

    public CustomJsonReader(InputStream in) {
        this.bufferPool = new BufferPoolImpl();
        parser = new CustomJsonParser(in, bufferPool);
    }

//    CustomJsonReader(InputStream in, Charset charset, BufferPool bufferPool) {
//        parser = new JsonParserImpl(in, charset, bufferPool);
//        this.bufferPool = bufferPool;
//    }

    @Override
    public JsonStructure read() {
        if (readDone) {
            return null;
        }
        parser.reset( );
        if (parser.hasNext()) {
            JsonParser.Event e = parser.next();
            if (e == null) {
              readDone = true;
              return null;
            }
            if (e == JsonParser.Event.START_ARRAY) {
                return readArray(new JsonArrayBuilderImpl(bufferPool));
            } else if (e == JsonParser.Event.START_OBJECT) {
                return readObject(new JsonObjectBuilderImpl(bufferPool));
            } else {
              throw new JsonException( "Unexpected token" );
            }
        }
        readDone = true;
        return null;
    }

    @Override
    public JsonObject readObject() {
        if (readDone) {
            return null;
        }
        parser.reset();
        if (parser.hasNext()) {
            JsonParser.Event e = parser.next();
            if (e == JsonParser.Event.START_OBJECT) {
                return readObject(new JsonObjectBuilderImpl(bufferPool));
            } else if (e == JsonParser.Event.START_ARRAY) {
                throw new JsonException(JsonMessages.READER_EXPECTED_OBJECT_GOT_ARRAY());
            }
        }
        readDone = true;
        return null;
    }

    @Override
    public JsonArray readArray() {
      if (readDone) {
        return null;
    }
        if (parser.hasNext()) {
            JsonParser.Event e = parser.next();
            if (e == JsonParser.Event.START_ARRAY) {
                return readArray(new JsonArrayBuilderImpl(bufferPool));
            } else if (e == JsonParser.Event.START_OBJECT) {
                throw new JsonException(JsonMessages.READER_EXPECTED_ARRAY_GOT_OBJECT());
            }
        }
        readDone = true;
        return null;
    }

    @Override
    public void close() {
        readDone = true;
        parser.close();
    }

    private JsonArray readArray(JsonArrayBuilder builder) {
        while(parser.hasNext()) {
            JsonParser.Event e = parser.next();
            switch (e) {
                case START_ARRAY:
                    JsonArray array = readArray(new JsonArrayBuilderImpl(bufferPool));
                    builder.add(array);
                    break;
                case START_OBJECT:
                    JsonObject object = readObject(new JsonObjectBuilderImpl(bufferPool));
                    builder.add(object);
                    break;
                case VALUE_STRING:
                    builder.add(parser.getString());
                    break;
                case VALUE_NUMBER:
                    if (parser.isDefinitelyInt()) {
                        builder.add(parser.getInt());
                    } else {
                        builder.add(parser.getBigDecimal());
                    }
                    break;
                case VALUE_TRUE:
                    builder.add(JsonValue.TRUE);
                    break;
                case VALUE_FALSE:
                    builder.add(JsonValue.FALSE);
                    break;
                case VALUE_NULL:
                    builder.addNull();
                    break;
                case END_ARRAY:
                    return builder.build();
                default:
                    throw new JsonException("Internal Error");
            }
        }
        throw new JsonException("Internal Error");
    }

    private JsonObject readObject(JsonObjectBuilder builder) {
        String key = null;
        while(parser.hasNext()) {
            JsonParser.Event e = parser .next();
            switch (e) {
                case START_ARRAY:
                    JsonArray array = readArray(new JsonArrayBuilderImpl(bufferPool));
                    builder.add(key, array);
                    break;
                case START_OBJECT:
                    JsonObject object = readObject(new JsonObjectBuilderImpl(bufferPool));
                    builder.add(key, object);
                    break;
                case KEY_NAME:
                    key = parser.getString();
                    break;
                case VALUE_STRING:
                    builder.add(key, parser.getString());
                    break;
                case VALUE_NUMBER:
                    if (parser.isDefinitelyInt()) {
                        builder.add(key, parser.getInt());
                    } else {
                        builder.add(key, parser.getBigDecimal());
                    }
                    break;
                case VALUE_TRUE:
                    builder.add(key, JsonValue.TRUE);
                    break;
                case VALUE_FALSE:
                    builder.add(key, JsonValue.FALSE);
                    break;
                case VALUE_NULL:
                    builder.addNull(key);
                    break;
                case END_OBJECT:
                    return builder.build();
                default:
                    throw new JsonException("Internal Error");
            }
        }
        throw new JsonException("Internal Error");
    }

}
