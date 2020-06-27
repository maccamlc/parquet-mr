/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.schema;

import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestConversionPatterns {

  @Test public void mapTypeShouldNotUseMapKeyValue() {
    final GroupType mapType = ConversionPatterns.mapType(Type.Repetition.OPTIONAL, "testMap",
      new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveTypeName.BINARY, "key", LogicalTypeAnnotation.stringType()),
      new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveTypeName.INT32, "value",
        LogicalTypeAnnotation.intType(32, true)));

    assertNotNull(mapType);
    assertEquals("testMap", mapType.getName());
    assertEquals(LogicalTypeAnnotation.mapType(), mapType.getLogicalTypeAnnotation());
    assertEquals(OriginalType.MAP, mapType.getOriginalType());
    assertEquals(Type.Repetition.OPTIONAL, mapType.getRepetition());
    assertEquals(1, mapType.getFieldCount());
    assertEquals("key_value", mapType.getFieldName(0));

    final Type keyValueType = mapType.getType(0);
    assertEquals("key_value", keyValueType.getName());
    assertEquals(Type.Repetition.REPEATED, keyValueType.getRepetition());
    assertNull(keyValueType.getLogicalTypeAnnotation());
    assertNull(keyValueType.getOriginalType());
    assertTrue("Instanceof GroupType", keyValueType instanceof GroupType);
    assertEquals(2, ((GroupType) keyValueType).getFieldCount());
    assertEquals("key", ((GroupType) keyValueType).getFieldName(0));
    assertEquals("value", ((GroupType) keyValueType).getFieldName(1));

    final Type keyType = ((GroupType) keyValueType).getType(0);
    assertEquals("key", keyType.getName());
    assertEquals(Type.Repetition.REQUIRED, keyType.getRepetition());
    assertEquals(LogicalTypeAnnotation.stringType(), keyType.getLogicalTypeAnnotation());
    assertEquals(OriginalType.UTF8, keyType.getOriginalType());
    assertTrue("Instanceof PrimitiveType", keyType instanceof PrimitiveType);

    final Type valueType = ((GroupType) keyValueType).getType(1);
    assertEquals("value", valueType.getName());
    assertEquals(Type.Repetition.REQUIRED, valueType.getRepetition());
    assertEquals(LogicalTypeAnnotation.intType(32, true), valueType.getLogicalTypeAnnotation());
    assertEquals(OriginalType.INT_32, valueType.getOriginalType());
    assertTrue("Instanceof PrimitiveType", valueType instanceof PrimitiveType);
  }

}
