/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.core.scan.result.vector;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.DataTypeUtil;

public class DimensionDataVectorProcessor {


  /**
   * Put the data to vector
   *
   * @param vector
   * @param value
   * @param vectorRow
   * @param length
   */
  public static void putDataToVector(CarbonColumnVector vector, byte[] value, int vectorRow,
      int length) {
    DataType dt = vector.getType();
    if ((!(dt == DataTypes.STRING) && length == 0) || ByteUtil.UnsafeComparer.INSTANCE
        .equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY, 0,
            CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY.length, value, 0, length)) {
      vector.putNull(vectorRow);
    } else {
      if (dt == DataTypes.STRING) {
        vector.putBytes(vectorRow, 0, length, value);
      } else if (dt == DataTypes.BOOLEAN) {
        vector.putBoolean(vectorRow, ByteUtil.toBoolean(value[0]));
      } else if (dt == DataTypes.SHORT) {
        vector.putShort(vectorRow, ByteUtil.toXorShort(value, 0, length));
      } else if (dt == DataTypes.INT) {
        vector.putInt(vectorRow, ByteUtil.toXorInt(value, 0, length));
      } else if (dt == DataTypes.LONG) {
        vector.putLong(vectorRow, DataTypeUtil
            .getDataBasedOnRestructuredDataType(value, vector.getBlockDataType(), 0, length));
      } else if (dt == DataTypes.TIMESTAMP) {
        vector.putLong(vectorRow, ByteUtil.toXorLong(value, 0, length) * 1000L);
      }
    }
  }
}
