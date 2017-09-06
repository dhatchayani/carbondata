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

package org.apache.carbondata.spark.testsuite.directdictionary

import java.sql.Timestamp

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.keygenerator.directdictionary.timestamp.TimeStampGranularityConstants
import org.apache.carbondata.core.util.CarbonProperties

class TimestampDataTypeCutoffTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll: Unit = {
    CarbonProperties.getInstance()
      .addProperty(TimeStampGranularityConstants.CARBON_CUTOFF_TIMESTAMP, "1500-01-01 00:00:00")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy-MM-dd HH:mm:ss")
  }

  test("test cutoff date beyond 1970") {
    sql("create table directDictionaryTable_cutoff (name string, dob timestamp) " +
        "stored by 'carbondata' TBLPROPERTIES ('DICTIONARY_INCLUDE'='dob')")
    var csvFilePath = s"$resourcesPath/datasample1.csv"
    sql("LOAD DATA LOCAL INPATH 'D:/carbondata/integration/spark-common-test/src/test/resources/datasample1.csv' into table directDictionaryTable_cutoff")
    try{
      csvFilePath = s"$resourcesPath/datasample2.csv"
      sql("LOAD DATA LOCAL INPATH 'D:/carbondata/integration/spark-common-test/src/test/resources/datasample2.csv' into table directDictionaryTable_cutoff" +
          " OPTIONS('BAD_RECORDS_ACTION'='FAIL')")
    } catch {
      case e: Exception =>
        assert(e.getMessage.contains("Data load failed due to bad record"))
    }
    checkAnswer(
      sql("select * from directDictionaryTable_cutoff"),
      Row("aaa", Timestamp.valueOf("1500-01-01 00:00:01.0"))
    )
    CarbonProperties.getInstance()
      .addProperty(TimeStampGranularityConstants.CARBON_CUTOFF_TIMESTAMP, "2000-12-13 02:10:00.0")
  }

  override def afterAll {
    sql("drop table if exists directDictionaryTable_cutoff")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
  }
}
