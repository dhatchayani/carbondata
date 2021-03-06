/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.carbondata.spark.testsuite.singlevaluerow

import java.io.File

import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.scalatest.BeforeAndAfterAll

class TestEmptyRows extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop table if exists emptyRowCarbonTable")
    sql("drop table if exists emptyRowHiveTable")
    //eid,ename,sal,presal,comm,deptno,Desc
    sql(
      "create table if not exists emptyRowCarbonTable (eid int,ename String,sal decimal,presal " +
        "decimal,comm decimal" +
        "(37,37),deptno decimal(18,2),Desc String) STORED BY 'org.apache.carbondata.format'"
    )
    sql(
      "create table if not exists emptyRowHiveTable(eid int,ename String,sal decimal,presal " +
        "decimal,comm " +
        "decimal(37,37),deptno decimal(18,2),Desc String)row format delimited fields " +
        "terminated by ','"
    )
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/mm/dd")
    val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
      .getCanonicalPath
    val csvFilePath = currentDirectory + "/src/test/resources/emptyrow/emptyRows.csv"

    sql(
      s"""LOAD DATA INPATH '$csvFilePath' INTO table emptyRowCarbonTable OPTIONS('DELIMITER'=',','QUOTECHAR'='"','FILEHEADER'='eid,ename,sal,presal,comm,deptno,Desc')""")

    sql(
      "LOAD DATA LOCAL INPATH '" + csvFilePath + "' into table " +
        "emptyRowHiveTable"
    );
  }

  test("select eid from table") {
    checkAnswer(
      sql("select eid from emptyRowCarbonTable"),
      sql("select eid from emptyRowHiveTable")
    )
  }

  test("select Desc from emptyRowTable") {
    checkAnswer(
      sql("select Desc from emptyRowCarbonTable"),
      sql("select Desc from emptyRowHiveTable")
    )
  }

  override def afterAll {
    sql("drop table emptyRowCarbonTable")
    sql("drop table emptyRowHiveTable")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
  }
}
