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

package org.apache.carbondata.core.datastore.page.encoding.dimension.legacy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.datastore.columnar.PageIndexGenerator;
import org.apache.carbondata.core.datastore.columnar.PrimitivePageIndexGenerator;
import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoder;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.core.datastore.page.encoding.DefaultEncodingFactory;
import org.apache.carbondata.core.datastore.page.encoding.EncodedColumnPage;
import org.apache.carbondata.core.datastore.page.statistics.PrimitivePageStatsCollector;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.format.Encoding;

public class PrimitiveTypeColumnCodec extends IndexStorageCodec {

  private EncodedColumnPage encode;

  private ColumnPageEncoder encoder;

  private ColumnPage adaptivePage;

  public PrimitiveTypeColumnCodec(boolean isSort, boolean isInvertedIndex, Compressor compressor) {
    super(isSort, isInvertedIndex, compressor);
  }

  @Override public String getName() {
    return "PrimitiveTypeColumnCodec";
  }

  @Override public ColumnPageEncoder createEncoder(Map<String, String> parameter) {
    return new IndexStorageEncoder() {

      @Override protected void encodeIndexStorage(ColumnPage input) {
        PageIndexGenerator<Object[]> pageIndexGenerator;
        Object[] data = input.getPageBasedOnDataType();
        pageIndexGenerator =
            new PrimitivePageIndexGenerator(data, isSort, input.getDataType(), true);
        if (isInvertedIndex) {
          try {
            adaptivePage =
                ColumnPage.newPage(input.getColumnSpec(), input.getDataType(), input.getPageSize());
            adaptivePage.setStatsCollector(
                PrimitivePageStatsCollector.newInstance(input.getColumnSpec().getSchemaDataType()));
          } catch (MemoryException e) {
            throw new RuntimeException(e);
          }
          Object[] dataPage = pageIndexGenerator.getDataPage();
          for (int i = 0; i < dataPage.length; i++) {
            adaptivePage.putData(i, dataPage[i]);
          }
        } else {
          adaptivePage = input;
        }
        try {
          encoder =
              DefaultEncodingFactory.createEncoderForMeasureOrNoDictionaryPrimitive(adaptivePage);
          encode = encoder.encode(adaptivePage);
        } catch (IOException | MemoryException e) {
          throw new RuntimeException(e);
        }
        super.compressedDataPage = encode.getEncodedData().array();
        super.pageIndexGenerator = pageIndexGenerator;
      }

      @Override protected List<Encoding> getEncodingList() {
        List<Encoding> encodings = new ArrayList<>();
        List<Encoding> encoders = encode.getPageMetadata().getEncoders();
        encodings.addAll(encoders);
        if (pageIndexGenerator.getRowIdPageLengthInBytes() > 0) {
          encodings.add(Encoding.INVERTED_INDEX);
        }
        if (pageIndexGenerator.getDataRlePageLengthInBytes() > 0) {
          encodings.add(Encoding.RLE);
        }
        return encodings;
      }

      @Override protected ColumnPageEncoderMeta getEncoderMeta(ColumnPage inputPage) {
        if (CarbonUtil.isEncodedWithMeta(encode.getPageMetadata().getEncoders()) && null != encoder
            .getTargetDataType(adaptivePage)) {
          return new ColumnPageEncoderMeta(inputPage.getColumnSpec(),
              encoder.getTargetDataType(adaptivePage), encode.getStats(), compressor.getName());
        }
        return null;
      }
    };
  }
}
