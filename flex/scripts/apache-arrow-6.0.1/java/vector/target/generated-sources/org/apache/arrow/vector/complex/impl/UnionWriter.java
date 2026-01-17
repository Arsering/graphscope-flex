

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
package org.apache.arrow.vector.complex.impl;


import static org.apache.arrow.util.Preconditions.checkArgument;
import static org.apache.arrow.util.Preconditions.checkState;

import com.google.flatbuffers.FlatBufferBuilder;

import org.apache.arrow.memory.*;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.Types.*;
import org.apache.arrow.vector.types.pojo.*;
import org.apache.arrow.vector.types.pojo.ArrowType.*;
import org.apache.arrow.vector.types.*;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.holders.*;
import org.apache.arrow.vector.util.*;
import org.apache.arrow.vector.complex.*;
import org.apache.arrow.vector.complex.reader.*;
import org.apache.arrow.vector.complex.impl.*;
import org.apache.arrow.vector.complex.writer.*;
import org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.arrow.vector.util.JsonStringArrayList;

import java.util.Arrays;
import java.util.Random;
import java.util.List;

import java.io.Closeable;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.ZonedDateTime;


import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.types.Types.MinorType;

/*
 * This class is generated using freemarker and the UnionWriter.java template.
 */
@SuppressWarnings("unused")
public class UnionWriter extends AbstractFieldWriter implements FieldWriter {

  UnionVector data;
  private StructWriter structWriter;
  private UnionListWriter listWriter;
  private UnionMapWriter mapWriter;
  private List<BaseWriter> writers = new java.util.ArrayList<>();
  private final NullableStructWriterFactory nullableStructWriterFactory;

  public UnionWriter(UnionVector vector) {
    this(vector, NullableStructWriterFactory.getNullableStructWriterFactoryInstance());
  }

  public UnionWriter(UnionVector vector, NullableStructWriterFactory nullableStructWriterFactory) {
    data = vector;
    this.nullableStructWriterFactory = nullableStructWriterFactory;
  }

  @Override
  public void setPosition(int index) {
    super.setPosition(index);
    for (BaseWriter writer : writers) {
      writer.setPosition(index);
    }
  }


  @Override
  public void start() {
    data.setType(idx(), MinorType.STRUCT);
    getStructWriter().start();
  }

  @Override
  public void end() {
    getStructWriter().end();
  }

  @Override
  public void startList() {
    getListWriter().startList();
    data.setType(idx(), MinorType.LIST);
  }

  @Override
  public void endList() {
    getListWriter().endList();
  }

  @Override
  public void startMap() {
    getMapWriter().startMap();
    data.setType(idx(), MinorType.MAP);
  }

  @Override
  public void endMap() {
    getMapWriter().endMap();
  }

  @Override
  public void startEntry() {
    getMapWriter().startEntry();
  }

  @Override
  public MapWriter key() {
    return getMapWriter().key();
  }

  @Override
  public MapWriter value() {
    return getMapWriter().value();
  }

  @Override
  public void endEntry() {
    getMapWriter().endEntry();
  }

  private StructWriter getStructWriter() {
    if (structWriter == null) {
      structWriter = nullableStructWriterFactory.build(data.getStruct());
      structWriter.setPosition(idx());
      writers.add(structWriter);
    }
    return structWriter;
  }

  public StructWriter asStruct() {
    data.setType(idx(), MinorType.STRUCT);
    return getStructWriter();
  }

  private ListWriter getListWriter() {
    if (listWriter == null) {
      listWriter = new UnionListWriter(data.getList(), nullableStructWriterFactory);
      listWriter.setPosition(idx());
      writers.add(listWriter);
    }
    return listWriter;
  }

  public ListWriter asList() {
    data.setType(idx(), MinorType.LIST);
    return getListWriter();
  }

  private MapWriter getMapWriter() {
    if (mapWriter == null) {
      mapWriter = new UnionMapWriter(data.getMap(new ArrowType.Map(false)));
      mapWriter.setPosition(idx());
      writers.add(mapWriter);
    }
    return mapWriter;
  }

  private MapWriter getMapWriter(ArrowType arrowType) {
    if (mapWriter == null) {
      mapWriter = new UnionMapWriter(data.getMap(arrowType));
      mapWriter.setPosition(idx());
      writers.add(mapWriter);
    }
    return mapWriter;
  }

  public MapWriter asMap(ArrowType arrowType) {
    data.setType(idx(), MinorType.MAP);
    return getMapWriter(arrowType);
  }

  BaseWriter getWriter(MinorType minorType) {
    return getWriter(minorType, null);
  }

  BaseWriter getWriter(MinorType minorType, ArrowType arrowType) {
    switch (minorType) {
    case STRUCT:
      return getStructWriter();
    case LIST:
      return getListWriter();
    case MAP:
      return getMapWriter(arrowType);
    case TINYINT:
      return getTinyIntWriter();
    case UINT1:
      return getUInt1Writer();
    case UINT2:
      return getUInt2Writer();
    case SMALLINT:
      return getSmallIntWriter();
    case INT:
      return getIntWriter();
    case UINT4:
      return getUInt4Writer();
    case FLOAT4:
      return getFloat4Writer();
    case DATEDAY:
      return getDateDayWriter();
    case INTERVALYEAR:
      return getIntervalYearWriter();
    case TIMESEC:
      return getTimeSecWriter();
    case TIMEMILLI:
      return getTimeMilliWriter();
    case BIGINT:
      return getBigIntWriter();
    case UINT8:
      return getUInt8Writer();
    case FLOAT8:
      return getFloat8Writer();
    case DATEMILLI:
      return getDateMilliWriter();
    case TIMESTAMPSEC:
      return getTimeStampSecWriter();
    case TIMESTAMPMILLI:
      return getTimeStampMilliWriter();
    case TIMESTAMPMICRO:
      return getTimeStampMicroWriter();
    case TIMESTAMPNANO:
      return getTimeStampNanoWriter();
    case TIMEMICRO:
      return getTimeMicroWriter();
    case TIMENANO:
      return getTimeNanoWriter();
    case INTERVALDAY:
      return getIntervalDayWriter();
    case INTERVALMONTHDAYNANO:
      return getIntervalMonthDayNanoWriter();
    case DECIMAL256:
      return getDecimal256Writer(arrowType);
    case DECIMAL:
      return getDecimalWriter(arrowType);
    case VARBINARY:
      return getVarBinaryWriter();
    case VARCHAR:
      return getVarCharWriter();
    case LARGEVARCHAR:
      return getLargeVarCharWriter();
    case LARGEVARBINARY:
      return getLargeVarBinaryWriter();
    case BIT:
      return getBitWriter();
    default:
      throw new UnsupportedOperationException("Unknown type: " + minorType);
    }
  }

  private TinyIntWriter tinyIntWriter;

  private TinyIntWriter getTinyIntWriter() {
    if (tinyIntWriter == null) {
      tinyIntWriter = new TinyIntWriterImpl(data.getTinyIntVector());
      tinyIntWriter.setPosition(idx());
      writers.add(tinyIntWriter);
    }
    return tinyIntWriter;
  }

  public TinyIntWriter asTinyInt() {
    data.setType(idx(), MinorType.TINYINT);
    return getTinyIntWriter();
  }

  @Override
  public void write(TinyIntHolder holder) {
    data.setType(idx(), MinorType.TINYINT);
    
    getTinyIntWriter().setPosition(idx());
    getTinyIntWriter().writeTinyInt(holder.value);
  }

  public void writeTinyInt(byte value) {
    data.setType(idx(), MinorType.TINYINT);
    getTinyIntWriter().setPosition(idx());
    getTinyIntWriter().writeTinyInt(value);
  }

  private UInt1Writer uInt1Writer;

  private UInt1Writer getUInt1Writer() {
    if (uInt1Writer == null) {
      uInt1Writer = new UInt1WriterImpl(data.getUInt1Vector());
      uInt1Writer.setPosition(idx());
      writers.add(uInt1Writer);
    }
    return uInt1Writer;
  }

  public UInt1Writer asUInt1() {
    data.setType(idx(), MinorType.UINT1);
    return getUInt1Writer();
  }

  @Override
  public void write(UInt1Holder holder) {
    data.setType(idx(), MinorType.UINT1);
    
    getUInt1Writer().setPosition(idx());
    getUInt1Writer().writeUInt1(holder.value);
  }

  public void writeUInt1(byte value) {
    data.setType(idx(), MinorType.UINT1);
    getUInt1Writer().setPosition(idx());
    getUInt1Writer().writeUInt1(value);
  }

  private UInt2Writer uInt2Writer;

  private UInt2Writer getUInt2Writer() {
    if (uInt2Writer == null) {
      uInt2Writer = new UInt2WriterImpl(data.getUInt2Vector());
      uInt2Writer.setPosition(idx());
      writers.add(uInt2Writer);
    }
    return uInt2Writer;
  }

  public UInt2Writer asUInt2() {
    data.setType(idx(), MinorType.UINT2);
    return getUInt2Writer();
  }

  @Override
  public void write(UInt2Holder holder) {
    data.setType(idx(), MinorType.UINT2);
    
    getUInt2Writer().setPosition(idx());
    getUInt2Writer().writeUInt2(holder.value);
  }

  public void writeUInt2(char value) {
    data.setType(idx(), MinorType.UINT2);
    getUInt2Writer().setPosition(idx());
    getUInt2Writer().writeUInt2(value);
  }

  private SmallIntWriter smallIntWriter;

  private SmallIntWriter getSmallIntWriter() {
    if (smallIntWriter == null) {
      smallIntWriter = new SmallIntWriterImpl(data.getSmallIntVector());
      smallIntWriter.setPosition(idx());
      writers.add(smallIntWriter);
    }
    return smallIntWriter;
  }

  public SmallIntWriter asSmallInt() {
    data.setType(idx(), MinorType.SMALLINT);
    return getSmallIntWriter();
  }

  @Override
  public void write(SmallIntHolder holder) {
    data.setType(idx(), MinorType.SMALLINT);
    
    getSmallIntWriter().setPosition(idx());
    getSmallIntWriter().writeSmallInt(holder.value);
  }

  public void writeSmallInt(short value) {
    data.setType(idx(), MinorType.SMALLINT);
    getSmallIntWriter().setPosition(idx());
    getSmallIntWriter().writeSmallInt(value);
  }

  private IntWriter intWriter;

  private IntWriter getIntWriter() {
    if (intWriter == null) {
      intWriter = new IntWriterImpl(data.getIntVector());
      intWriter.setPosition(idx());
      writers.add(intWriter);
    }
    return intWriter;
  }

  public IntWriter asInt() {
    data.setType(idx(), MinorType.INT);
    return getIntWriter();
  }

  @Override
  public void write(IntHolder holder) {
    data.setType(idx(), MinorType.INT);
    
    getIntWriter().setPosition(idx());
    getIntWriter().writeInt(holder.value);
  }

  public void writeInt(int value) {
    data.setType(idx(), MinorType.INT);
    getIntWriter().setPosition(idx());
    getIntWriter().writeInt(value);
  }

  private UInt4Writer uInt4Writer;

  private UInt4Writer getUInt4Writer() {
    if (uInt4Writer == null) {
      uInt4Writer = new UInt4WriterImpl(data.getUInt4Vector());
      uInt4Writer.setPosition(idx());
      writers.add(uInt4Writer);
    }
    return uInt4Writer;
  }

  public UInt4Writer asUInt4() {
    data.setType(idx(), MinorType.UINT4);
    return getUInt4Writer();
  }

  @Override
  public void write(UInt4Holder holder) {
    data.setType(idx(), MinorType.UINT4);
    
    getUInt4Writer().setPosition(idx());
    getUInt4Writer().writeUInt4(holder.value);
  }

  public void writeUInt4(int value) {
    data.setType(idx(), MinorType.UINT4);
    getUInt4Writer().setPosition(idx());
    getUInt4Writer().writeUInt4(value);
  }

  private Float4Writer float4Writer;

  private Float4Writer getFloat4Writer() {
    if (float4Writer == null) {
      float4Writer = new Float4WriterImpl(data.getFloat4Vector());
      float4Writer.setPosition(idx());
      writers.add(float4Writer);
    }
    return float4Writer;
  }

  public Float4Writer asFloat4() {
    data.setType(idx(), MinorType.FLOAT4);
    return getFloat4Writer();
  }

  @Override
  public void write(Float4Holder holder) {
    data.setType(idx(), MinorType.FLOAT4);
    
    getFloat4Writer().setPosition(idx());
    getFloat4Writer().writeFloat4(holder.value);
  }

  public void writeFloat4(float value) {
    data.setType(idx(), MinorType.FLOAT4);
    getFloat4Writer().setPosition(idx());
    getFloat4Writer().writeFloat4(value);
  }

  private DateDayWriter dateDayWriter;

  private DateDayWriter getDateDayWriter() {
    if (dateDayWriter == null) {
      dateDayWriter = new DateDayWriterImpl(data.getDateDayVector());
      dateDayWriter.setPosition(idx());
      writers.add(dateDayWriter);
    }
    return dateDayWriter;
  }

  public DateDayWriter asDateDay() {
    data.setType(idx(), MinorType.DATEDAY);
    return getDateDayWriter();
  }

  @Override
  public void write(DateDayHolder holder) {
    data.setType(idx(), MinorType.DATEDAY);
    
    getDateDayWriter().setPosition(idx());
    getDateDayWriter().writeDateDay(holder.value);
  }

  public void writeDateDay(int value) {
    data.setType(idx(), MinorType.DATEDAY);
    getDateDayWriter().setPosition(idx());
    getDateDayWriter().writeDateDay(value);
  }

  private IntervalYearWriter intervalYearWriter;

  private IntervalYearWriter getIntervalYearWriter() {
    if (intervalYearWriter == null) {
      intervalYearWriter = new IntervalYearWriterImpl(data.getIntervalYearVector());
      intervalYearWriter.setPosition(idx());
      writers.add(intervalYearWriter);
    }
    return intervalYearWriter;
  }

  public IntervalYearWriter asIntervalYear() {
    data.setType(idx(), MinorType.INTERVALYEAR);
    return getIntervalYearWriter();
  }

  @Override
  public void write(IntervalYearHolder holder) {
    data.setType(idx(), MinorType.INTERVALYEAR);
    
    getIntervalYearWriter().setPosition(idx());
    getIntervalYearWriter().writeIntervalYear(holder.value);
  }

  public void writeIntervalYear(int value) {
    data.setType(idx(), MinorType.INTERVALYEAR);
    getIntervalYearWriter().setPosition(idx());
    getIntervalYearWriter().writeIntervalYear(value);
  }

  private TimeSecWriter timeSecWriter;

  private TimeSecWriter getTimeSecWriter() {
    if (timeSecWriter == null) {
      timeSecWriter = new TimeSecWriterImpl(data.getTimeSecVector());
      timeSecWriter.setPosition(idx());
      writers.add(timeSecWriter);
    }
    return timeSecWriter;
  }

  public TimeSecWriter asTimeSec() {
    data.setType(idx(), MinorType.TIMESEC);
    return getTimeSecWriter();
  }

  @Override
  public void write(TimeSecHolder holder) {
    data.setType(idx(), MinorType.TIMESEC);
    
    getTimeSecWriter().setPosition(idx());
    getTimeSecWriter().writeTimeSec(holder.value);
  }

  public void writeTimeSec(int value) {
    data.setType(idx(), MinorType.TIMESEC);
    getTimeSecWriter().setPosition(idx());
    getTimeSecWriter().writeTimeSec(value);
  }

  private TimeMilliWriter timeMilliWriter;

  private TimeMilliWriter getTimeMilliWriter() {
    if (timeMilliWriter == null) {
      timeMilliWriter = new TimeMilliWriterImpl(data.getTimeMilliVector());
      timeMilliWriter.setPosition(idx());
      writers.add(timeMilliWriter);
    }
    return timeMilliWriter;
  }

  public TimeMilliWriter asTimeMilli() {
    data.setType(idx(), MinorType.TIMEMILLI);
    return getTimeMilliWriter();
  }

  @Override
  public void write(TimeMilliHolder holder) {
    data.setType(idx(), MinorType.TIMEMILLI);
    
    getTimeMilliWriter().setPosition(idx());
    getTimeMilliWriter().writeTimeMilli(holder.value);
  }

  public void writeTimeMilli(int value) {
    data.setType(idx(), MinorType.TIMEMILLI);
    getTimeMilliWriter().setPosition(idx());
    getTimeMilliWriter().writeTimeMilli(value);
  }

  private BigIntWriter bigIntWriter;

  private BigIntWriter getBigIntWriter() {
    if (bigIntWriter == null) {
      bigIntWriter = new BigIntWriterImpl(data.getBigIntVector());
      bigIntWriter.setPosition(idx());
      writers.add(bigIntWriter);
    }
    return bigIntWriter;
  }

  public BigIntWriter asBigInt() {
    data.setType(idx(), MinorType.BIGINT);
    return getBigIntWriter();
  }

  @Override
  public void write(BigIntHolder holder) {
    data.setType(idx(), MinorType.BIGINT);
    
    getBigIntWriter().setPosition(idx());
    getBigIntWriter().writeBigInt(holder.value);
  }

  public void writeBigInt(long value) {
    data.setType(idx(), MinorType.BIGINT);
    getBigIntWriter().setPosition(idx());
    getBigIntWriter().writeBigInt(value);
  }

  private UInt8Writer uInt8Writer;

  private UInt8Writer getUInt8Writer() {
    if (uInt8Writer == null) {
      uInt8Writer = new UInt8WriterImpl(data.getUInt8Vector());
      uInt8Writer.setPosition(idx());
      writers.add(uInt8Writer);
    }
    return uInt8Writer;
  }

  public UInt8Writer asUInt8() {
    data.setType(idx(), MinorType.UINT8);
    return getUInt8Writer();
  }

  @Override
  public void write(UInt8Holder holder) {
    data.setType(idx(), MinorType.UINT8);
    
    getUInt8Writer().setPosition(idx());
    getUInt8Writer().writeUInt8(holder.value);
  }

  public void writeUInt8(long value) {
    data.setType(idx(), MinorType.UINT8);
    getUInt8Writer().setPosition(idx());
    getUInt8Writer().writeUInt8(value);
  }

  private Float8Writer float8Writer;

  private Float8Writer getFloat8Writer() {
    if (float8Writer == null) {
      float8Writer = new Float8WriterImpl(data.getFloat8Vector());
      float8Writer.setPosition(idx());
      writers.add(float8Writer);
    }
    return float8Writer;
  }

  public Float8Writer asFloat8() {
    data.setType(idx(), MinorType.FLOAT8);
    return getFloat8Writer();
  }

  @Override
  public void write(Float8Holder holder) {
    data.setType(idx(), MinorType.FLOAT8);
    
    getFloat8Writer().setPosition(idx());
    getFloat8Writer().writeFloat8(holder.value);
  }

  public void writeFloat8(double value) {
    data.setType(idx(), MinorType.FLOAT8);
    getFloat8Writer().setPosition(idx());
    getFloat8Writer().writeFloat8(value);
  }

  private DateMilliWriter dateMilliWriter;

  private DateMilliWriter getDateMilliWriter() {
    if (dateMilliWriter == null) {
      dateMilliWriter = new DateMilliWriterImpl(data.getDateMilliVector());
      dateMilliWriter.setPosition(idx());
      writers.add(dateMilliWriter);
    }
    return dateMilliWriter;
  }

  public DateMilliWriter asDateMilli() {
    data.setType(idx(), MinorType.DATEMILLI);
    return getDateMilliWriter();
  }

  @Override
  public void write(DateMilliHolder holder) {
    data.setType(idx(), MinorType.DATEMILLI);
    
    getDateMilliWriter().setPosition(idx());
    getDateMilliWriter().writeDateMilli(holder.value);
  }

  public void writeDateMilli(long value) {
    data.setType(idx(), MinorType.DATEMILLI);
    getDateMilliWriter().setPosition(idx());
    getDateMilliWriter().writeDateMilli(value);
  }

  private TimeStampSecWriter timeStampSecWriter;

  private TimeStampSecWriter getTimeStampSecWriter() {
    if (timeStampSecWriter == null) {
      timeStampSecWriter = new TimeStampSecWriterImpl(data.getTimeStampSecVector());
      timeStampSecWriter.setPosition(idx());
      writers.add(timeStampSecWriter);
    }
    return timeStampSecWriter;
  }

  public TimeStampSecWriter asTimeStampSec() {
    data.setType(idx(), MinorType.TIMESTAMPSEC);
    return getTimeStampSecWriter();
  }

  @Override
  public void write(TimeStampSecHolder holder) {
    data.setType(idx(), MinorType.TIMESTAMPSEC);
    
    getTimeStampSecWriter().setPosition(idx());
    getTimeStampSecWriter().writeTimeStampSec(holder.value);
  }

  public void writeTimeStampSec(long value) {
    data.setType(idx(), MinorType.TIMESTAMPSEC);
    getTimeStampSecWriter().setPosition(idx());
    getTimeStampSecWriter().writeTimeStampSec(value);
  }

  private TimeStampMilliWriter timeStampMilliWriter;

  private TimeStampMilliWriter getTimeStampMilliWriter() {
    if (timeStampMilliWriter == null) {
      timeStampMilliWriter = new TimeStampMilliWriterImpl(data.getTimeStampMilliVector());
      timeStampMilliWriter.setPosition(idx());
      writers.add(timeStampMilliWriter);
    }
    return timeStampMilliWriter;
  }

  public TimeStampMilliWriter asTimeStampMilli() {
    data.setType(idx(), MinorType.TIMESTAMPMILLI);
    return getTimeStampMilliWriter();
  }

  @Override
  public void write(TimeStampMilliHolder holder) {
    data.setType(idx(), MinorType.TIMESTAMPMILLI);
    
    getTimeStampMilliWriter().setPosition(idx());
    getTimeStampMilliWriter().writeTimeStampMilli(holder.value);
  }

  public void writeTimeStampMilli(long value) {
    data.setType(idx(), MinorType.TIMESTAMPMILLI);
    getTimeStampMilliWriter().setPosition(idx());
    getTimeStampMilliWriter().writeTimeStampMilli(value);
  }

  private TimeStampMicroWriter timeStampMicroWriter;

  private TimeStampMicroWriter getTimeStampMicroWriter() {
    if (timeStampMicroWriter == null) {
      timeStampMicroWriter = new TimeStampMicroWriterImpl(data.getTimeStampMicroVector());
      timeStampMicroWriter.setPosition(idx());
      writers.add(timeStampMicroWriter);
    }
    return timeStampMicroWriter;
  }

  public TimeStampMicroWriter asTimeStampMicro() {
    data.setType(idx(), MinorType.TIMESTAMPMICRO);
    return getTimeStampMicroWriter();
  }

  @Override
  public void write(TimeStampMicroHolder holder) {
    data.setType(idx(), MinorType.TIMESTAMPMICRO);
    
    getTimeStampMicroWriter().setPosition(idx());
    getTimeStampMicroWriter().writeTimeStampMicro(holder.value);
  }

  public void writeTimeStampMicro(long value) {
    data.setType(idx(), MinorType.TIMESTAMPMICRO);
    getTimeStampMicroWriter().setPosition(idx());
    getTimeStampMicroWriter().writeTimeStampMicro(value);
  }

  private TimeStampNanoWriter timeStampNanoWriter;

  private TimeStampNanoWriter getTimeStampNanoWriter() {
    if (timeStampNanoWriter == null) {
      timeStampNanoWriter = new TimeStampNanoWriterImpl(data.getTimeStampNanoVector());
      timeStampNanoWriter.setPosition(idx());
      writers.add(timeStampNanoWriter);
    }
    return timeStampNanoWriter;
  }

  public TimeStampNanoWriter asTimeStampNano() {
    data.setType(idx(), MinorType.TIMESTAMPNANO);
    return getTimeStampNanoWriter();
  }

  @Override
  public void write(TimeStampNanoHolder holder) {
    data.setType(idx(), MinorType.TIMESTAMPNANO);
    
    getTimeStampNanoWriter().setPosition(idx());
    getTimeStampNanoWriter().writeTimeStampNano(holder.value);
  }

  public void writeTimeStampNano(long value) {
    data.setType(idx(), MinorType.TIMESTAMPNANO);
    getTimeStampNanoWriter().setPosition(idx());
    getTimeStampNanoWriter().writeTimeStampNano(value);
  }

  private TimeMicroWriter timeMicroWriter;

  private TimeMicroWriter getTimeMicroWriter() {
    if (timeMicroWriter == null) {
      timeMicroWriter = new TimeMicroWriterImpl(data.getTimeMicroVector());
      timeMicroWriter.setPosition(idx());
      writers.add(timeMicroWriter);
    }
    return timeMicroWriter;
  }

  public TimeMicroWriter asTimeMicro() {
    data.setType(idx(), MinorType.TIMEMICRO);
    return getTimeMicroWriter();
  }

  @Override
  public void write(TimeMicroHolder holder) {
    data.setType(idx(), MinorType.TIMEMICRO);
    
    getTimeMicroWriter().setPosition(idx());
    getTimeMicroWriter().writeTimeMicro(holder.value);
  }

  public void writeTimeMicro(long value) {
    data.setType(idx(), MinorType.TIMEMICRO);
    getTimeMicroWriter().setPosition(idx());
    getTimeMicroWriter().writeTimeMicro(value);
  }

  private TimeNanoWriter timeNanoWriter;

  private TimeNanoWriter getTimeNanoWriter() {
    if (timeNanoWriter == null) {
      timeNanoWriter = new TimeNanoWriterImpl(data.getTimeNanoVector());
      timeNanoWriter.setPosition(idx());
      writers.add(timeNanoWriter);
    }
    return timeNanoWriter;
  }

  public TimeNanoWriter asTimeNano() {
    data.setType(idx(), MinorType.TIMENANO);
    return getTimeNanoWriter();
  }

  @Override
  public void write(TimeNanoHolder holder) {
    data.setType(idx(), MinorType.TIMENANO);
    
    getTimeNanoWriter().setPosition(idx());
    getTimeNanoWriter().writeTimeNano(holder.value);
  }

  public void writeTimeNano(long value) {
    data.setType(idx(), MinorType.TIMENANO);
    getTimeNanoWriter().setPosition(idx());
    getTimeNanoWriter().writeTimeNano(value);
  }

  private IntervalDayWriter intervalDayWriter;

  private IntervalDayWriter getIntervalDayWriter() {
    if (intervalDayWriter == null) {
      intervalDayWriter = new IntervalDayWriterImpl(data.getIntervalDayVector());
      intervalDayWriter.setPosition(idx());
      writers.add(intervalDayWriter);
    }
    return intervalDayWriter;
  }

  public IntervalDayWriter asIntervalDay() {
    data.setType(idx(), MinorType.INTERVALDAY);
    return getIntervalDayWriter();
  }

  @Override
  public void write(IntervalDayHolder holder) {
    data.setType(idx(), MinorType.INTERVALDAY);
    
    getIntervalDayWriter().setPosition(idx());
    getIntervalDayWriter().writeIntervalDay(holder.days, holder.milliseconds);
  }

  public void writeIntervalDay(int days, int milliseconds) {
    data.setType(idx(), MinorType.INTERVALDAY);
    getIntervalDayWriter().setPosition(idx());
    getIntervalDayWriter().writeIntervalDay(days, milliseconds);
  }

  private IntervalMonthDayNanoWriter intervalMonthDayNanoWriter;

  private IntervalMonthDayNanoWriter getIntervalMonthDayNanoWriter() {
    if (intervalMonthDayNanoWriter == null) {
      intervalMonthDayNanoWriter = new IntervalMonthDayNanoWriterImpl(data.getIntervalMonthDayNanoVector());
      intervalMonthDayNanoWriter.setPosition(idx());
      writers.add(intervalMonthDayNanoWriter);
    }
    return intervalMonthDayNanoWriter;
  }

  public IntervalMonthDayNanoWriter asIntervalMonthDayNano() {
    data.setType(idx(), MinorType.INTERVALMONTHDAYNANO);
    return getIntervalMonthDayNanoWriter();
  }

  @Override
  public void write(IntervalMonthDayNanoHolder holder) {
    data.setType(idx(), MinorType.INTERVALMONTHDAYNANO);
    
    getIntervalMonthDayNanoWriter().setPosition(idx());
    getIntervalMonthDayNanoWriter().writeIntervalMonthDayNano(holder.months, holder.days, holder.nanoseconds);
  }

  public void writeIntervalMonthDayNano(int months, int days, long nanoseconds) {
    data.setType(idx(), MinorType.INTERVALMONTHDAYNANO);
    getIntervalMonthDayNanoWriter().setPosition(idx());
    getIntervalMonthDayNanoWriter().writeIntervalMonthDayNano(months, days, nanoseconds);
  }

  private Decimal256Writer decimal256Writer;

  private Decimal256Writer getDecimal256Writer(ArrowType arrowType) {
    if (decimal256Writer == null) {
      decimal256Writer = new Decimal256WriterImpl(data.getDecimal256Vector(arrowType));
      decimal256Writer.setPosition(idx());
      writers.add(decimal256Writer);
    }
    return decimal256Writer;
  }

  public Decimal256Writer asDecimal256(ArrowType arrowType) {
    data.setType(idx(), MinorType.DECIMAL256);
    return getDecimal256Writer(arrowType);
  }

  @Override
  public void write(Decimal256Holder holder) {
    data.setType(idx(), MinorType.DECIMAL256);
    ArrowType arrowType = new ArrowType.Decimal(holder.precision, holder.scale, Decimal256Holder.WIDTH * 8);
    getDecimal256Writer(arrowType).setPosition(idx());
    getDecimal256Writer(arrowType).writeDecimal256(holder.start, holder.buffer, arrowType);
  }

  public void writeDecimal256(long start, ArrowBuf buffer, ArrowType arrowType) {
    data.setType(idx(), MinorType.DECIMAL256);
    getDecimal256Writer(arrowType).setPosition(idx());
    getDecimal256Writer(arrowType).writeDecimal256(start, buffer, arrowType);
  }
  public void writeDecimal256(BigDecimal value) {
    data.setType(idx(), MinorType.DECIMAL256);
    ArrowType arrowType = new ArrowType.Decimal(value.precision(), value.scale(), Decimal256Vector.TYPE_WIDTH * 8);
    getDecimal256Writer(arrowType).setPosition(idx());
    getDecimal256Writer(arrowType).writeDecimal256(value);
  }

  public void writeBigEndianBytesToDecimal256(byte[] value, ArrowType arrowType) {
    data.setType(idx(), MinorType.DECIMAL256);
    getDecimal256Writer(arrowType).setPosition(idx());
    getDecimal256Writer(arrowType).writeBigEndianBytesToDecimal256(value, arrowType);
  }

  private DecimalWriter decimalWriter;

  private DecimalWriter getDecimalWriter(ArrowType arrowType) {
    if (decimalWriter == null) {
      decimalWriter = new DecimalWriterImpl(data.getDecimalVector(arrowType));
      decimalWriter.setPosition(idx());
      writers.add(decimalWriter);
    }
    return decimalWriter;
  }

  public DecimalWriter asDecimal(ArrowType arrowType) {
    data.setType(idx(), MinorType.DECIMAL);
    return getDecimalWriter(arrowType);
  }

  @Override
  public void write(DecimalHolder holder) {
    data.setType(idx(), MinorType.DECIMAL);
    ArrowType arrowType = new ArrowType.Decimal(holder.precision, holder.scale, DecimalHolder.WIDTH * 8);
    getDecimalWriter(arrowType).setPosition(idx());
    getDecimalWriter(arrowType).writeDecimal(holder.start, holder.buffer, arrowType);
  }

  public void writeDecimal(long start, ArrowBuf buffer, ArrowType arrowType) {
    data.setType(idx(), MinorType.DECIMAL);
    getDecimalWriter(arrowType).setPosition(idx());
    getDecimalWriter(arrowType).writeDecimal(start, buffer, arrowType);
  }
  public void writeDecimal(BigDecimal value) {
    data.setType(idx(), MinorType.DECIMAL);
    ArrowType arrowType = new ArrowType.Decimal(value.precision(), value.scale(), DecimalVector.TYPE_WIDTH * 8);
    getDecimalWriter(arrowType).setPosition(idx());
    getDecimalWriter(arrowType).writeDecimal(value);
  }

  public void writeBigEndianBytesToDecimal(byte[] value, ArrowType arrowType) {
    data.setType(idx(), MinorType.DECIMAL);
    getDecimalWriter(arrowType).setPosition(idx());
    getDecimalWriter(arrowType).writeBigEndianBytesToDecimal(value, arrowType);
  }

  private VarBinaryWriter varBinaryWriter;

  private VarBinaryWriter getVarBinaryWriter() {
    if (varBinaryWriter == null) {
      varBinaryWriter = new VarBinaryWriterImpl(data.getVarBinaryVector());
      varBinaryWriter.setPosition(idx());
      writers.add(varBinaryWriter);
    }
    return varBinaryWriter;
  }

  public VarBinaryWriter asVarBinary() {
    data.setType(idx(), MinorType.VARBINARY);
    return getVarBinaryWriter();
  }

  @Override
  public void write(VarBinaryHolder holder) {
    data.setType(idx(), MinorType.VARBINARY);
    
    getVarBinaryWriter().setPosition(idx());
    getVarBinaryWriter().writeVarBinary(holder.start, holder.end, holder.buffer);
  }

  public void writeVarBinary(int start, int end, ArrowBuf buffer) {
    data.setType(idx(), MinorType.VARBINARY);
    getVarBinaryWriter().setPosition(idx());
    getVarBinaryWriter().writeVarBinary(start, end, buffer);
  }

  private VarCharWriter varCharWriter;

  private VarCharWriter getVarCharWriter() {
    if (varCharWriter == null) {
      varCharWriter = new VarCharWriterImpl(data.getVarCharVector());
      varCharWriter.setPosition(idx());
      writers.add(varCharWriter);
    }
    return varCharWriter;
  }

  public VarCharWriter asVarChar() {
    data.setType(idx(), MinorType.VARCHAR);
    return getVarCharWriter();
  }

  @Override
  public void write(VarCharHolder holder) {
    data.setType(idx(), MinorType.VARCHAR);
    
    getVarCharWriter().setPosition(idx());
    getVarCharWriter().writeVarChar(holder.start, holder.end, holder.buffer);
  }

  public void writeVarChar(int start, int end, ArrowBuf buffer) {
    data.setType(idx(), MinorType.VARCHAR);
    getVarCharWriter().setPosition(idx());
    getVarCharWriter().writeVarChar(start, end, buffer);
  }

  private LargeVarCharWriter largeVarCharWriter;

  private LargeVarCharWriter getLargeVarCharWriter() {
    if (largeVarCharWriter == null) {
      largeVarCharWriter = new LargeVarCharWriterImpl(data.getLargeVarCharVector());
      largeVarCharWriter.setPosition(idx());
      writers.add(largeVarCharWriter);
    }
    return largeVarCharWriter;
  }

  public LargeVarCharWriter asLargeVarChar() {
    data.setType(idx(), MinorType.LARGEVARCHAR);
    return getLargeVarCharWriter();
  }

  @Override
  public void write(LargeVarCharHolder holder) {
    data.setType(idx(), MinorType.LARGEVARCHAR);
    
    getLargeVarCharWriter().setPosition(idx());
    getLargeVarCharWriter().writeLargeVarChar(holder.start, holder.end, holder.buffer);
  }

  public void writeLargeVarChar(long start, long end, ArrowBuf buffer) {
    data.setType(idx(), MinorType.LARGEVARCHAR);
    getLargeVarCharWriter().setPosition(idx());
    getLargeVarCharWriter().writeLargeVarChar(start, end, buffer);
  }

  private LargeVarBinaryWriter largeVarBinaryWriter;

  private LargeVarBinaryWriter getLargeVarBinaryWriter() {
    if (largeVarBinaryWriter == null) {
      largeVarBinaryWriter = new LargeVarBinaryWriterImpl(data.getLargeVarBinaryVector());
      largeVarBinaryWriter.setPosition(idx());
      writers.add(largeVarBinaryWriter);
    }
    return largeVarBinaryWriter;
  }

  public LargeVarBinaryWriter asLargeVarBinary() {
    data.setType(idx(), MinorType.LARGEVARBINARY);
    return getLargeVarBinaryWriter();
  }

  @Override
  public void write(LargeVarBinaryHolder holder) {
    data.setType(idx(), MinorType.LARGEVARBINARY);
    
    getLargeVarBinaryWriter().setPosition(idx());
    getLargeVarBinaryWriter().writeLargeVarBinary(holder.start, holder.end, holder.buffer);
  }

  public void writeLargeVarBinary(long start, long end, ArrowBuf buffer) {
    data.setType(idx(), MinorType.LARGEVARBINARY);
    getLargeVarBinaryWriter().setPosition(idx());
    getLargeVarBinaryWriter().writeLargeVarBinary(start, end, buffer);
  }

  private BitWriter bitWriter;

  private BitWriter getBitWriter() {
    if (bitWriter == null) {
      bitWriter = new BitWriterImpl(data.getBitVector());
      bitWriter.setPosition(idx());
      writers.add(bitWriter);
    }
    return bitWriter;
  }

  public BitWriter asBit() {
    data.setType(idx(), MinorType.BIT);
    return getBitWriter();
  }

  @Override
  public void write(BitHolder holder) {
    data.setType(idx(), MinorType.BIT);
    
    getBitWriter().setPosition(idx());
    getBitWriter().writeBit(holder.value);
  }

  public void writeBit(int value) {
    data.setType(idx(), MinorType.BIT);
    getBitWriter().setPosition(idx());
    getBitWriter().writeBit(value);
  }

  public void writeNull() {
  }

  @Override
  public StructWriter struct() {
    data.setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().struct();
  }

  @Override
  public ListWriter list() {
    data.setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().list();
  }

  @Override
  public ListWriter list(String name) {
    data.setType(idx(), MinorType.STRUCT);
    getStructWriter().setPosition(idx());
    return getStructWriter().list(name);
  }

  @Override
  public StructWriter struct(String name) {
    data.setType(idx(), MinorType.STRUCT);
    getStructWriter().setPosition(idx());
    return getStructWriter().struct(name);
  }

  @Override
  public MapWriter map() {
    data.setType(idx(), MinorType.MAP);
    getListWriter().setPosition(idx());
    return getListWriter().map();
  }

  @Override
  public MapWriter map(boolean keysSorted) {
    data.setType(idx(), MinorType.MAP);
    getListWriter().setPosition(idx());
    return getListWriter().map(keysSorted);
  }

  @Override
  public MapWriter map(String name) {
    data.setType(idx(), MinorType.MAP);
    getStructWriter().setPosition(idx());
    return getStructWriter().map(name);
  }

  @Override
  public MapWriter map(String name, boolean keysSorted) {
    data.setType(idx(), MinorType.MAP);
    getStructWriter().setPosition(idx());
    return getStructWriter().map(name, keysSorted);
  }

  @Override
  public TinyIntWriter tinyInt(String name) {
    data.setType(idx(), MinorType.STRUCT);
    getStructWriter().setPosition(idx());
    return getStructWriter().tinyInt(name);
  }

  @Override
  public TinyIntWriter tinyInt() {
    data.setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().tinyInt();
  }
  @Override
  public UInt1Writer uInt1(String name) {
    data.setType(idx(), MinorType.STRUCT);
    getStructWriter().setPosition(idx());
    return getStructWriter().uInt1(name);
  }

  @Override
  public UInt1Writer uInt1() {
    data.setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().uInt1();
  }
  @Override
  public UInt2Writer uInt2(String name) {
    data.setType(idx(), MinorType.STRUCT);
    getStructWriter().setPosition(idx());
    return getStructWriter().uInt2(name);
  }

  @Override
  public UInt2Writer uInt2() {
    data.setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().uInt2();
  }
  @Override
  public SmallIntWriter smallInt(String name) {
    data.setType(idx(), MinorType.STRUCT);
    getStructWriter().setPosition(idx());
    return getStructWriter().smallInt(name);
  }

  @Override
  public SmallIntWriter smallInt() {
    data.setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().smallInt();
  }
  @Override
  public IntWriter integer(String name) {
    data.setType(idx(), MinorType.STRUCT);
    getStructWriter().setPosition(idx());
    return getStructWriter().integer(name);
  }

  @Override
  public IntWriter integer() {
    data.setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().integer();
  }
  @Override
  public UInt4Writer uInt4(String name) {
    data.setType(idx(), MinorType.STRUCT);
    getStructWriter().setPosition(idx());
    return getStructWriter().uInt4(name);
  }

  @Override
  public UInt4Writer uInt4() {
    data.setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().uInt4();
  }
  @Override
  public Float4Writer float4(String name) {
    data.setType(idx(), MinorType.STRUCT);
    getStructWriter().setPosition(idx());
    return getStructWriter().float4(name);
  }

  @Override
  public Float4Writer float4() {
    data.setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().float4();
  }
  @Override
  public DateDayWriter dateDay(String name) {
    data.setType(idx(), MinorType.STRUCT);
    getStructWriter().setPosition(idx());
    return getStructWriter().dateDay(name);
  }

  @Override
  public DateDayWriter dateDay() {
    data.setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().dateDay();
  }
  @Override
  public IntervalYearWriter intervalYear(String name) {
    data.setType(idx(), MinorType.STRUCT);
    getStructWriter().setPosition(idx());
    return getStructWriter().intervalYear(name);
  }

  @Override
  public IntervalYearWriter intervalYear() {
    data.setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().intervalYear();
  }
  @Override
  public TimeSecWriter timeSec(String name) {
    data.setType(idx(), MinorType.STRUCT);
    getStructWriter().setPosition(idx());
    return getStructWriter().timeSec(name);
  }

  @Override
  public TimeSecWriter timeSec() {
    data.setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().timeSec();
  }
  @Override
  public TimeMilliWriter timeMilli(String name) {
    data.setType(idx(), MinorType.STRUCT);
    getStructWriter().setPosition(idx());
    return getStructWriter().timeMilli(name);
  }

  @Override
  public TimeMilliWriter timeMilli() {
    data.setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().timeMilli();
  }
  @Override
  public BigIntWriter bigInt(String name) {
    data.setType(idx(), MinorType.STRUCT);
    getStructWriter().setPosition(idx());
    return getStructWriter().bigInt(name);
  }

  @Override
  public BigIntWriter bigInt() {
    data.setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().bigInt();
  }
  @Override
  public UInt8Writer uInt8(String name) {
    data.setType(idx(), MinorType.STRUCT);
    getStructWriter().setPosition(idx());
    return getStructWriter().uInt8(name);
  }

  @Override
  public UInt8Writer uInt8() {
    data.setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().uInt8();
  }
  @Override
  public Float8Writer float8(String name) {
    data.setType(idx(), MinorType.STRUCT);
    getStructWriter().setPosition(idx());
    return getStructWriter().float8(name);
  }

  @Override
  public Float8Writer float8() {
    data.setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().float8();
  }
  @Override
  public DateMilliWriter dateMilli(String name) {
    data.setType(idx(), MinorType.STRUCT);
    getStructWriter().setPosition(idx());
    return getStructWriter().dateMilli(name);
  }

  @Override
  public DateMilliWriter dateMilli() {
    data.setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().dateMilli();
  }
  @Override
  public TimeStampSecWriter timeStampSec(String name) {
    data.setType(idx(), MinorType.STRUCT);
    getStructWriter().setPosition(idx());
    return getStructWriter().timeStampSec(name);
  }

  @Override
  public TimeStampSecWriter timeStampSec() {
    data.setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().timeStampSec();
  }
  @Override
  public TimeStampMilliWriter timeStampMilli(String name) {
    data.setType(idx(), MinorType.STRUCT);
    getStructWriter().setPosition(idx());
    return getStructWriter().timeStampMilli(name);
  }

  @Override
  public TimeStampMilliWriter timeStampMilli() {
    data.setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().timeStampMilli();
  }
  @Override
  public TimeStampMicroWriter timeStampMicro(String name) {
    data.setType(idx(), MinorType.STRUCT);
    getStructWriter().setPosition(idx());
    return getStructWriter().timeStampMicro(name);
  }

  @Override
  public TimeStampMicroWriter timeStampMicro() {
    data.setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().timeStampMicro();
  }
  @Override
  public TimeStampNanoWriter timeStampNano(String name) {
    data.setType(idx(), MinorType.STRUCT);
    getStructWriter().setPosition(idx());
    return getStructWriter().timeStampNano(name);
  }

  @Override
  public TimeStampNanoWriter timeStampNano() {
    data.setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().timeStampNano();
  }
  @Override
  public TimeMicroWriter timeMicro(String name) {
    data.setType(idx(), MinorType.STRUCT);
    getStructWriter().setPosition(idx());
    return getStructWriter().timeMicro(name);
  }

  @Override
  public TimeMicroWriter timeMicro() {
    data.setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().timeMicro();
  }
  @Override
  public TimeNanoWriter timeNano(String name) {
    data.setType(idx(), MinorType.STRUCT);
    getStructWriter().setPosition(idx());
    return getStructWriter().timeNano(name);
  }

  @Override
  public TimeNanoWriter timeNano() {
    data.setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().timeNano();
  }
  @Override
  public IntervalDayWriter intervalDay(String name) {
    data.setType(idx(), MinorType.STRUCT);
    getStructWriter().setPosition(idx());
    return getStructWriter().intervalDay(name);
  }

  @Override
  public IntervalDayWriter intervalDay() {
    data.setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().intervalDay();
  }
  @Override
  public IntervalMonthDayNanoWriter intervalMonthDayNano(String name) {
    data.setType(idx(), MinorType.STRUCT);
    getStructWriter().setPosition(idx());
    return getStructWriter().intervalMonthDayNano(name);
  }

  @Override
  public IntervalMonthDayNanoWriter intervalMonthDayNano() {
    data.setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().intervalMonthDayNano();
  }
  @Override
  public Decimal256Writer decimal256(String name) {
    data.setType(idx(), MinorType.STRUCT);
    getStructWriter().setPosition(idx());
    return getStructWriter().decimal256(name);
  }

  @Override
  public Decimal256Writer decimal256() {
    data.setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().decimal256();
  }
  @Override
  public Decimal256Writer decimal256(String name, int scale, int precision) {
    data.setType(idx(), MinorType.STRUCT);
    getStructWriter().setPosition(idx());
    return getStructWriter().decimal256(name, scale, precision);
  }
  @Override
  public DecimalWriter decimal(String name) {
    data.setType(idx(), MinorType.STRUCT);
    getStructWriter().setPosition(idx());
    return getStructWriter().decimal(name);
  }

  @Override
  public DecimalWriter decimal() {
    data.setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().decimal();
  }
  @Override
  public DecimalWriter decimal(String name, int scale, int precision) {
    data.setType(idx(), MinorType.STRUCT);
    getStructWriter().setPosition(idx());
    return getStructWriter().decimal(name, scale, precision);
  }
  @Override
  public VarBinaryWriter varBinary(String name) {
    data.setType(idx(), MinorType.STRUCT);
    getStructWriter().setPosition(idx());
    return getStructWriter().varBinary(name);
  }

  @Override
  public VarBinaryWriter varBinary() {
    data.setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().varBinary();
  }
  @Override
  public VarCharWriter varChar(String name) {
    data.setType(idx(), MinorType.STRUCT);
    getStructWriter().setPosition(idx());
    return getStructWriter().varChar(name);
  }

  @Override
  public VarCharWriter varChar() {
    data.setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().varChar();
  }
  @Override
  public LargeVarCharWriter largeVarChar(String name) {
    data.setType(idx(), MinorType.STRUCT);
    getStructWriter().setPosition(idx());
    return getStructWriter().largeVarChar(name);
  }

  @Override
  public LargeVarCharWriter largeVarChar() {
    data.setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().largeVarChar();
  }
  @Override
  public LargeVarBinaryWriter largeVarBinary(String name) {
    data.setType(idx(), MinorType.STRUCT);
    getStructWriter().setPosition(idx());
    return getStructWriter().largeVarBinary(name);
  }

  @Override
  public LargeVarBinaryWriter largeVarBinary() {
    data.setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().largeVarBinary();
  }
  @Override
  public BitWriter bit(String name) {
    data.setType(idx(), MinorType.STRUCT);
    getStructWriter().setPosition(idx());
    return getStructWriter().bit(name);
  }

  @Override
  public BitWriter bit() {
    data.setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().bit();
  }

  @Override
  public void allocate() {
    data.allocateNew();
  }

  @Override
  public void clear() {
    data.clear();
  }

  @Override
  public void close() throws Exception {
    data.close();
  }

  @Override
  public Field getField() {
    return data.getField();
  }

  @Override
  public int getValueCapacity() {
    return data.getValueCapacity();
  }
}
