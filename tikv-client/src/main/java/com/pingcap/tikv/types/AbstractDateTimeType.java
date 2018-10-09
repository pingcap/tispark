package com.pingcap.tikv.types;


import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.codec.Codec;
import com.pingcap.tikv.codec.Codec.DateTimeCodec;
import com.pingcap.tikv.codec.Codec.DateCodec;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.exception.InvalidCodecFormatException;
import com.pingcap.tikv.meta.TiColumnInfo.InternalTypeHolder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;

public abstract class AbstractDateTimeType extends DataType {
  AbstractDateTimeType(InternalTypeHolder holder) {
    super(holder);
  }

  AbstractDateTimeType(MySQLType tp) {
    super(tp);
  }

  /**
   * Return timezone used for encoding and decoding
   */
  protected abstract DateTimeZone getTimezone();

  /**
   * Decode DateTime from packed long value
   * In TiDB / MySQL, timestamp type is converted to UTC and stored
   */
  DateTime decodeDateTime(int flag, CodecDataInput cdi) {
    DateTime dateTime;
    if (flag == Codec.UVARINT_FLAG) {
      dateTime = DateTimeCodec.readFromUVarInt(cdi, getTimezone());
    } else if (flag == Codec.UINT_FLAG) {
      dateTime = DateTimeCodec.readFromUInt(cdi, getTimezone());
    } else {
      throw new InvalidCodecFormatException("Invalid Flag type for " + getClass().getSimpleName() + ": " + flag);
    }
    return dateTime;
  }

  /**
   * Decode Date from packed long value
   */
  LocalDate decodeDate(int flag, CodecDataInput cdi) {
    LocalDate date;
    if (flag == Codec.UVARINT_FLAG) {
      date = DateCodec.readFromUVarInt(cdi);
    } else if (flag == Codec.UINT_FLAG) {
      date = DateCodec.readFromUInt(cdi);
    } else {
      throw new InvalidCodecFormatException("Invalid Flag type for " + getClass().getSimpleName() + ": " + flag);
    }
    return date;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void encodeKey(CodecDataOutput cdo, Object value) {
    DateTime dt = Converter.convertToDateTime(value);
    DateTimeCodec.writeDateTimeFully(cdo, dt, getTimezone());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void encodeValue(CodecDataOutput cdo, Object value) {
    encodeKey(cdo, value);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void encodeProto(CodecDataOutput cdo, Object value) {
    DateTime dt = Converter.convertToDateTime(value);
    DateTimeCodec.writeDateTimeProto(cdo, dt, getTimezone());
  }

  @Override
  public ExprType getProtoExprType() {
    return ExprType.MysqlTime;
  }
}
