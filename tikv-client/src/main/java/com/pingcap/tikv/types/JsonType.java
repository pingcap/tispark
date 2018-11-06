package com.pingcap.tikv.types;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteStreams;
import com.google.gson.*;
import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.meta.TiColumnInfo;
import java.io.DataInput;
import java.io.IOException;
import java.io.UncheckedIOException;
import javax.annotation.Nullable;
import org.apache.commons.io.Charsets;

public class JsonType extends DataType {

  private static final int KEY_ENTRY_LENGTH = 6;
  private static final int VALUE_ENTRY_SIZE = 5;

  // TypeCodeObject indicates the JSON is an object.
  private static final byte TYPE_CODE_OBJECT = 0x01;
  // TypeCodeArray indicates the JSON is an array.
  private static final byte TYPE_CODE_ARRAY = 0x03;
  // TypeCodeLiteral indicates the JSON is a literal.
  private static final byte TYPE_CODE_LITERAL = 0x04;
  // TypeCodeInt64 indicates the JSON is a signed integer.
  private static final byte TYPE_CODE_INT64 = 0x09;
  // TypeCodeUint64 indicates the JSON is a unsigned integer.
  private static final byte TYPE_CODE_UINT64 = 0x0a;
  // TypeCodeFloat64 indicates the JSON is a double float number.
  private static final byte TYPE_CODE_FLOAT64 = 0x0b;
  // TypeCodeString indicates the JSON is a string.
  private static final byte TYPE_CODE_STRING = 0x0c;

  // LiteralNil represents JSON null.
  private static final byte LITERAL_NIL = 0x00;
  // LiteralTrue represents JSON true.
  private static final byte LITERAL_TRUE = 0x01;
  // LiteralFalse represents JSON false.
  private static final byte LITERAL_FALSE = 0x02;
  private static final JsonPrimitive JSON_FALSE = new JsonPrimitive(false);
  private static final JsonPrimitive JSON_TRUE = new JsonPrimitive(true);
  public static MySQLType[] subTypes = new MySQLType[] {MySQLType.TypeJSON};

  protected JsonType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }

  public JsonType(MySQLType type) {
    super(type);
  }

  public JsonType(MySQLType type, int flag, int len, int decimal, String charset, int collation) {
    super(type, flag, len, decimal, charset, collation);
  }

  @Override
  protected Object decodeNotNull(int flag, CodecDataInput cdi) {
    byte type = readByte(cdi);
    return parseValue(type, cdi).toString();
  }

  /*
     The binary JSON format from MySQL 5.7 is as follows:
     JSON doc ::= type value
     type ::=
         0x01 |       // large JSON object
         0x03 |       // large JSON array
         0x04 |       // literal (true/false/null)
         0x05 |       // int16
         0x06 |       // uint16
         0x07 |       // int32
         0x08 |       // uint32
         0x09 |       // int64
         0x0a |       // uint64
         0x0b |       // double
         0x0c |       // utf8mb4 string
     value ::=
         object  |
         array   |
         literal |
         number  |
         string  |
     object ::= element-count size key-entry* value-entry* key* value*
     array ::= element-count size value-entry* value*
     // number of members in object or number of elements in array
     element-count ::= uint32
     // number of bytes in the binary representation of the object or array
     size ::= uint32
     key-entry ::= key-offset key-length
     key-offset ::= uint32
     key-length ::= uint16    // key length must be less than 64KB
     value-entry ::= type offset-or-inlined-value
     // This field holds either the offset to where the value is stored,
     // or the value itself if it is small enough to be inlined (that is,
     // if it is a JSON literal or a small enough [u]int).
     offset-or-inlined-value ::= uint32
     key ::= utf8mb4-data
     literal ::=
         0x00 |   // JSON null literal
         0x01 |   // JSON true literal
         0x02 |   // JSON false literal
     number ::=  ....    // little-endian format for [u]int(16|32|64), whereas
                         // double is stored in a platform-independent, eight-byte
                         // format using float8store()
     string ::= data-length utf8mb4-data
     data-length ::= uint8*    // If the high bit of a byte is 1, the length
                               // field is continued in the next byte,
                               // otherwise it is the last byte of the length
                               // field. So we need 1 byte to represent
                               // lengths up to 127, 2 bytes to represent
                               // lengths up to 16383, and so on...
  */
  private JsonElement parseValue(byte type, DataInput di) {
    switch (type) {
      case TYPE_CODE_OBJECT:
        return parseObject(di);
      case TYPE_CODE_ARRAY:
        return parseArray(di);
      case TYPE_CODE_LITERAL:
        return parseLiteralJson(di);
      case TYPE_CODE_INT64:
        return new JsonPrimitive(parseInt64(di));
      case TYPE_CODE_UINT64:
        return new JsonPrimitive(parseUint64(di));
      case TYPE_CODE_FLOAT64:
        return new JsonPrimitive(parseDouble(di));
      case TYPE_CODE_STRING:
        long length = parseDataLength(di);
        return new JsonPrimitive(parseString(di, length));
      default:
        throw new AssertionError("error type|type=" + (int) type);
    }
  }

  // * notice use this as a unsigned long
  private long parseUint64(DataInput cdi) {
    byte[] readBuffer = new byte[8];
    readFully(cdi, readBuffer, 0, 8);

    return ((long) (readBuffer[7]) << 56)
        + ((long) (readBuffer[6] & 255) << 48)
        + ((long) (readBuffer[5] & 255) << 40)
        + ((long) (readBuffer[4] & 255) << 32)
        + ((long) (readBuffer[3] & 255) << 24)
        + ((readBuffer[2] & 255) << 16)
        + ((readBuffer[1] & 255) << 8)
        + ((readBuffer[0] & 255) << 0);
  }

  private void readFully(DataInput cdi, byte[] readBuffer, final int off, final int len) {
    try {
      cdi.readFully(readBuffer, off, len);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private long parseInt64(DataInput cdi) {
    byte[] readBuffer = new byte[8];
    readFully(cdi, readBuffer);
    return ((long) readBuffer[7] << 56)
        + ((long) (readBuffer[6] & 255) << 48)
        + ((long) (readBuffer[5] & 255) << 40)
        + ((long) (readBuffer[4] & 255) << 32)
        + ((long) (readBuffer[3] & 255) << 24)
        + ((readBuffer[2] & 255) << 16)
        + ((readBuffer[1] & 255) << 8)
        + ((readBuffer[0] & 255) << 0);
  }

  private long parseUint32(DataInput cdi) {
    byte[] readBuffer = new byte[4];
    readFully(cdi, readBuffer);

    return ((long) (readBuffer[3] & 255) << 24)
        + ((readBuffer[2] & 255) << 16)
        + ((readBuffer[1] & 255) << 8)
        + ((readBuffer[0] & 255) << 0);
  }

  private double parseDouble(DataInput cdi) {
    byte[] readBuffer = new byte[8];
    readFully(cdi, readBuffer);
    return Double.longBitsToDouble(
        ((long) readBuffer[7] << 56)
            + ((long) (readBuffer[6] & 255) << 48)
            + ((long) (readBuffer[5] & 255) << 40)
            + ((long) (readBuffer[4] & 255) << 32)
            + ((long) (readBuffer[3] & 255) << 24)
            + ((readBuffer[2] & 255) << 16)
            + ((readBuffer[1] & 255) << 8)
            + ((readBuffer[0] & 255) << 0));
  }

  private int parseUint16(DataInput cdi) {
    byte[] readBuffer = new byte[2];
    readFully(cdi, readBuffer);

    return ((readBuffer[1] & 255) << 8) + ((readBuffer[0] & 255) << 0);
  }

  private String parseString(DataInput di, long length) {

    byte[] buffer = new byte[Math.toIntExact(length)];
    readFully(di, buffer);
    return new String(buffer, Charsets.UTF_8);
  }

  /**
   * func Uvarint(buf []byte) (uint64, int) { var x uint64 var s uint for i, b := range buf { if b <
   * 0x80 { if i > 9 || i == 9 && b > 1 { return 0, -(i + 1) // overflow } return x | uint64(b)<<s,
   * i + 1 * } x |= uint64(b&0x7f) << s s += 7 } return 0, 0 }
   *
   * @param di
   * @return
   */
  private long parseDataLength(DataInput di) {
    long x = 0;
    byte b;
    int i = 0;
    int s = 0;
    while ((b = readByte(di)) < 0) {
      if (i == 9) {
        throw new IllegalArgumentException("overflow: found >=9 leading bytes");
      }
      x |= ((long) (b & 0x7f)) << s;
      s += 7;
      i++;
    }

    if (i == 9 && b > 1) {
      throw new IllegalArgumentException("overflow: 8 leading byte and last one > 1");
    }
    x |= ((long) b) << s;
    return x;
  }

  private @Nullable Boolean parseLiteral(DataInput cdi) {
    byte type;
    type = readByte(cdi);
    switch (type) {
      case LITERAL_FALSE:
        return Boolean.FALSE;
      case LITERAL_NIL:
        return null;
      case LITERAL_TRUE:
        return Boolean.TRUE;
      default:
        throw new AssertionError("unknown literal type|" + (int) type);
    }
  }

  private byte readByte(DataInput cdi) {
    byte type;
    try {
      type = cdi.readByte();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return type;
  }

  private JsonArray parseArray(DataInput di) {
    long elementCount = parseUint32(di);
    long size = parseUint32(di);
    byte[] buffer = new byte[Math.toIntExact(size - 8)];
    readFully(di, buffer);
    JsonArray jsonArray = new JsonArray();
    for (int i = 0; i < elementCount; i++) {
      JsonElement value = parseValueEntry(buffer, VALUE_ENTRY_SIZE * i);
      jsonArray.add(value);
    }
    return jsonArray;
  }

  private JsonObject parseObject(DataInput di) {
    long elementCount = parseUint32(di);
    long size = parseUint32(di);

    byte[] buffer = new byte[Math.toIntExact(size - 8)];
    readFully(di, buffer);
    JsonObject jsonObject = new JsonObject();
    for (int i = 0; i < elementCount; i++) {
      KeyEntry keyEntry = parseKeyEntry(ByteStreams.newDataInput(buffer, i * KEY_ENTRY_LENGTH));
      String key =
          parseString(
              ByteStreams.newDataInput(buffer, Math.toIntExact(keyEntry.keyOffset - 8)),
              keyEntry.keyLength);
      long valueEntryOffset = elementCount * KEY_ENTRY_LENGTH + i * VALUE_ENTRY_SIZE;
      JsonElement value = parseValueEntry(buffer, valueEntryOffset);
      jsonObject.add(key, value);
    }
    return jsonObject;
  }

  private JsonElement parseValueEntry(byte[] buffer, long valueEntryOffset) {
    byte valueType = buffer[Math.toIntExact(valueEntryOffset)];
    JsonElement value;
    ByteArrayDataInput bs = ByteStreams.newDataInput(buffer, Math.toIntExact(valueEntryOffset + 1));
    switch (valueType) {
      case TYPE_CODE_LITERAL:
        value = parseLiteralJson(bs);
        break;
      default:
        long valueOffset = parseUint32(bs);
        value =
            parseValue(
                valueType, ByteStreams.newDataInput(buffer, Math.toIntExact(valueOffset - 8)));
    }
    return value;
  }

  private JsonElement parseLiteralJson(DataInput di) {
    JsonElement value;
    Boolean bool = parseLiteral(di);
    if (bool == null) {
      value = JsonNull.INSTANCE;
    } else if (bool) {
      value = JSON_TRUE;
    } else {
      value = JSON_FALSE;
    }
    return value;
  }

  private KeyEntry parseKeyEntry(DataInput di) {
    return new KeyEntry(parseUint32(di), parseUint16(di));
  }

  static class KeyEntry {
    long keyOffset;
    int keyLength;

    public KeyEntry(long keyOffset, int keyLength) {
      this.keyOffset = keyOffset;
      this.keyLength = keyLength;
    }
  }

  private void readFully(DataInput di, byte[] buffer) {
    try {
      di.readFully(buffer);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  protected void encodeKey(CodecDataOutput cdo, Object value) {
    throw new UnsupportedOperationException("JsonType.encodeKey|value=" + value);
  }

  @Override
  protected void encodeValue(CodecDataOutput cdo, Object value) {
    throw new UnsupportedOperationException("JsonType.encodeValue|value=" + value);
  }

  @Override
  protected void encodeProto(CodecDataOutput cdo, Object value) {
    throw new UnsupportedOperationException("JsonType.encodeProto|value=" + value);
  }

  @Override
  public ExprType getProtoExprType() {
    return ExprType.MysqlJson;
  }

  @Override
  public Object getOriginDefaultValueNonNull(String value) {
    throw new AssertionError("json can't have a default value");
  }
}
