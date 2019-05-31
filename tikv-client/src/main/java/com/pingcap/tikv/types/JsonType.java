package com.pingcap.tikv.types;

import com.google.gson.*;
import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.exception.ConvertDataOverflowException;
import com.pingcap.tikv.exception.TypeConvertNotSupportException;
import com.pingcap.tikv.meta.TiColumnInfo.InternalTypeHolder;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nullable;

public class JsonType extends DataType {

  private static final int KEY_ENTRY_LENGTH = 6;
  private static final int VALUE_ENTRY_SIZE = 5;
  private static final int PREFIX_LENGTH = 8;

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

  protected JsonType(InternalTypeHolder holder) {
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
    byte type = cdi.readByte();
    return parseValue(type, cdi).toString();
  }

  @Override
  public Object convertToTiDBType(Object value)
      throws TypeConvertNotSupportException, ConvertDataOverflowException {
    throw new TypeConvertNotSupportException(value.getClass().getName(), this.getClass().getName());
  }

  /**
   * The binary JSON format from MySQL 5.7 is as follows:
   *
   * <pre>{@code
   * JsonFormat {
   *      JSON doc ::= type value
   *      type ::=
   *          0x01 |       // large JSON object
   *          0x03 |       // large JSON array
   *          0x04 |       // literal (true/false/null)
   *          0x05 |       // int16
   *          0x06 |       // uint16
   *          0x07 |       // int32
   *          0x08 |       // uint32
   *          0x09 |       // int64
   *          0x0a |       // uint64
   *          0x0b |       // double
   *          0x0c |       // utf8mb4 string
   *      value ::=
   *          object  |
   *          array   |
   *          literal |
   *          number  |
   *          string  |
   *      object ::= element-count size key-entry* value-entry* key* value*
   *      array ::= element-count size value-entry* value*
   *      // number of members in object or number of elements in array
   *      element-count ::= uint32
   *      // number of bytes in the binary representation of the object or array
   *      size ::= uint32
   *      key-entry ::= key-offset key-length
   *      key-offset ::= uint32
   *      key-length ::= uint16    // key length must be less than 64KB
   *      value-entry ::= type offset-or-inlined-value
   *      // This field holds either the offset to where the value is stored,
   *      // or the value itself if it is small enough to be inlined (that is,
   *      // if it is a JSON literal or a small enough [u]int).
   *      offset-or-inlined-value ::= uint32
   *      key ::= utf8mb4-data
   *      literal ::=
   *          0x00 |   // JSON null literal
   *          0x01 |   // JSON true literal
   *          0x02 |   // JSON false literal
   *      number ::=  ....    // little-endian format for [u]int(16|32|64), whereas
   *                          // double is stored in a platform-independent, eight-byte
   *                          // format using float8store()
   *      string ::= data-length utf8mb4-data
   *      data-length ::= uint8*    // If the high bit of a byte is 1, the length
   *                                // field is continued in the next byte,
   *                                // otherwise it is the last byte of the length
   *                                // field. So we need 1 byte to represent
   *                                // lengths up to 127, 2 bytes to represent
   *                                // lengths up to 16383, and so on...
   * }
   * }</pre>
   *
   * @param type type byte
   * @param cdi codec data input
   * @return Json element parsed
   */
  private JsonElement parseValue(byte type, CodecDataInput cdi) {
    int elementCount, length;
    switch (type) {
      case TYPE_CODE_OBJECT:
        elementCount = parseUint32(cdi);
        length = parseUint32(cdi) - PREFIX_LENGTH;
        return parseObject(cdi, elementCount, length);
      case TYPE_CODE_ARRAY:
        elementCount = parseUint32(cdi);
        length = parseUint32(cdi) - PREFIX_LENGTH;
        return parseArray(cdi, elementCount, length);
      case TYPE_CODE_LITERAL:
        return parseLiteralJson(cdi);
      case TYPE_CODE_INT64:
        return new JsonPrimitive(parseInt64(cdi));
      case TYPE_CODE_UINT64:
        return new JsonPrimitive(parseUint64(cdi));
      case TYPE_CODE_FLOAT64:
        return new JsonPrimitive(parseDouble(cdi));
      case TYPE_CODE_STRING:
        length = parseDataLength(cdi);
        return new JsonPrimitive(parseString(cdi, length));
      default:
        throw new AssertionError("error type|type=" + (int) type);
    }
  }

  // * notice use this as a unsigned long
  private long parseUint64(CodecDataInput cdi) {
    return cdi.readLongLSB();
  }

  private long parseInt64(CodecDataInput cdi) {
    return cdi.readLongLSB();
  }

  private int parseUint32(CodecDataInput cdi) {
    return cdi.readIntLSB();
  }

  private int parseUint16(CodecDataInput cdi) {
    return cdi.readUnsignedShortLSB();
  }

  private double parseDouble(CodecDataInput cdi) {
    return cdi.readDoubleLSB();
  }

  private String parseString(CodecDataInput cdi, int length) {
    byte[] buffer = new byte[length];
    cdi.readFully(buffer, 0, length);
    return new String(buffer, StandardCharsets.UTF_8);
  }

  /**
   * func Uvarint(buf []byte) (uint64, int) { var x uint64 var s uint for i, b := range buf { if b <
   * 0x80 { if i > 9 || i == 9 && b > 1 { return 0, -(i + 1) // overflow } return x | uint64(b)<<s,
   * i + 1 * } x |= uint64(b&0x7f) << s s += 7 } return 0, 0 }
   */
  private int parseDataLength(CodecDataInput cdi) {
    int x = 0;
    byte b;
    int i = 0;
    int s = 0;
    while ((b = cdi.readByte()) < 0) {
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

  private @Nullable Boolean parseLiteral(CodecDataInput cdi) {
    byte type;
    type = cdi.readByte();
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

  private JsonArray parseArray(CodecDataInput cdi, int elementCount, int length) {
    byte[] buffer = new byte[length];
    cdi.readFully(buffer);
    JsonArray jsonArray = new JsonArray();
    for (int i = 0; i < elementCount; i++) {
      JsonElement value = parseValueEntry(buffer, i * VALUE_ENTRY_SIZE);
      jsonArray.add(value);
    }
    return jsonArray;
  }

  private JsonObject parseObject(CodecDataInput cdi, int elementCount, int length) {
    byte[] buffer = new byte[length];
    cdi.readFully(buffer);
    JsonObject jsonObject = new JsonObject();
    long valueEntryOffset = elementCount * KEY_ENTRY_LENGTH;
    for (int i = 0; i < elementCount; i++) {
      KeyEntry keyEntry = parseKeyEntry(new CodecDataInput(buffer, i * KEY_ENTRY_LENGTH));
      String key = parseString(new CodecDataInput(buffer, keyEntry.keyOffset), keyEntry.keyLength);
      JsonElement value = parseValueEntry(buffer, valueEntryOffset + i * VALUE_ENTRY_SIZE);
      jsonObject.add(key, value);
    }
    return jsonObject;
  }

  private JsonElement parseValueEntry(byte[] buffer, long valueEntryOffset) {
    byte valueType = buffer[Math.toIntExact(valueEntryOffset)];
    CodecDataInput bs = new CodecDataInput(buffer, Math.toIntExact(valueEntryOffset + 1));
    if (valueType == TYPE_CODE_LITERAL) {
      return parseLiteralJson(bs);
    } else {
      int valueOffset = parseUint32(bs) - PREFIX_LENGTH;
      return parseValue(valueType, new CodecDataInput(buffer, valueOffset));
    }
  }

  private JsonElement parseLiteralJson(CodecDataInput cdi) {
    JsonElement value;
    Boolean bool = parseLiteral(cdi);
    if (bool == null) {
      value = JsonNull.INSTANCE;
    } else if (bool) {
      value = JSON_TRUE;
    } else {
      value = JSON_FALSE;
    }
    return value;
  }

  private KeyEntry parseKeyEntry(CodecDataInput cdi) {
    int offset = parseUint32(cdi) - PREFIX_LENGTH;
    int length = parseUint16(cdi);
    return new KeyEntry(offset, length);
  }

  static class KeyEntry {
    int keyOffset;
    int keyLength;

    KeyEntry(int keyOffset, int keyLength) {
      this.keyOffset = keyOffset;
      this.keyLength = keyLength;
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
  public Object getOriginDefaultValueNonNull(String value, long version) {
    throw new AssertionError("json can't have a default value");
  }
}
