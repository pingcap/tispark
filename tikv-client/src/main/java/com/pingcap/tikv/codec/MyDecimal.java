/*
 * Copyright 2017 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv.codec;

import org.spark_project.guava.annotations.VisibleForTesting;

import java.math.BigDecimal;
import java.util.Arrays;

// TODO: We shouldn't allow empty MyDecimal
// TODO: It seems MyDecimal to BigDecimal is very slow
public class MyDecimal {
  // how many digits that a word has
  private static final int digitsPerWord = 9;
  // MyDecimal can holds at most 9 words.
  public static int wordBufLen = 9;
  // A word is 4 bytes int
  private static final int wordSize = 4;
  private static final int ten0 = 1;
  private static final int ten1 = 10;
  private static final int ten2 = 100;
  private static final int ten3 = 1000;
  private static final int ten4 = 10000;
  private static final int ten5 = 100000;
  private static final int ten6 = 1000000;
  private static final int ten7 = 10000000;
  private static final int ten8 = 100000000;
  private static final int ten9 = 1000000000;
  private static final int digMask = ten8;
  private static final int wordBase = ten9;
  private static final int wordMax = wordBase - 1;
  private static final int[] powers10 =
      new int[] {ten0, ten1, ten2, ten3, ten4, ten5, ten6, ten7, ten8, ten9};

  // A MyDecimal holds 9 words.
  public static final int maxWordBufLen = 9;
  private static final int maxFraction = 30;
  private static final int[] dig2bytes = new int[] {0, 1, 1, 2, 2, 3, 3, 4, 4, 4};

  // The following are fields of MyDecimal
  private int digitsInt;
  private int digitsFrac;
  private int resultFrac;
  private boolean negative;
  private int[] wordBuf = new int[maxWordBufLen];

  // Integer limit values
  private static final int maxInt32 = 1 << 31 - 1;
  private static final int minInt32 = -1 << 31;

  public enum MyDecimalError {
    // Error returned by shiftDecimal
    noError,
    errOverflow,
    errTruncated
  }

  public enum RoundMode {
    // mode for RoundMode
    // ModeHalfEven rounds normally.
    // Truncate just truncates the decimal.
    // Ceiling is not supported now.
    modeHalfEven,
    modeTruncate,
    modeCeiling
  }

  private static final int[] fracMax = new int[] {
    900000000,
    990000000,
    999000000,
    999900000,
    999990000,
    999999000,
    999999900,
    999999990,
  };

  /*
   * Returns total precision of this decimal. Basically, it is sum of digitsInt and digitsFrac. But there
   * are some special cases need to be token care of such as 000.001.
   * Precision reflects the actual effective precision without leading zero
   */
  public int precision() {
    int frac = this.digitsFrac;
    int digitsInt =
        this.removeLeadingZeros()[1]; /*this function return an array and the second element is digitsInt*/
    int precision = digitsInt + frac;
    // if no precision, it is just 0.
    if (precision == 0) {
      precision = 1;
    }
    return precision;
  }

  /**
   * Returns fraction digits that counts how many digits after ".".
   * frac() reflects the actual effective fraction without trailing zero
   */
  public int frac() {
    return digitsFrac;
  }

  /**
   * Parses a decimal value from a string
   *
   * @param value an double value
   */
  public void fromDecimal(double value) {
    String s = value + "";
    this.fromString(s);
  }

  /**
   * Parses a decimal from binary string for given precision and frac.
   *
   * @param precision precision specifies total digits that this decimal will be..
   * @param frac frac specifies how many fraction digits
   * @param bin bin is binary string which represents a decimal value.
   */
  public int fromBin(int precision, int frac, int[] bin) {
    if (bin.length == 0) {
      throw new IllegalArgumentException("Bad Float Number to parse");
    }

    int digitsInt = precision - frac;
    int wordsInt = digitsInt / digitsPerWord;
    int leadingDigits = digitsInt - wordsInt * digitsPerWord;
    int wordsFrac = frac / digitsPerWord;
    int trailingDigits = frac - wordsFrac * digitsPerWord;
    int wordsIntTo = wordsInt;
    if (leadingDigits > 0) {
      wordsIntTo++;
    }
    int wordsFracTo = wordsFrac;
    if (trailingDigits > 0) {
      wordsFracTo++;
    }

    int binIdx = 0;
    int mask = -1;
    int sign = bin[binIdx] & 0x80;
    if (sign > 0) {
      mask = 0;
    }
    int binSize = decimalBinSize(precision, frac);
    int[] dCopy;
    dCopy = Arrays.copyOf(bin, binSize);
    dCopy[0] ^= 0x80;
    bin = dCopy;

    int oldWordsIntTo = wordsIntTo;
    boolean overflow = false;
    boolean truncated = false;
    if (wordsIntTo + wordsFracTo > wordBufLen) {
      if (wordsIntTo > wordBufLen) {
        wordsIntTo = wordBufLen;
        wordsFracTo = 0;
        overflow = true;
      } else {
        wordsIntTo = wordsInt;
        wordsFracTo = wordBufLen - wordsInt;
        truncated = true;
      }
    }

    if (overflow || truncated) {
      if (wordsIntTo < oldWordsIntTo) {
        binIdx += dig2bytes[leadingDigits] + (wordsInt - wordsIntTo) * wordSize;
      } else {
        trailingDigits = 0;
        wordsFrac = wordsFracTo;
      }
    }

    this.negative = mask != 0;
    this.digitsInt = (byte)(wordsInt * digitsPerWord + leadingDigits);
    this.digitsFrac = (byte)(wordsFrac * digitsPerWord + trailingDigits);

    int wordIdx = 0;
    if (leadingDigits > 0) {
      int i = dig2bytes[leadingDigits];
      int x = readWord(bin, i, binIdx);
      binIdx += i;
      this.wordBuf[wordIdx] = (x ^ mask) > 0 ? x ^ mask : (x ^ mask) & 0xFF;
      if (this.wordBuf[wordIdx] >= powers10[leadingDigits + 1]) {
        throw new IllegalArgumentException("BadNumber");
      }
      if (this.wordBuf[wordIdx] != 0) {
        wordIdx++;
      } else {
        this.digitsInt -= leadingDigits;
      }
    }
    for (int stop = binIdx + wordsInt * wordSize; binIdx < stop; binIdx += wordSize) {
      this.wordBuf[wordIdx] = (readWord(bin, 4, binIdx) ^ mask);
      if (this.wordBuf[wordIdx] > wordMax) {
        throw new IllegalArgumentException("BadNumber");
      }
      if (wordIdx > 0 || this.wordBuf[wordIdx] != 0) {
        wordIdx++;
      } else {
        this.digitsInt -= digitsPerWord;
      }
    }

    for (int stop = binIdx + wordsFrac * wordSize; binIdx < stop; binIdx += wordSize) {
      int x = readWord(bin, 4, binIdx);
      this.wordBuf[wordIdx] = (x ^ mask) > 0 ? x ^ mask : (x ^ mask) & 0xFF;
      if (this.wordBuf[wordIdx] > wordMax) {
        throw new IllegalArgumentException("BadNumber");
      }
      wordIdx++;
    }

    if (trailingDigits > 0) {
      int i = dig2bytes[trailingDigits];
      int x = readWord(bin, i, binIdx);
      this.wordBuf[wordIdx] =
          ((x ^ mask) > 0 ? x ^ mask : (x ^ mask) & 0xFF)
              * powers10[digitsPerWord - trailingDigits];
      if (this.wordBuf[wordIdx] > wordMax) {
        throw new IllegalArgumentException("BadNumber");
      }
      wordIdx++;
    }

    this.resultFrac = frac;
    return binSize;
  }

  /** Returns a double value from MyDecimal instance. */
  public BigDecimal toDecimal() {
    return new BigDecimal(toString());
  }

  public double toDouble() {
    return Double.parseDouble(toString());
  }

  /** Truncates any prefix zeros such as 00.001. After this, digitsInt is truncated from 2 to 0. */
  private int[] removeLeadingZeros() {
    int wordIdx = 0;
    int digitsInt = this.digitsInt;
    int i = ((digitsInt - 1) % digitsPerWord) + 1;
    for (; digitsInt > 0 && this.wordBuf[wordIdx] == 0; ) {
      digitsInt -= i;
      i = digitsPerWord;
      wordIdx++;
    }
    if (digitsInt > 0) {
      digitsInt -= countLeadingZeroes((digitsInt - 1) % digitsPerWord, this.wordBuf[wordIdx]);
    } else {
      digitsInt = 0;
    }
    int[] res = new int[2];
    res[0] = wordIdx;
    res[1] = digitsInt;
    return res;
  }

  /**
   * Counts the number of digits of prefix zeors. For 00.001, it reutrns two.
   *
   * @param i i is index for getting powers10.
   * @param word word is a integer.
   */
  private int countLeadingZeroes(int i, int word) {
    int leading = 0;
    for (; word < powers10[i]; ) {
      i--;
      leading++;
    }
    return leading;
  }

  /** Returns size of word for a give value with number of digits */
  private int digitsToWords(int digits) {
    return (digits + digitsPerWord - 1) / digitsPerWord;
  }

  /**
   * Reads a word from a array at given size.
   *
   * @param b b is source data of unsigned byte as int[]
   * @param size is word size which can be used in switch statement.
   * @param start start indicates the where start to read.
   */
  @VisibleForTesting
  public static int readWord(int[] b, int size, int start) {
    int x = 0;
    switch (size) {
      case 1:
        x = (byte)b[start];
        break;
      case 2:
        x = (((byte)b[start]) << 8) + (b[start + 1] & 0xFF);
        break;
      case 3:
        int sign = b[start] & 128;
        if (sign > 0) {
          x = 0xFF << 24 | (b[start] << 16) | (b[start + 1] << 8) | (b[start + 2]);
        } else {
          x = b[start] << 16 | (b[start + 1] << 8) | b[start + 2];
        }
        break;
      case 4:
        x = b[start + 3] + (b[start + 2] << 8) + (b[start + 1] << 16) + (b[start] << 24);
        break;
    }
    return x;
  }

  /**
   * parser a decimal value from a string.
   *
   * @param s s is a decimal in string form.
   */
  @VisibleForTesting
  public void fromString(String s) {
    char[] sCharArray = s.toCharArray();
    fromCharArray(sCharArray);
  }

  // helper function for fromString
  // TODO: return error if possible
  private void fromCharArray(char[] str) {
    int startIdx = 0;
    // found first character is not space and start from here
    for (; startIdx < str.length; startIdx++) {
      if (!Character.isSpaceChar(str[startIdx])) {
        break;
      }
    }

    // prevent empty string
    if (str.length == 0 || startIdx >= str.length) {
      throw new IllegalArgumentException("BadNumber");
    }

    // skip sign and record where digits start from
    // [-, 1, 2, 3]
    // [+, 1, 2, 3]
    // for +/-, we need skip them and record sign information into negative field.
    switch (str[startIdx]) {
      case '-':
        this.negative = true;
        startIdx++;
        break;
      case '+':
        startIdx++;
        break;
    }
    int strIdx = startIdx;
    for (; strIdx < str.length && Character.isDigit(str[strIdx]); ) {
      strIdx++;
    }
    // we initialize strIdx in case of sign notation, here we need substract startIdx from strIdx casue strIdx is used for counting the number of digits.
    int digitsInt = strIdx - startIdx;
    int digitsFrac;
    int endIdx;
    if (strIdx < str.length && str[strIdx] == '.') {
      endIdx = strIdx + 1;
      // detect where is the end index of this char array.
      for (; endIdx < str.length && Character.isDigit(str[endIdx]); ) {
        endIdx++;
      }
      digitsFrac = endIdx - strIdx - 1;
    } else {
      digitsFrac = 0;
      endIdx = strIdx;
    }

    if (digitsInt + digitsFrac == 0) {
      throw new IllegalArgumentException("BadNumber");
    }
    int wordsInt = digitsToWords(digitsInt);
    int wordsFrac = digitsToWords(digitsFrac);

    // TODO the following code are fixWordCntError such as overflow and truncated error
    boolean overflow = false;
    boolean truncated = false;
    if (wordsInt + wordsFrac > wordBufLen) {
      if (wordsInt > wordBufLen) {
        wordsInt = wordBufLen;
        wordsFrac = 0;
        overflow = true;
      } else {
        wordsFrac = wordBufLen - wordsInt;
        truncated = true;
      }

    }

    if (overflow || truncated) {
      digitsFrac = wordsFrac * digitsPerWord;
      if (overflow) {
        digitsInt = wordsInt * digitsPerWord;
      }
    }
    this.digitsInt = digitsInt;
    this.digitsFrac = digitsFrac;
    int wordIdx = wordsInt;
    int strIdxTmp = strIdx;
    int word = 0;
    int innerIdx = 0;
    for (; digitsInt > 0; ) {
      digitsInt--;
      strIdx--;
      word += (str[strIdx] - '0') * powers10[innerIdx];
      innerIdx++;
      if (innerIdx == digitsPerWord) {
        wordIdx--;
        this.wordBuf[wordIdx] = word;
        word = 0;
        innerIdx = 0;
      }
    }

    if (innerIdx != 0) {
      wordIdx--;
      this.wordBuf[wordIdx] = word;
    }

    wordIdx = wordsInt;
    strIdx = strIdxTmp;
    word = 0;
    innerIdx = 0;

    for (; digitsFrac > 0; ) {
      digitsFrac--;
      strIdx++;
      word = (str[strIdx] - '0') + word * 10;
      innerIdx++;
      if (innerIdx == digitsPerWord) {
        this.wordBuf[wordIdx] = word;
        wordIdx++;
        word = 0;
        innerIdx = 0;
      }
    }
    if (innerIdx != 0) {
      this.wordBuf[wordIdx] = word * powers10[digitsPerWord - innerIdx];
    }

    // for e & E of double
    if (endIdx + 1 <= str.length && (str[endIdx] == 'e' || str[endIdx] == 'E')) {
      LongWithError result = strToLong(String.valueOf(str).substring(endIdx + 1));
      int exponent = result.result;
      MyDecimalError error = result.error;

      if (!error.equals(MyDecimalError.noError)) {
        if (!error.equals(MyDecimalError.errTruncated)) {
          this.clear();
        }
      }

      if (exponent > maxInt32 / 2) {
          boolean negative = this.negative;
          maxDecimal(wordBufLen * digitsPerWord, 0);
          this.negative = negative;
          overflow = true;
      }

      if (exponent < minInt32 / 2 && !overflow) {
        this.clear();
        truncated = true;
      }

      if (!overflow) {
        MyDecimalError shiftErr = this.shiftDecimal(exponent);
        if (!shiftErr.equals(MyDecimalError.noError)) {
          if (shiftErr.equals(MyDecimalError.errOverflow)) {
            boolean negative = this.negative;
            maxDecimal(wordBufLen * digitsPerWord, 0);
            this.negative = negative;
          }
          // TODO, handle shift error
        }
      }
    }

    // this is -0000 is just 0.
    boolean allZero = true;
    for (int i = 0; i < wordBufLen; i++) {
      if (this.wordBuf[i] != 0) {
        allZero = false;
        break;
      }
    }
    if (allZero) {
      this.negative = false;
    }

    this.resultFrac = this.digitsFrac;
  }

  private class LongWithError {
    public int result;
    public MyDecimalError error;

    public LongWithError(int result, MyDecimalError error) {
      this.result = result;
      this.error = error;
    }
  }

  // parse a string to a int.
  private LongWithError strToLong(String str) {
    str = str.trim();
    if (str.isEmpty()) {
      return new LongWithError(0, MyDecimalError.errTruncated);
    }
    boolean negative = false;
    int i = 0;
    if (str.charAt(i) == '-') {
      negative = true;
      i++;
    } else if (str.charAt(i) == '+') {
      i++;
    }

    int r = 0;
    boolean hasNum = false;
    MyDecimalError error = MyDecimalError.noError;
    for (; i < str.length(); i++) {
      if (!Character.isDigit(str.charAt(i))) {
        error = MyDecimalError.errTruncated;
        break;
      }
      hasNum = true;
      r = r * 10;

      int r1 = r + (str.charAt(i) - '0');

      if (r1 < r) {
        r = 0;
        break;
      }
      r = r1;
    }

    if (!hasNum) {
      error = MyDecimalError.errTruncated;
    }

    // TODO: deal with uint64

    if (negative) {
      r = -r;
    }
    return new LongWithError(r, error);
  }

  // Returns a decimal string.
  public String toString() {
    char[] str;
    int digitsFrac = this.digitsFrac;
    int[] res = removeLeadingZeros();
    int wordStartIdx = res[0];
    int digitsInt = res[1];
    if (digitsInt + digitsFrac == 0) {
      digitsInt = 1;
      wordStartIdx = 0;
    }

    int digitsIntLen = digitsInt;
    if (digitsIntLen == 0) {
      digitsIntLen = 1;
    }
    int digitsFracLen = digitsFrac;
    int length = digitsIntLen + digitsFracLen;
    if (this.negative) {
      length++;
    }
    if (digitsFrac > 0) {
      length++;
    }
    str = new char[length];
    int strIdx = 0;
    if (this.negative) {
      str[strIdx] = '-';
      strIdx++;
    }
    int fill = 0;
    if (digitsFrac > 0) {
      int fracIdx = strIdx + digitsIntLen;
      fill = digitsFracLen - digitsFrac;
      int wordIdx = wordStartIdx + digitsToWords(digitsInt);
      str[fracIdx] = '.';
      fracIdx++;
      for (; digitsFrac > 0; digitsFrac -= digitsPerWord) {
        int x = this.wordBuf[wordIdx];
        wordIdx++;
        for (int i = Math.min(digitsFrac, digitsPerWord); i > 0; i--) {
          int y = x / digMask;
          str[fracIdx] = (char) (y + '0');
          fracIdx++;
          x -= y * digMask;
          x *= 10;
        }
      }
      for (; fill > 0; fill--) {
        str[fracIdx] = '0';
        fracIdx++;
      }
    }
    fill = digitsIntLen - digitsInt;
    if (digitsInt == 0) {
      fill--; /* symbol 0 before digital point */
    }
    for (; fill > 0; fill--) {
      str[strIdx] = '0';
      strIdx++;
    }
    if (digitsInt > 0) {
      strIdx += digitsInt;
      int wordIdx = wordStartIdx + digitsToWords(digitsInt);
      for (; digitsInt > 0; digitsInt -= digitsPerWord) {
        wordIdx--;
        int x = this.wordBuf[wordIdx];
        for (int i = Math.min(digitsInt, digitsPerWord); i > 0; i--) {
          int y = x / 10;
          strIdx--;
          str[strIdx] = (char) ('0' + (x - y * 10));
          x = y;
        }
      }
    } else {
      str[strIdx] = '0';
    }

    return new String(str);
  }

  private int stringSize() {
    return digitsInt + digitsFrac + 3;
  }

  public long toLong() {
    long x = 0;
    int wordIdx = 0;
    for (int i = this.digitsInt; i > 0; i -= digitsPerWord) {
      /*
        Attention: trick!
        we're calculating -|from| instead of |from| here
        because |LONGLONG_MIN| > LONGLONG_MAX
        so we can convert -9223372036854775808 correctly
      */
      long y = x;
      x = x * wordBase - (long)this.wordBuf[wordIdx];
      wordIdx++;
      if (y < Long.MIN_VALUE/wordBase || x > y) {
        	  /*
			        the decimal is bigger than any possible integer
			        return border integer depending on the sign
			      */
        	 if (this.negative) {
        	   return Long.MIN_VALUE;
           }
           return Long.MAX_VALUE;
      }
    }

    /* boundary case: 9223372036854775808 */
    if (!this.negative && x == Long.MIN_VALUE) {
               return Long.MAX_VALUE;
    }

    if (!this.negative) {
      x = -x;
    }
    for (int i = this.digitsFrac; i > 0; i -= digitsPerWord) {
                  if (this.wordBuf[wordIdx] != 0){
                    return x;
                  }
                  wordIdx++;
    }
    return x;
  }

  // decimalBinSize returns the size of array to hold a binary representation of a decimal.
  private int decimalBinSize(int precision, int frac) {
    int digitsInt = precision - frac;
    int wordsInt = digitsInt / digitsPerWord;
    int wordsFrac = frac / digitsPerWord;
    int xInt = digitsInt - wordsInt * digitsPerWord;
    int xFrac = frac - wordsFrac * digitsPerWord;
    return wordsInt * wordSize + dig2bytes[xInt] + wordsFrac * wordSize + dig2bytes[xFrac];
  }

  /**
   * ToBin converts decimal to its binary fixed-length representation two representations of the
   * same length can be compared with memcmp with the correct -1/0/+1 result
   *
   * <p>PARAMS precision/frac - if precision is 0, internal value of the decimal will be used, then
   * the encoded value is not memory comparable.
   *
   * <p>NOTE the buffer is assumed to be of the size decimalBinSize(precision, frac)
   *
   * <p>RETURN VALUE bin - binary value errCode - eDecOK/eDecTruncate/eDecOverflow
   *
   * <p>DESCRIPTION for storage decimal numbers are converted to the "binary" format.
   *
   * <p>This format has the following properties: 1. length of the binary representation depends on
   * the {precision, frac} as provided by the caller and NOT on the digitsInt/digitsFrac of the
   * decimal to convert. 2. binary representations of the same {precision, frac} can be compared
   * with memcmp - with the same result as DecimalCompare() of the original decimals (not taking
   * into account possible precision loss during conversion).
   *
   * <p>This binary format is as follows: 1. First the number is converted to have a requested
   * precision and frac. 2. Every full digitsPerWord digits of digitsInt part are stored in 4 bytes
   * as is 3. The first digitsInt % digitesPerWord digits are stored in the reduced number of bytes
   * (enough bytes to store this number of digits - see dig2bytes) 4. same for frac - full word are
   * stored as is, the last frac % digitsPerWord digits - in the reduced number of bytes. 5. If the
   * number is negative - every byte is inversed. 5. The very first bit of the resulting byte array
   * is inverted (because memcmp compares unsigned bytes, see property 2 above)
   *
   * <p>Example:
   *
   * <p>1234567890.1234
   *
   * <p>internally is represented as 3 words
   *
   * <p>1 234567890 123400000
   *
   * <p>(assuming we want a binary representation with precision=14, frac=4) in hex it's
   *
   * <p>00-00-00-01 0D-FB-38-D2 07-5A-EF-40
   *
   * <p>now, middle word is full - it stores 9 decimal digits. It goes into binary representation as
   * is:
   *
   * <p>........... 0D-FB-38-D2 ............
   *
   * <p>First word has only one decimal digit. We can store one digit in one byte, no need to waste
   * four:
   *
   * <p>01 0D-FB-38-D2 ............
   *
   * <p>now, last word. It's 123400000. We can store 1234 in two bytes:
   *
   * <p>01 0D-FB-38-D2 04-D2
   *
   * <p>So, we've packed 12 bytes number in 7 bytes. And now we invert the highest bit to get the
   * final result:
   *
   * <p>81 0D FB 38 D2 04 D2
   *
   * <p>And for -1234567890.1234 it would be
   *
   * <p>7E F2 04 C7 2D FB 2D return a int array which represents a decimal value.
   *
   * @param precision precision for decimal value.
   * @param frac fraction for decimal value.
   */
  public int[] toBin(int precision, int frac) {
    if (precision > digitsPerWord * maxWordBufLen
        || precision < 0
        || frac > maxFraction
        || frac < 0) {
      throw new IllegalArgumentException("BadNumber");
    }

    int mask = 0;
    if (this.negative) {
      mask = -1;
    }

    int digitsInt = precision - frac;
    int wordsInt = digitsInt / digitsPerWord;
    int leadingDigits = digitsInt - wordsInt * digitsPerWord;
    int wordsFrac = frac / digitsPerWord;
    int trailingDigits = frac - wordsFrac * digitsPerWord;

    // this should be one of 0, 1, 2, 3, 4
    int wordsFracFrom = this.digitsFrac / digitsPerWord;
    int trailingDigitsFrom = this.digitsFrac - wordsFracFrom * digitsPerWord;
    int intSize = wordsInt * wordSize + dig2bytes[leadingDigits];
    int fracSize = wordsFrac * wordSize + dig2bytes[trailingDigits];
    int fracSizeFrom = wordsFracFrom * wordSize + dig2bytes[trailingDigitsFrom];
    int originIntSize = intSize;
    int originFracSize = fracSize;
    int[] bin = new int[intSize + fracSize];
    int binIdx = 0;
    int[] res = this.removeLeadingZeros();
    int wordIdxFrom = res[0];
    int digitsIntFrom = res[1];
    if (digitsIntFrom + fracSizeFrom == 0) {
      mask = 0;
      digitsInt = 1;
    }

    int wordsIntFrom = digitsIntFrom / digitsPerWord;
    int leadingDigitsFrom = digitsIntFrom - wordsIntFrom * digitsPerWord;
    int iSizeFrom = wordsIntFrom * wordSize + dig2bytes[leadingDigitsFrom];

    if (digitsInt < digitsIntFrom) {
      wordIdxFrom += (wordsIntFrom - wordsInt);
      if (leadingDigitsFrom > 0) {
        wordIdxFrom++;
      }

      if (leadingDigits > 0) {
        wordIdxFrom--;
      }

      wordsIntFrom = wordsInt;
      leadingDigitsFrom = leadingDigits;
      //TODO overflow here
    } else if (intSize > iSizeFrom) {
      for (; intSize > iSizeFrom; ) {
        intSize--;
        bin[binIdx] = mask & 0xff;
        binIdx++;
      }
    }

    // when fracSize smaller than fracSizeFrom, output is truncated
    if (fracSize < fracSizeFrom) {
      wordsFracFrom = wordsFrac;
      trailingDigitsFrom = trailingDigits;
      //TODO truncated
    } else if (fracSize > fracSizeFrom && trailingDigitsFrom > 0) {
      if (wordsFrac == wordsFracFrom) {
        trailingDigitsFrom = trailingDigits;
        fracSize = fracSizeFrom;
      } else {
        wordsFracFrom++;
        trailingDigitsFrom = 0;
      }
    }

    // xIntFrom part
    if (leadingDigitsFrom > 0) {
      int i = dig2bytes[leadingDigitsFrom];
      int x = (this.wordBuf[wordIdxFrom] % powers10[leadingDigitsFrom]) ^ mask;
      wordIdxFrom++;
      writeWord(bin, x, i, binIdx);
      binIdx += i;
    }

    // wordsInt + wordsFrac part.
    for (int stop = wordIdxFrom + wordsIntFrom + wordsFracFrom;
        wordIdxFrom < stop;
        binIdx += wordSize) {
      int x = this.wordBuf[wordIdxFrom] ^ mask;
      wordIdxFrom++;
      writeWord(bin, x, 4, binIdx);
    }

    if (trailingDigitsFrom > 0) {
      int x;
      int i = dig2bytes[trailingDigitsFrom];
      int lim = trailingDigits;
      if (wordsFracFrom < wordsFrac) {
        lim = digitsPerWord;
      }

      for (; trailingDigitsFrom < lim && dig2bytes[trailingDigitsFrom] == i; ) {
        trailingDigitsFrom++;
      }
      x = (this.wordBuf[wordIdxFrom] / powers10[digitsPerWord - trailingDigitsFrom]) ^ mask;
      writeWord(bin, x, i, binIdx);
      binIdx += i;
    }

    if (fracSize > fracSizeFrom) {
      int binIdxEnd = originIntSize + originFracSize;
      for (; fracSize > fracSizeFrom && binIdx < binIdxEnd; ) {
        fracSize--;
        bin[binIdx] = mask & 0xff;
        binIdx++;
      }
    }
    bin[0] ^= 0x80;
    return bin;
  }

  // write a word into buf.
  private void writeWord(int[] b, int word, int size, int start) {
    switch (size) {
      case 1:
        b[start] = word & 0xFF;
        break;
      case 2:
        b[start] = (word >>> 8) & 0xFF;
        b[start + 1] = word & 0xFF;
        break;
      case 3:
        b[start] = (word >>> 16) & 0xFF;
        b[start + 1] = (word >>> 8) & 0xFF;
        b[start + 2] = word & 0xFF;
        break;
      case 4:
        b[start] = (word >>> 24) & 0xFF;
        b[start + 1] = (word >>> 16) & 0xFF;
        b[start + 2] = (word >>> 8) & 0xFF;
        b[start + 3] = word & 0xFF;
        break;
    }
  }

  public void maxDecimal(int precision, int frac) {
    int digitsInt = precision - frac;
    this.negative = false;
    this.digitsInt = digitsInt & 0xff;
    int idx = 0;
    if (digitsInt > 0) {
      int firstWordDigits = digitsInt % digitsPerWord;
      if (firstWordDigits > 0) {
        this.wordBuf[idx] = powers10[firstWordDigits] - 1; /* get 9 99 999 ... */
        idx++;
      }
      for (digitsInt /= digitsPerWord; digitsInt > 0; digitsInt--) {
        this.wordBuf[idx] = wordMax;
        idx++;
      }
    }
    this.digitsFrac = frac & 0xff;
    if (frac > 0) {
      int lastDigits = frac % digitsPerWord;
      for (frac /= digitsPerWord; frac > 0; frac--) {
        this.wordBuf[idx] = wordMax;
        idx++;
      }

      if (lastDigits > 0) {
        this.wordBuf[idx] = fracMax[lastDigits - 1];
      }
    }
  }

  // Shift shifts decimal digits in given number (with rounding if it need), shift > 0 means shift to left shift,
  // shift < 0 means right shift. In fact it is multiplying on 10^shift.
  public MyDecimalError shiftDecimal(int shift) {
    MyDecimalError error = MyDecimalError.noError;
    if (shift == 0) {
      return MyDecimalError.noError;
    }

    // digitBegin is index of first non zero digit (all indexes from 0).
    int digitBegin = 0;
    // digitEnd is index of position after last decimal digit.
    int digitEnd = 0;
    // point is index of digit position just after point.
    int point = digitsToWords(this.digitsInt) * digitsPerWord;
    // new point position.
    int newPoint = point + shift;
    // number of digits in result.
    int digitsInt = 0;
    int digitsFrac = 0;
    int newFront = 0;

    int[] res = this.digitBounds();
    digitBegin = res[0];
    digitEnd = res[1];

    if (digitBegin == digitEnd) {
		  this.clear();
      return MyDecimalError.noError;
    }

    digitsInt = newPoint - digitBegin;
    if (digitsInt < 0) {
      digitsInt = 0;
    }
    digitsFrac = digitEnd - newPoint;
    if (digitsFrac < 0) {
      digitsFrac = 0;
    }
    int wordsInt = digitsToWords(digitsInt);
    int wordsFrac = digitsToWords(digitsFrac);
    int newLen = wordsInt + wordsFrac;
    if (newLen > wordBufLen) {
      int lack = newLen - wordBufLen;
      if (wordsFrac < lack) {
        return MyDecimalError.errOverflow;
      }
      /* cut off fraction part to allow new number to fit in our buffer */
      error = MyDecimalError.errTruncated;
      wordsFrac -= lack;
      int diff = digitsFrac - wordsFrac * digitsPerWord;
      MyDecimalError err1 = this.round(this, digitEnd-point-diff, RoundMode.modeHalfEven);
      if (!err1.equals(MyDecimalError.noError)) {
        return err1;
      }
      digitEnd -= diff;
      digitsFrac = wordsFrac * digitsPerWord;
      if (digitEnd <= digitBegin) {
			/*
			   We lost all digits (they will be shifted out of buffer), so we can
			   just return 0.
			*/
        this.clear();
        return MyDecimalError.errTruncated;
      }
    }

    if (shift % digitsPerWord != 0) {
      int lMiniShift = 0, rMiniShift = 0, miniShift = 0;
      boolean doLeft = false;
		/*
		   Calculate left/right shift to align decimal digits inside our bug
		   digits correctly.
		*/
      if (shift > 0) {
        lMiniShift = shift % digitsPerWord;
        rMiniShift = digitsPerWord - lMiniShift;
        doLeft = lMiniShift <= digitBegin;
      } else {
        rMiniShift = (-shift) % digitsPerWord;
        lMiniShift = digitsPerWord - rMiniShift;
        doLeft = (digitsPerWord * wordBufLen - digitEnd) < rMiniShift;
      }
      if (doLeft) {
        this.doMiniLeftShift(lMiniShift, digitBegin, digitEnd);
        miniShift = -lMiniShift;
      } else {
        this.doMiniRightShift(rMiniShift, digitBegin, digitEnd);
        miniShift = rMiniShift;
      }
      newPoint += miniShift;
		/*
		   If number is shifted and correctly aligned in buffer we can finish.
		*/
      if (shift+miniShift == 0 && (newPoint-digitsInt) < digitsPerWord) {
        this.digitsInt = digitsInt & 0xff;
        this.digitsFrac = digitsFrac & 0xff;
        return error; /* already shifted as it should be */
      }
      digitBegin += miniShift;
      digitEnd += miniShift;
    }

    /* if new 'decimal front' is in first digit, we do not need move digits */
    newFront = newPoint - digitsInt;
    if (newFront >= digitsPerWord || newFront < 0) {
      /* need to move digits */
      int wordShift;
      if (newFront > 0) {
        /* move left */
        wordShift = newFront / digitsPerWord;
        int to = digitBegin/digitsPerWord - wordShift;
        int barier = (digitEnd-1)/digitsPerWord - wordShift;
        for (; to <= barier; to++) {
          this.wordBuf[to] = this.wordBuf[to+wordShift];
        }
        for (barier += wordShift; to <= barier; to++) {
          this.wordBuf[to] = 0;
        }
        wordShift = -wordShift;
      } else {
        /* move right */
        wordShift = (1 - newFront) / digitsPerWord;
        int to = (digitEnd-1)/digitsPerWord + wordShift;
        int barier = digitBegin/digitsPerWord + wordShift;
        for (; to >= barier; to--) {
          this.wordBuf[to] = this.wordBuf[to - wordShift];
        }
        for (barier -= wordShift; to >= barier; to--) {
          this.wordBuf[to] = 0;
        }
      }
      int digitShift = wordShift * digitsPerWord;
      digitBegin += digitShift;
      digitEnd += digitShift;
      newPoint += digitShift;
    }
	/*
	   If there are gaps then fill them with 0.
	   Only one of following 'for' loops will work because wordIdxBegin <= wordIdxEnd.
	*/
    int wordIdxBegin = digitBegin / digitsPerWord;
    int wordIdxEnd = (digitEnd - 1) / digitsPerWord;
    int wordIdxNewPoint = 0;

    /* We don't want negative new_point below */
    if (newPoint != 0) {
      wordIdxNewPoint = (newPoint - 1) / digitsPerWord;
    }
    if (wordIdxNewPoint > wordIdxEnd) {
      for (; wordIdxNewPoint > wordIdxEnd; ) {
        this.wordBuf[wordIdxNewPoint] = 0;
        wordIdxNewPoint--;
      }
    } else {
      for (; wordIdxNewPoint < wordIdxBegin; wordIdxNewPoint++) {
        this.wordBuf[wordIdxNewPoint] = 0;
      }
    }
    this.digitsInt = digitsInt & 0xff;
    this.digitsFrac = digitsFrac & 0xff;
    return error;
  }


  // digitBounds returns bounds of decimal digits in the number.
  int[] digitBounds() {
    int i = 0;
    int bufBeg = 0;
    int bufLen = digitsToWords(this.digitsInt) + digitsToWords(this.digitsFrac);
    int bufEnd = bufLen - 1;
    int start = 0;
    int end = 0;


    /* find non-zero digit from number beginning */
    for (; bufBeg < bufLen && this.wordBuf[bufBeg] == 0; ) {
      bufBeg++;
    }
    if (bufBeg >= bufLen) {
      return new int[] {0, 0};
    }

    /* find non-zero decimal digit from number beginning */
    if (bufBeg == 0 && this.digitsInt > 0) {
      i = (this.digitsInt - 1) % digitsPerWord;
      start = digitsPerWord - i - 1;
    } else {
      i = digitsPerWord - 1;
      start = bufBeg * digitsPerWord;
    }

    start += this.countLeadingZeroes(i, this.wordBuf[bufBeg]);

    /* find non-zero digit at the end */
    for (; bufEnd > bufBeg && this.wordBuf[bufEnd] == 0; ) {
      bufEnd--;
    }
    /* find non-zero decimal digit from the end */
    if (bufEnd == bufLen-1 && this.digitsFrac > 0) {
      i = (this.digitsFrac-1) % digitsPerWord + 1;
      end = bufEnd * digitsPerWord + i;
      i = digitsPerWord - i + 1;
    } else {
      end = (bufEnd + 1) * digitsPerWord;
      i = 1;
    }
    end -= this.countTrailingZeroes(i, this.wordBuf[bufEnd]);
    return new int[] {start, end};
  }

  // countTrailingZeros returns the number of trailing zeroes that can be removed from fraction.
  private int countTrailingZeroes(int i, int word) {
    int trailing = 0;
    for (; word % powers10[i] == 0; ) {
      i++;
      trailing++;
    }
    return trailing;
  }

  // Round rounds the decimal to "frac" digits.
  //
  //    to			- result buffer. d == to is allowed
  //    frac			- to what position after fraction point to round. can be negative!
  //    roundMode		- round to nearest even or truncate
  // 			ModeHalfEven rounds normally.
  // 			Truncate just truncates the decimal.
  //
  // NOTES
  //  scale can be negative !
  //  one TRUNCATED error (line XXX below) isn't treated very logical :(
  public MyDecimalError round(MyDecimal to, int frac, RoundMode roundMode) {
    // wordsFracTo is the number of fraction words in buffer.
    MyDecimalError error = MyDecimalError.noError;
    int wordsFracTo = (frac + 1) / digitsPerWord;
    if (frac > 0) {
      wordsFracTo = digitsToWords(frac);
    }
    int wordsFrac = digitsToWords(this.digitsFrac);
    int wordsInt = digitsToWords(this.digitsInt);

    int roundDigit = 0;
    /* TODO - fix this code as it won't work for CEILING mode */
    switch (roundMode) {
      case modeCeiling:
        roundDigit = 0;
        break;
      case modeHalfEven:
        roundDigit = 5;
        break;
      case modeTruncate:
        roundDigit = 10;
        break;
    }

    if (wordsInt + wordsFracTo > wordBufLen) {
      wordsFracTo = wordBufLen - wordsInt;
      frac = wordsFracTo * digitsPerWord;
      error = MyDecimalError.errTruncated;
    }
    if (this.digitsInt + frac < 0) {
      to.clear();
      return MyDecimalError.noError;
    }
    if (to != this) {
      to.wordBuf = this.wordBuf.clone();
      to.negative = this.negative;
      to.digitsInt = Math.min(wordsInt, wordBufLen) * digitsPerWord;
    }
    if (wordsFracTo > wordsFrac) {
      int idx = wordsInt + wordsFrac;
      for (; wordsFracTo > wordsFrac; ) {
        wordsFracTo--;
        to.wordBuf[idx] = 0;
        idx++;
      }
      to.digitsFrac = frac & 0xff;
      to.resultFrac = to.digitsFrac;
      return error;
    }
    if (frac >= this.digitsFrac) {
      to.digitsFrac = frac & 0xff;
      to.resultFrac = to.digitsFrac;
      return error;
    }

    // Do increment.
    int toIdx = wordsInt + wordsFracTo - 1;
    if (frac == wordsFracTo * digitsPerWord) {
      boolean doInc = false;
      switch (roundDigit) {
        // Notice: No support for ceiling mode now.
        case 0:
          // If any word after scale is not zero, do increment.
          // e.g ceiling 3.0001 to scale 1, gets 3.1
          int idx = toIdx + (wordsFrac - wordsFracTo);
          for (; idx > toIdx; ) {
            if (this.wordBuf[idx] != 0) {
              doInc = true;
              break;
            }
            idx--;
          }
          break;
        case 5:
          int digAfterScale = this.wordBuf[toIdx+1] / digMask; // the first digit after scale.
          // If first digit after scale is 5 and round even, do increment if digit at scale is odd.
          doInc = (digAfterScale > 5) || (digAfterScale == 5);
          break;
        case 10:
          // Never round, just truncate.
          doInc = false;
      }
      if (doInc) {
        if (toIdx >= 0) {
          to.wordBuf[toIdx]++;
        } else {
          toIdx++;
          to.wordBuf[toIdx] = wordBase;
        }
      } else if (wordsInt+wordsFracTo == 0) {
        to.clear();
        return MyDecimalError.noError;
      }
    } else {
      /* TODO - fix this code as it won't work for CEILING mode */
      int pos = wordsFracTo*digitsPerWord - frac - 1;
      int shiftedNumber = to.wordBuf[toIdx] / powers10[pos];
      int digAfterScale = shiftedNumber % 10;
      if (digAfterScale > roundDigit || (roundDigit == 5 && digAfterScale == 5)) {
        shiftedNumber += 10;
      }
      to.wordBuf[toIdx] = powers10[pos] * (shiftedNumber - digAfterScale);
    }
	/*
	   In case we're rounding e.g. 1.5e9 to 2.0e9, the decimal words inside
	   the buffer are as follows.

	   Before <1, 5e8>
	   After  <2, 5e8>

	   Hence we need to set the 2nd field to 0.
	   The same holds if we round 1.5e-9 to 2e-9.
	*/
    if (wordsFracTo < wordsFrac) {
      int idx = wordsInt + wordsFracTo;
      if (frac == 0 && wordsInt == 0) {
        idx = 1;
      }
      for (; idx < wordBufLen; ) {
        to.wordBuf[idx] = 0;
        idx++;
      }
    }

    // Handle carry.
    int carry = 0;
    if (to.wordBuf[toIdx] >= wordBase) {
      carry = 1;
      to.wordBuf[toIdx] -= wordBase;
      for (; toIdx > 0; ) {
        toIdx--;
        int[] res = this.add(to.wordBuf[toIdx], 0, carry);
        to.wordBuf[toIdx] = res[0];
        carry = res[1];
      }
      if (carry > 0) {
        if (wordsInt+wordsFracTo >= wordBufLen) {
          wordsFracTo--;
          frac = wordsFracTo * digitsPerWord;
          error = MyDecimalError.errTruncated;
        }
        for (toIdx = wordsInt + Math.max(wordsFracTo, 0); toIdx > 0; toIdx--) {
          if (toIdx < wordBufLen) {
            to.wordBuf[toIdx] = to.wordBuf[toIdx-1];
          } else {
            error = MyDecimalError.errOverflow;
          }
        }
        to.wordBuf[toIdx] = 1;
        /* We cannot have more than 9 * 9 = 81 digits. */
        if (to.digitsInt < digitsPerWord * wordBufLen) {
          to.digitsInt++;
        } else {
          error = MyDecimalError.errOverflow;
        }
      }
    } else {
      while (true) {
        if (to.wordBuf[toIdx] != 0) {
          break;
        }
        if (toIdx == 0) {
          /* making 'zero' with the proper scale */
          int idx = wordsFracTo + 1;
          to.digitsInt = 1;
          to.digitsFrac = Math.max(frac, 0) & 0xff;
          to.negative = false;
          for (; toIdx < idx; ) {
            to.wordBuf[toIdx] = 0;
            toIdx++;
          }
          to.resultFrac = to.digitsFrac;
          return MyDecimalError.noError;
        }
        toIdx--;
      }
    }
    /* Here we check 999.9 -> 1000 case when we need to increase intDigCnt */
    int firstDig = to.digitsInt % digitsPerWord;
    if (firstDig > 0 && to.wordBuf[toIdx] >= powers10[firstDig]) {
      to.digitsInt++;
    }
    if (frac < 0) {
      frac = 0;
    }
    to.digitsFrac = frac & 0xff;
    to.resultFrac = to.digitsFrac;
    return error;
  }

  // add adds a and b and carry, returns the sum and new carry
  private int[] add(int a, int b, int carry) {
    int sum = a + b + carry;
    if (sum >= wordBase) {
      carry = 1;
      sum -= wordBase;
    } else {
      carry = 0;
    }
    return new int[] {sum, carry};
  }

  /*
  doMiniLeftShift does left shift for alignment of data in buffer.

    shift   number of decimal digits on which it should be shifted
    beg/end bounds of decimal digits (see digitsBounds())

    NOTE
      Result fitting in the buffer should be garanted.
      'shift' have to be from 1 to digitsPerWord-1 (inclusive)
  */
  private void doMiniLeftShift(int shift, int beg, int end) {
    int bufFrom = beg / digitsPerWord;
    int bufEnd = (end - 1) / digitsPerWord;
    int cShift = digitsPerWord - shift;
    if (beg % digitsPerWord < shift) {
      this.wordBuf[bufFrom-1] = this.wordBuf[bufFrom] / powers10[cShift];
    }
    for (; bufFrom < bufEnd; ) {
      this.wordBuf[bufFrom] = (this.wordBuf[bufFrom]%powers10[cShift])*powers10[shift] + this.wordBuf[bufFrom+1]/powers10[cShift];
      bufFrom++;
    }
    this.wordBuf[bufFrom] = (this.wordBuf[bufFrom] % powers10[cShift]) * powers10[shift];
  }

  /*
    doMiniRightShift does right shift for alignment of data in buffer.

      shift   number of decimal digits on which it should be shifted
      beg/end bounds of decimal digits (see digitsBounds())

    NOTE
      Result fitting in the buffer should be garanted.
      'shift' have to be from 1 to digitsPerWord-1 (inclusive)
  */
  private void doMiniRightShift(int shift, int beg, int end) {
    int bufFrom = (end - 1) / digitsPerWord;
    int bufEnd = beg / digitsPerWord;
    int cShift = digitsPerWord - shift;
    if (digitsPerWord - ((end - 1) % digitsPerWord + 1) < shift) {
      this.wordBuf[bufFrom+1] = (this.wordBuf[bufFrom] % powers10[shift]) * powers10[cShift];
    }
    for (; bufFrom > bufEnd; ) {
      this.wordBuf[bufFrom] = this.wordBuf[bufFrom] / powers10[shift] + (this.wordBuf[bufFrom-1] % powers10[shift]) * powers10[cShift];
      bufFrom--;
    }
    this.wordBuf[bufFrom] = this.wordBuf[bufFrom] / powers10[shift];
  }

  /** Clears this instance. */
  public void clear() {
    this.digitsFrac = 0;
    this.digitsInt = 0;
    this.negative = false;
  }
}
