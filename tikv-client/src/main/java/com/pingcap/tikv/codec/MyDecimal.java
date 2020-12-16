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

import com.google.common.annotations.VisibleForTesting;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;

// TODO: We shouldn't allow empty MyDecimal
// TODO: It seems MyDecimal to BigDecimal is very slow
public class MyDecimal implements Serializable {
  // how many digits that a word has
  private static final int digitsPerWord = 9;
  // MyDecimal can holds at most 9 words.
  private static final int wordBufLen = 9;
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
  private static final BigInteger wordBaseBigInt = BigInteger.valueOf(ten9);
  private static final int wordMax = wordBase - 1;
  private static final int[] div9 =
      new int[] {
        0, 0, 0, 0, 0, 0, 0, 0, 0,
        1, 1, 1, 1, 1, 1, 1, 1, 1,
        2, 2, 2, 2, 2, 2, 2, 2, 2,
        3, 3, 3, 3, 3, 3, 3, 3, 3,
        4, 4, 4, 4, 4, 4, 4, 4, 4,
        5, 5, 5, 5, 5, 5, 5, 5, 5,
        6, 6, 6, 6, 6, 6, 6, 6, 6,
        7, 7, 7, 7, 7, 7, 7, 7, 7,
        8, 8, 8, 8, 8, 8, 8, 8, 8,
        9, 9, 9, 9, 9, 9, 9, 9, 9,
        10, 10, 10, 10, 10, 10, 10, 10, 10,
        11, 11, 11, 11, 11, 11, 11, 11, 11,
        12, 12, 12, 12, 12, 12, 12, 12, 12,
        13, 13, 13, 13, 13, 13, 13, 13, 13,
        14, 14,
      };
  private static final int[] powers10 =
      new int[] {ten0, ten1, ten2, ten3, ten4, ten5, ten6, ten7, ten8, ten9};

  private static final BigInteger[] powers10BigInt =
      new BigInteger[] {
        BigInteger.valueOf(ten0),
        BigInteger.valueOf(ten1),
        BigInteger.valueOf(ten2),
        BigInteger.valueOf(ten3),
        BigInteger.valueOf(ten4),
        BigInteger.valueOf(ten5),
        BigInteger.valueOf(ten6),
        BigInteger.valueOf(ten7),
        BigInteger.valueOf(ten8),
        BigInteger.valueOf(ten9)
      };

  // A MyDecimal holds 9 words.
  private static final int maxWordBufLen = 9;
  private static final int maxFraction = 30;
  private static final int[] dig2bytes = new int[] {0, 1, 1, 2, 2, 3, 3, 4, 4, 4};

  // The following are fields of MyDecimal
  private int digitsInt;
  private int digitsFrac;
  private boolean negative;
  private int[] wordBuf = new int[maxWordBufLen];

  public MyDecimal() {}

  public MyDecimal(int digitsInt, int digitsFrac, boolean negative, int[] wordBuf) {
    this.digitsInt = digitsInt;
    this.digitsFrac = digitsFrac;
    this.negative = negative;
    this.wordBuf = wordBuf;
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
        x = (byte) b[start];
        break;
      case 2:
        x = (((byte) b[start]) << 8) + (b[start + 1] & 0xFF);
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

  /*
   * Returns total precision of this decimal. Basically, it is sum of digitsInt and digitsFrac. But there
   * are some special cases need to be token care of such as 000.001.
   * Precision reflects the actual effective precision without leading zero
   */
  public int precision() {
    int frac = this.digitsFrac;
    int digitsInt =
        this.removeLeadingZeros()[
            1]; /*this function return an array and the second element is digitsInt*/
    int precision = digitsInt + frac;
    // if no precision, it is just 0.
    if (precision == 0) {
      precision = 1;
    }
    return precision;
  }

  /**
   * Returns fraction digits that counts how many digits after ".". frac() reflects the actual
   * effective fraction without trailing zero
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
    String s = Double.toString(value);
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

    int _digitsInt = precision - frac;
    int _wordsInt = _digitsInt / digitsPerWord;
    int _leadingDigits = _digitsInt - _wordsInt * digitsPerWord;
    int _wordsFrac = frac / digitsPerWord;
    int trailingDigits = frac - _wordsFrac * digitsPerWord;
    int wordsIntTo = _wordsInt;
    if (_leadingDigits > 0) {
      wordsIntTo++;
    }
    int wordsFracTo = _wordsFrac;
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
        wordsIntTo = _wordsInt;
        wordsFracTo = wordBufLen - _wordsInt;
        truncated = true;
      }
    }

    if (overflow || truncated) {
      if (wordsIntTo < oldWordsIntTo) {
        binIdx += dig2bytes[_leadingDigits] + (_wordsInt - wordsIntTo) * wordSize;
      } else {
        trailingDigits = 0;
        _wordsFrac = wordsFracTo;
      }
    }

    this.negative = mask != 0;
    this.digitsInt = (byte) (_wordsInt * digitsPerWord + _leadingDigits);
    this.digitsFrac = (byte) (_wordsFrac * digitsPerWord + trailingDigits);

    int wordIdx = 0;
    if (_leadingDigits > 0) {
      int i = dig2bytes[_leadingDigits];
      int x = readWord(bin, i, binIdx);
      binIdx += i;
      this.wordBuf[wordIdx] = (x ^ mask) > 0 ? x ^ mask : (x ^ mask) & 0xFF;
      if (this.wordBuf[wordIdx] >= powers10[_leadingDigits + 1]) {
        throw new IllegalArgumentException("BadNumber");
      }
      if (this.wordBuf[wordIdx] != 0) {
        wordIdx++;
      } else {
        this.digitsInt -= _leadingDigits;
      }
    }
    for (int stop = binIdx + _wordsInt * wordSize; binIdx < stop; binIdx += wordSize) {
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

    for (int stop = binIdx + _wordsFrac * wordSize; binIdx < stop; binIdx += wordSize) {
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
    }

    return binSize;
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
   * Counts the number of digits of prefix zeroes. For 00.001, it returns two.
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
    if ((digits + digitsPerWord - 1) >= 0 && ((digits + digitsPerWord - 1) < 128)) {
      return div9[digits + digitsPerWord - 1];
    }
    return (digits + digitsPerWord - 1) / digitsPerWord;
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
  private void fromCharArray(char[] str) {
    int startIdx = 0;
    // found first character is not space and start from here
    for (; startIdx < str.length; startIdx++) {
      if (!Character.isSpaceChar(str[startIdx])) {
        break;
      }
    }

    if (str.length == 0) {
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
    // we initialize strIdx in case of sign notation, here we need subtract startIdx from strIdx
    // cause strIdx is used for counting the number of digits.
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
  }

  // Returns a decimal string.
  @Override
  public String toString() {
    char[] str;

    int _digitsFrac = this.digitsFrac;
    int[] res = removeLeadingZeros();
    int wordStartIdx = res[0];
    int digitsInt = res[1];
    if (digitsInt + _digitsFrac == 0) {
      digitsInt = 1;
      wordStartIdx = 0;
    }

    int digitsIntLen = digitsInt;
    if (digitsIntLen == 0) {
      digitsIntLen = 1;
    }
    int digitsFracLen = _digitsFrac;
    int length = digitsIntLen + digitsFracLen;
    if (this.negative) {
      length++;
    }
    if (_digitsFrac > 0) {
      length++;
    }
    str = new char[length];

    int strIdx = 0;
    if (this.negative) {
      str[strIdx] = '-';
      strIdx++;
    }

    if (_digitsFrac > 0) {
      int fracIdx = strIdx + digitsIntLen;
      int wordIdx = wordStartIdx + digitsToWords(digitsInt);
      str[fracIdx] = '.';
      fracIdx++;
      for (; _digitsFrac > 0; _digitsFrac -= digitsPerWord) {
        int x = this.wordBuf[wordIdx];
        wordIdx++;
        for (int i = Math.min(_digitsFrac, MyDecimal.digitsPerWord); i > 0; i--) {
          int y = x / digMask;
          str[fracIdx] = (char) ((char) y + '0');
          fracIdx++;
          x -= y * digMask;
          x *= 10;
        }
      }
    }
    if (digitsInt > 0) {
      strIdx += digitsInt;
      int wordIdx = wordStartIdx + digitsToWords(digitsInt);
      for (; digitsInt > 0; digitsInt -= digitsPerWord) {
        wordIdx--;
        int x = this.wordBuf[wordIdx];
        for (int i = Math.min(digitsInt, MyDecimal.digitsPerWord); i > 0; i--) {
          int temp = x / 10;
          strIdx--;
          str[strIdx] = (char) ('0' + (x - temp * 10));
          x = temp;
        }
      }
    } else {
      str[strIdx] = '0';
    }

    return new String(str);
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
   * as is 3. The first digitsInt % digitsPerWord digits are stored in the reduced number of bytes
   * (enough bytes to store this number of digits - see dig2bytes) 4. same for frac - full word are
   * stored as is, the last frac % digitsPerWord digits - in the reduced number of bytes. 5. If the
   * number is negative - every byte is inverted. 5. The very first bit of the resulting byte array
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

    int digitsInt = precision - frac; // how many digits before dot
    int wordsInt = digitsInt / digitsPerWord; // how many words to stores int part before dot.
    int leadingDigits = digitsInt - wordsInt * digitsPerWord; // first digits
    int wordsFrac = frac / digitsPerWord; // how many words to store int part after dot
    int trailingDigits = frac - wordsFrac * digitsPerWord; // last digits

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
      // TODO overflow here
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
      // TODO truncated
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

  /** Clears this instance. */
  public void clear() {
    this.digitsFrac = 0;
    this.digitsInt = 0;
    this.negative = false;
  }

  private BigInteger toBigInteger() {
    BigInteger x = BigInteger.ZERO;
    int wordIdx = 0;
    for (int i = this.digitsInt; i > 0; i -= digitsPerWord) {
      x = x.multiply(wordBaseBigInt).add(BigInteger.valueOf(this.wordBuf[wordIdx]));
      wordIdx++;
    }

    for (int i = this.digitsFrac; i > 0; i -= digitsPerWord) {
      x = x.multiply(wordBaseBigInt).add(BigInteger.valueOf(this.wordBuf[wordIdx]));
      wordIdx++;
    }

    if (digitsFrac % digitsPerWord != 0) {
      x = x.divide(powers10BigInt[digitsPerWord - digitsFrac % digitsPerWord]);
    }
    if (negative) {
      x = x.negate();
    }
    return x;
  }

  private long toLong() {
    long x = 0;
    int wordIdx = 0;
    for (int i = this.digitsInt; i > 0; i -= digitsPerWord) {
      x = x * wordBase + this.wordBuf[wordIdx];
      wordIdx++;
    }

    for (int i = this.digitsFrac; i > 0; i -= digitsPerWord) {
      x = x * wordBase + this.wordBuf[wordIdx];
      wordIdx++;
    }

    if (digitsFrac % digitsPerWord != 0) {
      x = x / powers10[digitsPerWord - digitsFrac % digitsPerWord];
    }

    if (negative) {
      x = -x;
    }
    return x;
  }

  public BigDecimal toBigDecimal() {
    // 19 is the length of digits of Long.MAX_VALUE
    // If a decimal can be expressed as a long value, we should use toLong method which has
    // better performance than toBigInteger.
    if ((digitsInt + digitsPerWord - 1) / digitsPerWord
            + (digitsFrac + digitsPerWord - 1) / digitsPerWord
        < (19 + digitsPerWord - 1) / digitsPerWord) {
      return new BigDecimal(BigInteger.valueOf(toLong()), digitsFrac);
    }
    return new BigDecimal(toBigInteger(), digitsFrac);
  }
}
