/*
 * Copyright 2022 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv.collation;

import static org.junit.Assert.assertEquals;

import com.pingcap.tikv.meta.Collation;
import com.pingcap.tikv.meta.collate.BinPaddingCollator;
import com.pingcap.tikv.meta.collate.GeneralCICollator;
import com.pingcap.tikv.meta.collate.UnicodeCICollator;
import org.junit.Test;

public class CollationTest {

    @Test
    public void testTruncateTailingSpace() {
        String a = Collation.truncateTailingSpace("abc   ");
        assertEquals("abc", a);
    }

    @Test
    public void testCollatorCompare() {
        String[][] table = {
                {"a", "b"},
                {"a", "A"},
                {"a", "a "},
                {"😜", "😃"},
                {"a\t", "a"},
                {"ß", "ss"},
                {"中文", "汉字"}
        };
        int[] BinPaddingCollatorExpected = {-1, 1, 0, 1, 1, 1, -1};
        int[] GeneralCICollatorExpected = {-1, 0, 0, 0, 1, -1, -1};
        int[] UnicodeCICollatorExpected = {-1, 0, 0, 0, 1, 0, -1};
        for (int i = 0; i < table.length; i++) {
            assertEquals(
                    BinPaddingCollatorExpected[i], BinPaddingCollator.compare(table[i][0], table[i][1]));
            assertEquals(
                    GeneralCICollatorExpected[i], GeneralCICollator.compare(table[i][0], table[i][1]));
            assertEquals(
                    UnicodeCICollatorExpected[i], UnicodeCICollator.compare(table[i][0], table[i][1]));
        }
    }

    @Test
    public void testCollatorKey() {
        checkEqual(new byte[]{0x61}, BinPaddingCollator.key("a"));
        checkEqual(new byte[]{0x0, 0x41}, GeneralCICollator.key("A"));
        checkEqual(new byte[]{0x0E, 0x33}, UnicodeCICollator.key("A"));
        checkEqual(
                new byte[]{
                        0x46,
                        0x6f,
                        0x6f,
                        0x20,
                        (byte) 0xc2,
                        (byte) 0xa9,
                        0x20,
                        0x62,
                        0x61,
                        0x72,
                        0x20,
                        (byte) 0xf0,
                        (byte) 0x9d,
                        (byte) 0x8c,
                        (byte) 0x86,
                        0x20,
                        0x62,
                        0x61,
                        0x7a,
                        0x20,
                        (byte) 0xe2,
                        (byte) 0x98,
                        (byte) 0x83,
                        0x20,
                        0x71,
                        0x75,
                        0x78
                },
                BinPaddingCollator.key("Foo © bar 𝌆 baz ☃ qux"));
        checkEqual(
                new byte[]{
                        0x0,
                        0x46,
                        0x0,
                        0x4f,
                        0x0,
                        0x4f,
                        0x0,
                        0x20,
                        0x0,
                        (byte) 0xa9,
                        0x0,
                        0x20,
                        0x0,
                        0x42,
                        0x0,
                        0x41,
                        0x0,
                        0x52,
                        0x0,
                        0x20,
                        (byte) 0xff,
                        (byte) 0xfd,
                        0x0,
                        0x20,
                        0x0,
                        0x42,
                        0x0,
                        0x41,
                        0x0,
                        0x5a,
                        0x0,
                        0x20,
                        0x26,
                        0x3,
                        0x0,
                        0x20,
                        0x0,
                        0x51,
                        0x0,
                        0x55,
                        0x0,
                        0x58
                },
                GeneralCICollator.key("Foo © bar 𝌆 baz ☃ qux"));
        checkEqual(
                new byte[]{
                        0x0E,
                        (byte) 0xB9,
                        0x0F,
                        (byte) 0x82,
                        0x0F,
                        (byte) 0x82,
                        0x02,
                        0x09,
                        0x02,
                        (byte) 0xC5,
                        0x02,
                        0x09,
                        0x0E,
                        0x4A,
                        0x0E,
                        0x33,
                        0x0F,
                        (byte) 0xC0,
                        0x02,
                        0x09,
                        (byte) 0xFF,
                        (byte) 0xFD,
                        0x02,
                        0x09,
                        0x0E,
                        0x4A,
                        0x0E,
                        0x33,
                        0x10,
                        0x6A,
                        0x02,
                        0x09,
                        0x06,
                        (byte) 0xFF,
                        0x02,
                        0x09,
                        0x0F,
                        (byte) 0xB4,
                        0x10,
                        0x1F,
                        0x10,
                        0x5A
                },
                UnicodeCICollator.key("Foo © bar 𝌆 baz ☃ qux"));
        checkEqual(new byte[]{0x61}, BinPaddingCollator.key("a "));
        checkEqual(new byte[]{0x0, 0x41}, GeneralCICollator.key("a "));
        checkEqual(new byte[]{0x0E, 0x33}, UnicodeCICollator.key("a "));
        checkEqual(
                new byte[]{(byte) 0xE4, (byte) 0xB8, (byte) 0xAD, (byte) 0xE6, (byte) 0x96, (byte) 0x87},
                BinPaddingCollator.key("中文"));
        checkEqual(new byte[]{0x4E, 0x2D, 0x65, (byte) 0x87}, GeneralCICollator.key("中文"));
        checkEqual(
                new byte[]{
                        (byte) 0xFB, 0x40, (byte) 0xCE, 0x2D, (byte) 0xFB, 0x40, (byte) 0xE5, (byte) 0x87
                },
                UnicodeCICollator.key("中文"));
    }

    void checkEqual(byte[] except, byte[] actual) {
        assertEquals(except.length, actual.length);
        for (int i = 0; i < except.length; i++) {
            assertEquals(except[i], actual[i]);
        }
    }
}
