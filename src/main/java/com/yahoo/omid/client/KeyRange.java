/**
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package com.yahoo.omid.client;

import java.util.Arrays;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MurmurHash;
import com.yahoo.omid.tso.RowKey;

public class KeyRange implements Comparable<KeyRange> {
    private byte[] lower = null;
    private byte[] upper = null;

    public KeyRange(RowKey key) {
        this.lower = key.getByteArray();
        this.upper = this.lower;
    }

    public KeyRange(byte[] lower, byte[] upper) {
        this.lower = lower;
        this.upper = upper;
    }

    public KeyRange(String lower, String upper) {
        this.lower = lower == null ? null : lower.getBytes();
        this.upper = upper == null ? null : upper.getBytes();
    }

    @Override
    public String toString() {
        return "KeyRange: [" + Bytes.toString(lower) + "," + Bytes.toString(upper) + ")";
    }

    public boolean includes(RowKey key) {
        byte[] keyInBytes = key.getByteArray();
        return (compareTo(this.lower,keyInBytes) <= 0 &&
                (upper == null || compareTo(this.upper,keyInBytes) > 0));
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof KeyRange))
            return false;
        KeyRange other = (KeyRange) obj;
        return Bytes.equals(other.lower, lower) 
            && Bytes.equals(other.upper, upper);
    }

    int hash;
    @Override
    public int hashCode() {
        if (hash != 0) {
            return hash;
        }
        byte[] key;
        if (lower == null)
            key = upper;
        else if (upper == null)
            key = lower;
        else {
            key = Arrays.copyOf(lower, lower.length + upper.length);
            System.arraycopy(upper, 0, key, lower.length, upper.length);
        }
        hash = MurmurHash.getInstance().hash(key, 0, key.length, 0xbeefdead);
        return hash;
    }

    @Override
    public int compareTo(KeyRange other) {
        return compareTo(this.lower, other.lower);
    }

    public int compareTo(byte[] me, byte[] other) {
        if (me == null && other == null)
            return 0;
        else if (me == null)
            return -1;
        else if (other == null)
            return 1;
        else
            return Bytes.compareTo(me, other);
    }
}


