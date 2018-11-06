/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://wwapache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.impl.processor.customwindow2;

import com.hazelcast.jet.impl.processor.customwindow2.CustomWindowP.WindowDef;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.StringJoiner;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.impl.util.Util.toLocalDateTime;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.System.arraycopy;

// TODO [viliam] custom serialization
public class MergingWindowSet<K, A, S> implements WindowSet<K, A, S>, Serializable {

    private int size;
    private long[] starts = new long[2];
    private long[] ends = new long[2];
    private Value<A, S>[] values = (Value<A, S>[]) new Value[2];

    @Nonnull @Override
    public WindowDef mergeWindow(@Nonnull K key, @Nonnull WindowDef window, @Nonnull WindowSetCallback<K, A, S> windowSetCallback) {
        int i;
        for (i = 0; i < size && starts[i] <= window.end(); i++) {
            // the entire window `i` is not  after the added window interval

            if (ends[i] < window.start()) {
                // the entire window `i` is before the added window interval
                continue;
            }
            if (starts[i] <= window.start() && ends[i] >= window.end()) {
                // the window `i` fully covers the added window interval
                return new WindowDef(starts[i], ends[i]);
            }
            // the window `i` overlaps the added window interval

            if (i + 1 == size || starts[i + 1] > window.end()) {
                // the window `i + 1` doesn't overlap the added window interval
                windowSetCallback.remove(key, values[i], new WindowDef(starts[i], ends[i]));
                starts[i] = min(starts[i], window.start());
                ends[i] = max(ends[i], window.end());
                return new WindowDef(starts[i], ends[i]);
            }
            // both `i` and `i + 1` windows overlap the added window interval
            windowSetCallback.merge(values[i], values[i + 1]);
            windowSetCallback.remove(key, values[i], new WindowDef(starts[i], ends[i]));
            windowSetCallback.remove(key, values[i + 1], new WindowDef(starts[i + 1], ends[i + 1]));
            ends[i] = ends[i + 1];
            removeWindow(i + 1);
            return new WindowDef(starts[i], ends[i]);
        }
        insertWindow(i, window);
        return window;
    }

    @Override
    public void remove(@Nonnull WindowDef windowDef) {
        for (int i = 0; i < size; i++) {
            if (starts[i] == windowDef.start() && ends[i] == windowDef.end()) {
                removeWindow(i);
            }
        }
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Nullable @Override
    public Value<A, S> get(@Nonnull WindowDef windowDef) {
        for (int i = 0; i < size; i++) {
            if (starts[i] == windowDef.start() && ends[i] == windowDef.end()) {
                return values[i];
            }
        }
        return null;
    }

    @Nonnull @Override
    public Iterator<Entry<WindowDef, Value<A, S>>> iterator() {
        return new Iterator<Entry<WindowDef, Value<A, S>>>() {
            int pos;

            @Override
            public boolean hasNext() {
                return pos < size;
            }

            @Override
            public Entry<WindowDef, Value<A, S>> next() {
                if (pos == size) {
                    throw new NoSuchElementException();
                }
                try {
                    return entry(new WindowDef(starts[pos], ends[pos]), values[pos]);
                } finally {
                    pos++;
                }
            }
        };
    }

//    private List<OUT> closeWindows(Windows<A> w, K key, long wm) {
//        if (w == null) {
//            return emptyList();
//        }
//        List<OUT> results = new ArrayList<>();
//        int i = 0;
//        for (; i < size && ends[i] < wm; i++) {
//            OUT out = mapToOutputFn.apply(starts[i], ends[i], key, aggrOp.finishFn().apply(values[i]));
//            if (out != null) {
//                results.add(out);
//            }
//        }
//        if (i != size) {
//            removeHead(i);
//        } else {
//            keyToWindows.remove(key);
//            totalKeys.set(keyToWindows.size());
//        }
//        return results;
//    }

    private void insertWindow(int idx, WindowDef windowDef) {
        expandIfNeeded();
        copy(idx, idx + 1, size - idx);
        size++;
        starts[idx] = windowDef.start();
        ends[idx] = windowDef.end();
        values[idx] = new Value<>();
    }

    private void removeWindow(int idx) {
        size--;
        copy(idx + 1, idx, size - idx);
    }

    private void removeHead(int count) {
        copy(count, 0, size - count);
        size -= count;
    }

    private void copy(int from, int to, int length) {
        arraycopy(starts, from, starts, to, length);
        arraycopy(ends, from, ends, to, length);
        arraycopy(values, from, values, to, length);
    }

    private void expandIfNeeded() {
        if (size == starts.length) {
            starts = Arrays.copyOf(starts, 2 * starts.length);
            ends = Arrays.copyOf(ends, 2 * ends.length);
            values = Arrays.copyOf(values, 2 * values.length);
        }
    }

    @Override
    public String toString() {
        StringJoiner sj = new StringJoiner(", ", getClass().getSimpleName() + '{', "}");
        for (int i = 0; i < size; i++) {
            sj.add("[s=" + toLocalDateTime(starts[i]).toLocalTime()
                    + ", e=" + toLocalDateTime(ends[i]).toLocalTime()
                    + ", a=" + values[i] + ']');
        }
        return sj.toString();
    }
}
