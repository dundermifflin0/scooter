/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.impl.processor.customwindow;

import com.hazelcast.jet.JetException;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

public class OverlappingWindowSet<T> implements WindowSet<T> {

    private final Map<TwoLongs, Value<T>> data = new HashMap<>();

    @Override
    public T getOrCreate(long start, long end, Supplier<T> createFn) {
        return data.computeIfAbsent(new TwoLongs(start, end), k -> new Value(createFn.get())).value;
    }

    @Override
    public void mark(long start, long end, boolean fire, boolean purge) {
        Value<T> v = data.get(new TwoLongs(start, end));
        if (v == null) {
            throw new JetException("Window not found: (" + start + ", " + end + ")");
        }
        v.fire = fire;
        v.purge = purge;
    }

    private static class TwoLongs {
        final long start;
        final long end;

        private TwoLongs(long start, long end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TwoLongs twoLongs = (TwoLongs) o;
            return start == twoLongs.start &&
                    end == twoLongs.end;
        }

        @Override
        public int hashCode() {
            return Objects.hash(start, end);
        }
    }

    private static class Value<T> {
        boolean fire;
        boolean purge;
        T value;

        public Value(T value) {
            this.value = value;
        }
    }
}
