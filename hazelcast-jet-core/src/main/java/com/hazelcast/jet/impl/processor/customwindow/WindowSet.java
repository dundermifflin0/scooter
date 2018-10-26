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

import com.hazelcast.jet.impl.processor.customwindow.WindowSet.TwoLongs;
import com.hazelcast.jet.impl.processor.customwindow.WindowSet.Value;

import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Supplier;

public interface WindowSet<T, A, S> extends Iterable<Entry<TwoLongs, Value<A, S>>> {
    S accumulate(T item, long start, long end, Supplier<S> state);
    void mark(long start, long end, int action);

    class TwoLongs {
        final long start;
        final long end;

        TwoLongs(long start, long end) {
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

    class Value<A, S> {
        A accumulator;
        S handlerState;
        int action;

        Value(A accumulator, S handlerState) {
            this.accumulator = accumulator;
            this.handlerState = handlerState;
        }
    }
}
