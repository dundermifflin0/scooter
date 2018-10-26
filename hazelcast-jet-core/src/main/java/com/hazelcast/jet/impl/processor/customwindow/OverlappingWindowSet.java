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
import com.hazelcast.jet.aggregate.AggregateOperation1;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Supplier;

class OverlappingWindowSet<T, A, S> implements WindowSet<T, A, S> {

    private final AggregateOperation1 aggrOp;
    private final Map<TwoLongs, Value<A, S>> data = new HashMap<>();

    OverlappingWindowSet(AggregateOperation1<T, A, ?> aggrOp) {
        this.aggrOp = aggrOp;
    }

    @Override
    public S accumulate(T item, long start, long end, Supplier<S> state) {
        Value<A, S> value =
                data.computeIfAbsent(new TwoLongs(start, end), k -> new Value(aggrOp.createFn().get(), state.get()));
        aggrOp.accumulateFn().accept(value.accumulator, item);
        return value.handlerState;
    }

    @Override
    public void mark(long start, long end, int action) {
        Value<A, S> v = data.get(new TwoLongs(start, end));
        if (v == null) {
            throw new JetException("Window not found: (" + start + ", " + end + ")");
        }
        v.action = action;
    }

    @Nonnull @Override
    public Iterator<Entry<TwoLongs, Value<A, S>>> iterator() {
        return data.entrySet().iterator();
    }
}
