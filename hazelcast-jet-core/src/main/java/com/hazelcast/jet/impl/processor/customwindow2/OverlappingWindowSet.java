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

package com.hazelcast.jet.impl.processor.customwindow2;

import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.impl.processor.customwindow2.CustomWindowP.WindowDef;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

// TODO [viliam] custom serialization
class OverlappingWindowSet<T, A, S> implements WindowSet<T, A, S>, Serializable {

    private final Map<WindowDef, Value<A, S>> windows = new HashMap<>();

    OverlappingWindowSet() {
    }

    @Override
    public Value<A, S> accumulate(WindowDef windowDef, AggregateOperation1<T, A, ?> aggrOp, T item) {
        Value<A, S> value =
                windows.computeIfAbsent(windowDef, k -> new Value(aggrOp.createFn().get()));
        aggrOp.accumulateFn().accept(value.accumulator, item);
        return value;
    }

    @Override
    public void remove(WindowDef windowDef) {
        windows.remove(windowDef);
    }

    @Override
    public boolean isEmpty() {
        return windows.isEmpty();
    }

    @Override
    public Value<A, S> getWindowData(WindowDef windowDef) {
        return windows.get(windowDef);
    }

    @Nonnull @Override
    public Iterator<Entry<WindowDef, Value<A, S>>> iterator() {
        return windows.entrySet().iterator();
    }
}
