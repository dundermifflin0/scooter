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
import com.hazelcast.jet.impl.processor.customwindow2.WindowSet.Value;

import java.io.Serializable;
import java.util.Map.Entry;

public interface WindowSet<T, A, S> extends Iterable<Entry<WindowDef, Value<A, S>>> {
    Value<A, S> accumulate(WindowDef windowDef, AggregateOperation1<T, A, ?> aggrOp, T item);

    void remove(WindowDef windowDef);

    boolean isEmpty();

    /** Private API */
    // TODO [viliam] better serialization
    class Value<A, S> implements Serializable {
        A accumulator;
        S triggerState;
        long eventTimerTime = Long.MIN_VALUE;
        long systemTimerTime = Long.MIN_VALUE;

        Value(A accumulator) {
            this.accumulator = accumulator;
        }
    }

    /** Private API */
    Value<A, S> getWindowData(WindowDef windowDef);
}
