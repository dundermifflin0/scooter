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

import com.hazelcast.jet.impl.processor.customwindow2.CustomWindowP.WindowDef;
import com.hazelcast.jet.impl.processor.customwindow2.WindowSet.Value;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Map.Entry;

public interface WindowSet<K, A, S> extends Iterable<Entry<WindowDef, Value<A, S>>> {
    /**
     * Add the given {@code window} to the window set. Returns a new WindowDef
     * containing the window, which might be the result of windows merging.
     *
     * @param windowSetCallback a callback to merge window in 2nd argument into
     *                     window in the 1st argument
     */
    @Nonnull
    WindowDef mergeWindow(@Nonnull K key,
                          @Nonnull WindowDef window,
                          @Nonnull WindowSetCallback<K, A, S> windowSetCallback);

    void remove(@Nonnull WindowDef windowDef);

    boolean isEmpty();

    int size();

    /** Private API */
    // TODO [viliam] better serialization
    class Value<A, S> implements Serializable {
        A accumulator;
        S triggerState;
        long eventTimerTime = Long.MIN_VALUE;
        long systemTimerTime = Long.MIN_VALUE;
    }

    /** Private API */
    @Nullable
    Value<A, S> get(@Nonnull WindowDef windowDef);

    interface WindowSetCallback<K, A, S> {
        void merge(Value<A, S> mergeTarget, Value<A, S> mergeSource);
        void remove(K key, Value<A, S> value, WindowDef sourceWindow);
    }
}
