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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

// TODO [viliam] custom serialization
class OverlappingWindowSet<K, A, S> implements WindowSet<K, A, S>, Serializable {

    private final Map<WindowDef, Value<A, S>> windows = new HashMap<>();

    OverlappingWindowSet() {
    }

    @Nonnull @Override
    public WindowDef mergeWindow(@Nonnull K key,
                                 @Nonnull WindowDef window,
                                 @Nonnull WindowSetCallback<K, A, S> windowSetCallback) {
        windows.computeIfAbsent(window, k -> new Value<>());
        return window;
    }

    @Nullable
    @Override
    public Value<A, S> get(@Nonnull WindowDef windowDef) {
        return windows.get(windowDef);
    }

    @Override
    public void remove(@Nonnull WindowDef windowDef) {
        windows.remove(windowDef);
    }

    @Override
    public boolean isEmpty() {
        return windows.isEmpty();
    }

    @Override
    public int size() {
        return windows.size();
    }

    @Nonnull @Override
    public Iterator<Entry<WindowDef, Value<A, S>>> iterator() {
        return windows.entrySet().iterator();
    }
}
