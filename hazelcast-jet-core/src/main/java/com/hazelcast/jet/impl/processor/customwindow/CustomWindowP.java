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

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.impl.processor.customwindow.CustomWindowHandler.HandlerContext;
import com.hazelcast.jet.impl.processor.customwindow.CustomWindowHandler.Window;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

class CustomWindowP<T, K, S, A, R> extends AbstractProcessor {
    private final CustomWindowHandler<T, K, A> handler;
    private final AggregateOperation1<T, A, R> aggrOp;
    private final DistributedFunction<T, K> extractKeyF;
    private final Map<K, KeyState<S, A>> states = new HashMap<>();
    private KeyState<S, A> currentState;
    private final Traverser<R> traverser = Traversers.empty();
    private HandlerContext handlerContext = () -> currentWatermark;
    private T currentItem;

    public CustomWindowP(CustomWindowHandler<T, K, A> handler, AggregateOperation1<T, A, R> aggrOp,
                         DistributedFunction<T, K> extractKeyF) {
        this.handler = handler;
        this.aggrOp = aggrOp;
        this.extractKeyF = extractKeyF;
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        if (!emitFromTraverser(traverser)) {
            return false;
        }
        outputList.clear();
        currentItem = (T) item;
        K key = extractKeyF.apply(currentItem);
        currentState = states.computeIfAbsent(key, k -> new KeyState<>(handler.createState()));
        handler.onItem(currentItem, , currentState.handlerState, handlerContext);
        return emitFromTraverser(traverser);
    }

    private void addToWindowCallback(long start, long end, boolean emitNow) {
        A acc = currentState.windowSet.computeIfAbsent(new Window(start, end), k -> aggrOp.createFn().get());
        aggrOp.accumulateFn().accept(acc, currentItem);
        if (emitNow) {
            outputList.add(aggrOp.exportFn().apply(acc));
        }
    }

    private static class KeyState<S, A> {
        final S handlerState;
        final Map<Window, A> windowSet = new HashMap<>();

        public KeyState(S handlerState) {
            this.handlerState = handlerState;
        }
    }
}
