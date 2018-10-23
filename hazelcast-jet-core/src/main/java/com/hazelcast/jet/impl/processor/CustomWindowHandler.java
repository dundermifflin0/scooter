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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.accumulator.MutableReference;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Watermark;

import javax.annotation.Nonnull;
import java.util.Map;

import static com.hazelcast.util.Preconditions.checkPositive;

/**
 * @param <T> Input item type
 * @param <S> State object type
 * @param <R> Output item type
 */
public interface CustomWindowHandler<T, K, S, R> {

    default void init(Map<K, S> states) {
    }

    /**
     * Called when an item to process is received
     *
     * @param ordinal
     * @param item  Item to handle
     * @param key
     * @param timestamp
     * @param state 1-element array with state object. Null initially, can be mutated
     * @return Traverser (possibly empty) to emit
     */
    @Nonnull
    Traverser<R> onItem(int ordinal, @Nonnull T item, K key, long timestamp, @Nonnull MutableReference<S> state);

    /**
     * Called when a watermark is received, denoting the event time change.
     * @param states    All states
     * @return Traverser (possibly empty) to emit
     */
    @Nonnull
    Traverser<R> onWatermark(@Nonnull Watermark watermark, Map<K, S> states);

    /**
     * Called when {@link Processor#tryProcess()} is called
     * @param states All states
     * @return Traverser (possibly empty) to emit
     */
    @Nonnull
    Traverser<R> onTryProcess(Map<K, S> states);
}

class CountWindow<T, K, A, R> implements CustomWindowHandler<T, K, CountWindow.State<A>, R> {
    private final AggregateOperation1<T, A, R> aggrOp;
    private final int maxCount;

    public CountWindow(AggregateOperation1<T, A, R> aggrOp, int maxCount) {
        checkPositive(maxCount, "maxCount must be >= 1");
        this.aggrOp = aggrOp;
        this.maxCount = maxCount;
    }

    @Nonnull @Override
    public Traverser<R> onItem(int ordinal, @Nonnull T item, K key, long timestamp, @Nonnull MutableReference<State<A>> stateRef) {
        State<A> state = stateRef.createIfAbsent(() -> new State(aggrOp.createFn().get()));
        state.count++;
        aggrOp.accumulateFn().accept(state.acc, item);
        Traverser<R> res = Traversers.empty();
        if (state.count == maxCount) {
            res = Traversers.traverseItems(aggrOp.finishFn().apply(state.acc));
            state.count = 0;
            state.acc = aggrOp.createFn().get();
        }
        return res;
    }

    @Nonnull @Override
    public Traverser<R> onWatermark(@Nonnull Watermark watermark, Map<K, State<A>> states) {
        return Traversers.empty();
    }

    @Nonnull @Override
    public Traverser<R> onTryProcess(Map<K, State<A>> states) {
        return Traversers.empty();
    }

    public static final class State<A> {
        int count;
        A acc;

        public State(A acc) {
            this.acc = acc;
        }
    }
}
