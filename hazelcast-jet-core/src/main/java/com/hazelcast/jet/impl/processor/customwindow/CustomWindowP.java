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

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.function.KeyedWindowResultFunction;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.impl.processor.customwindow.CustomWindowHandler.NO_ACTION;
import static com.hazelcast.jet.impl.processor.customwindow.CustomWindowHandler.isFire;
import static com.hazelcast.jet.impl.processor.customwindow.CustomWindowHandler.isPurge;
import static com.hazelcast.jet.impl.util.Util.lazyIncrement;
import static com.hazelcast.jet.impl.util.Util.logLateEvent;

class CustomWindowP<T, K, A, R, S, OUT> extends AbstractProcessor {
    private final CustomWindowHandler<T, A, S> handler;
    private final List<ToLongFunction<Object>> timestampFns;
    private final List<Function<Object, K>> keyFns;
    private final AggregateOperation1<T, A, R> aggrOp;
    private final KeyedWindowResultFunction<? super K, ? super R, OUT> mapToOutputFn;

    private Traverser<OUT> traverser = Traversers.empty();
    private long currentWatermark = Long.MIN_VALUE;

    private final Map<K, WindowSet<T, A, S>> state = new HashMap<>();

    @Probe
    private AtomicLong lateEventsDropped = new AtomicLong();

    private final FlatMapper watermarkFlatMapper = flatMapper(this::watermarkToTraverser);

    public CustomWindowP(
            CustomWindowHandler<T, A, S> handler,
            List<ToLongFunction<Object>> timestampFns,
            List<Function<Object, K>> keyFns,
            AggregateOperation1<T, A, R> aggrOp,
            @Nonnull KeyedWindowResultFunction<? super K, ? super R, OUT> mapToOutputFn
    ) {
        this.handler = handler;
        this.timestampFns = timestampFns;
        this.keyFns = keyFns;
        this.aggrOp = aggrOp;
        this.mapToOutputFn = mapToOutputFn;
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        if (!emitFromTraverser(traverser)) {
            return false;
        }
        @SuppressWarnings("unchecked")
        final long timestamp = timestampFns.get(ordinal).applyAsLong(item);
        K key = keyFns.get(ordinal).apply(item);
        if (timestamp < currentWatermark) {
            logLateEvent(getLogger(), currentWatermark, item);
            lazyIncrement(lateEventsDropped);
            return true;
        }
        T castedItem = (T) item;

        WindowSet<T, A, S> windowSet = state.computeIfAbsent(key, k -> new OverlappingWindowSet<>(aggrOp));
        handler.onItem(castedItem, timestamp, windowSet);
        traverser = traverseIterable(windowSet)
                .removeIf(e -> isPurge(e.getValue().action))
                .filter(e -> {
                    boolean fire = isFire(e.getValue().action);
                    e.getValue().action = NO_ACTION;
                    return fire;
                })
                .map(e -> mapToOutputFn.apply(e.getKey().start, e.getKey().end, key,
                        ((Function<A, R>) (isPurge(e.getValue().action) ? aggrOp.finishFn() : aggrOp.exportFn()))
                                .apply(e.getValue().accumulator)));

        return emitFromTraverser(traverser);
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark wm) {
        return watermarkFlatMapper.tryProcess(wm);
    }

    private Traverser<OUT> watermarkToTraverser(Watermark wm) {
        return traverseIterable(state.entrySet())
                .flatMap(entry -> traverseIterable(entry.getValue())
                        .filter(wsEntry -> !isFire(handler.onWatermark(wm.timestamp(), wsEntry.getKey().start, wsEntry.getKey().end,
                                wsEntry.getValue().handlerState)))
                        .map(wsEntry -> mapToOutputFn.apply(wsEntry.getKey().start, wsEntry.getKey().end, entry.getKey(),
                                aggrOp.exportFn().apply(wsEntry.getValue().accumulator)))
                );
    }
}
