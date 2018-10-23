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

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.accumulator.MutableReference;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.Watermark;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFine;
import static com.hazelcast.jet.impl.util.Util.lazyIncrement;
import static com.hazelcast.jet.impl.util.Util.logLateEvent;

public class CustomWindowP<T, K, S, R> extends AbstractProcessor {
    private final CustomWindowHandler<T, K, S, R> handler;
    @Nonnull
    private final List<ToLongFunction<Object>> timestampFns;
    @Nonnull
    private final List<Function<Object, K>> keyFns;

    private final Map<K, S> states = new HashMap<>();
    private Traverser<R> traverser = Traversers.empty();
    private final MutableReference<S> stateRef = new MutableReference<>();
    private long currentWatermark = Long.MIN_VALUE;
    private Traverser snapshotTraverser;
    private ProcessingGuarantee processingGuarantee;
    private long minRestoredCurrentWatermark = Long.MAX_VALUE;

    @Probe
    private AtomicLong lateEventsDropped = new AtomicLong();

    public CustomWindowP(CustomWindowHandler<T, K, S, R> handler,
                         @Nonnull List<? extends ToLongFunction<?>> timestampFns,
                         @Nonnull List<? extends Function<?, ? extends K>> keyFns
    ) {
        this.handler = handler;
        this.timestampFns = (List<ToLongFunction<Object>>) timestampFns;
        this.keyFns = (List<Function<Object, K>>) keyFns;
    }

    @Override
    protected void init(@Nonnull Context context) {
        processingGuarantee = context.processingGuarantee();
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
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
        states.compute(key, (k, state) -> {
            stateRef.set(state);
            traverser = handler.onItem(ordinal, castedItem, key, timestamp, stateRef);
            return stateRef.get();
        });
        return emitFromTraverser(traverser);
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark wm) {
        if (!emitFromTraverser(traverser)) {
            return false;
        }
        currentWatermark = wm.timestamp();
        traverser = handler.onWatermark(wm, states);
        return emitFromTraverser(traverser);
    }

    @Override
    public boolean tryProcess() {
        if (!emitFromTraverser(traverser)) {
            return false;
        }
        traverser = handler.onTryProcess(states);
        return emitFromTraverser(traverser);
    }

    @Override
    public boolean saveToSnapshot() {
        if (snapshotTraverser == null) {
            snapshotTraverser = Traversers.<Object>traverseIterable(states.entrySet())
                    .append(entry(broadcastKey(Keys.CURRENT_WATERMARK), currentWatermark))
                    .onFirstNull(() -> snapshotTraverser = null);
        }
        return emitFromTraverserToSnapshot(snapshotTraverser);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        if (key instanceof BroadcastKey) {
            BroadcastKey bcastKey = (BroadcastKey) key;
            if (!Keys.CURRENT_WATERMARK.equals(bcastKey.key())) {
                throw new JetException("Unexpected broadcast key: " + bcastKey.key());
            }
            long newCurrentWatermark = (long) value;
            assert processingGuarantee != EXACTLY_ONCE
                    || minRestoredCurrentWatermark == Long.MAX_VALUE
                    || minRestoredCurrentWatermark == newCurrentWatermark
                    : "different values for currentWatermark restored, before=" + minRestoredCurrentWatermark
                    + ", new=" + newCurrentWatermark;
            minRestoredCurrentWatermark = Math.min(newCurrentWatermark, minRestoredCurrentWatermark);
            return;
        }

        if (states.put((K) key, (S) value) != null) {
            throw new JetException("Duplicate key in snapshot: " + key);
        }
    }

    @Override
    public boolean finishSnapshotRestore() {
        currentWatermark = minRestoredCurrentWatermark;
        logFine(getLogger(), "Restored currentWatermark from snapshot to: %s", currentWatermark);
        handler.init(states);
        return true;
    }

    // package-visible for test
    enum Keys {
        CURRENT_WATERMARK
    }
}
