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

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.AppendableTraverser;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.function.KeyedWindowResultFunction;
import com.hazelcast.jet.impl.processor.customwindow2.Trigger.Timers;
import com.hazelcast.jet.impl.processor.customwindow2.Trigger.TriggerAction;
import com.hazelcast.jet.impl.processor.customwindow2.WindowSet.Value;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.ToLongFunction;

import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFine;
import static com.hazelcast.jet.impl.util.Util.lazyIncrement;
import static com.hazelcast.jet.impl.util.Util.logLateEvent;
import static com.hazelcast.util.Preconditions.checkTrue;

/**
 * @param <IN>  input item type
 * @param <K>   input item key type
 * @param <A>   aggrOp's accumulator type
 * @param <R>   aggrOp's result type
 * @param <S>   triggerFn state type
 * @param <OUT> output item type
 */
public class CustomWindowP<IN, K, A, R, S, OUT> extends AbstractProcessor {

    private final List<ToLongFunction<Object>> timestampFns;
    private final List<Function<Object, K>> keyFns;
    private final AggregateOperation1<IN, A, R> aggrOp;
    private final DistributedSupplier<? extends WindowSet> createWindowSetFn;
    private final DistributedBiFunction<IN, Long, Collection<WindowDef>> windowFn;
    private final Trigger<IN, S> trigger;
    private final KeyedWindowResultFunction<? super K, ? super R, OUT> mapToOutputFn;
    private final LongSupplier clock;

    private final Map<K, WindowSet<IN, A, S>> windowSets = new HashMap<>();
    private final TreeMap<Long, Set<Tuple2<K, WindowDef>>> eventTimers = new TreeMap<>();
    private final TreeMap<Long, Set<Tuple2<K, WindowDef>>> systemTimers = new TreeMap<>();
    private long currentWatermark = Long.MIN_VALUE;
    private ProcessingGuarantee processingGuarantee;
    private long minRestoredCurrentWatermark = Long.MAX_VALUE;
    private ImmediateTimersImpl immediateTimersImpl = new ImmediateTimersImpl();
    private LazyTimersImpl lazyTimersImpl = new LazyTimersImpl();
    private final AppendableTraverser<OUT> appendableTraverser = new AppendableTraverser<>(16);
    private Traverser<OUT> onTimerTraverser = Traversers.empty();
    private Traverser snapshotTraverser;

    @Probe
    private AtomicLong lateEventsDropped = new AtomicLong();
    @Probe
    private AtomicLong totalKeys = new AtomicLong();
    @Probe
    private AtomicLong totalWindows = new AtomicLong();

    public CustomWindowP(
            @Nonnull List<? extends ToLongFunction<?>> timestampFns,
            @Nonnull List<? extends Function<?, ? extends K>> keyFns,
            @Nonnull AggregateOperation1<IN, A, R> aggrOp,
            @Nonnull DistributedSupplier<WindowSet> createWindowSetFn,
            @Nonnull DistributedBiFunction<IN, Long, Collection<WindowDef>> windowFn,
            @Nonnull Trigger<IN, S> trigger,
            @Nonnull KeyedWindowResultFunction<? super K, ? super R, OUT> mapToOutputFn,
            @Nonnull LongSupplier clock
    ) {
        checkTrue(keyFns.size() == aggrOp.arity(), keyFns.size() + " key functions " +
                "provided for " + aggrOp.arity() + "-arity aggregate operation");
        this.timestampFns = (List<ToLongFunction<Object>>) timestampFns;
        this.keyFns = (List<Function<Object, K>>) keyFns;
        this.aggrOp = aggrOp;
        this.createWindowSetFn = createWindowSetFn;
        this.windowFn = windowFn;
        this.trigger = trigger;
        this.mapToOutputFn = mapToOutputFn;
        this.clock = clock;
    }

    @Override
    protected void init(@Nonnull Context context) {
        processingGuarantee = context.processingGuarantee();
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        if (!emitFromTraverser(appendableTraverser)) {
            return false;
        }
        final long timestamp = timestampFns.get(ordinal).applyAsLong(item);
        if (timestamp < currentWatermark) {
            logLateEvent(getLogger(), currentWatermark, item);
            lazyIncrement(lateEventsDropped);
            return true;
        }

        K key = keyFns.get(ordinal).apply(item);
        IN castedItem = (IN) item;
        Collection<WindowDef> windowDefs = windowFn.apply(castedItem, timestamp);
        WindowSet<IN, A, S> windowSet = windowSets.computeIfAbsent(key, k -> createWindowSetFn.get());
        for (WindowDef windowDef : windowDefs) {
            Value<A, S> windowData = windowSet.accumulate(windowDef, aggrOp, castedItem);
            immediateTimersImpl.reset(key, windowDef, windowData);
            TriggerAction triggerAction = trigger.onItem(castedItem, timestamp, windowDef, windowData.triggerState,
                    immediateTimersImpl);
            OUT out = handleTriggerAction(windowSet, triggerAction, immediateTimersImpl);
            if (out != null) {
                appendableTraverser.append(out);
            }
        }

        return emitFromTraverser(appendableTraverser);
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark wm) {
        // TODO [viliam] extract method ref to reduce GC litter
        boolean res = handleTimers(eventTimers, wm.timestamp(), trigger::onEventTime);
        if (res) {
            // TODO purge windows even if triggers didn't say so
        }
        return res;
    }

    @Override
    public boolean tryProcess() {
        // TODO [viliam] extract method ref to reduce GC litter
        return handleTimers(systemTimers, clock.getAsLong(), trigger::onSystemTime);
    }

    @Override
    public boolean saveToSnapshot() {
        if (snapshotTraverser == null) {
            snapshotTraverser = Traversers.<Object>traverseIterable(windowSets.entrySet())
                    .append(entry(broadcastKey(Keys.CURRENT_WATERMARK), currentWatermark))
                    .onFirstNull(() -> snapshotTraverser = null);
        }
        return emitFromTraverserToSnapshot(snapshotTraverser);
    }

    @Override
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

        WindowSet<IN, A, S> value1 = (WindowSet<IN, A, S>) value;
        if (windowSets.put((K) key, value1) != null) {
            throw new JetException("Duplicate key in snapshot: " + key);
        }
        for (Entry<WindowDef, Value<A, S>> window : value1) {
            if (window.getValue().eventTimerTime != Long.MIN_VALUE) {
                eventTimers.computeIfAbsent(window.getValue().eventTimerTime, x -> new HashSet<>())
                      .add(tuple2((K) key, window.getKey()));
            }
            if (window.getValue().systemTimerTime != Long.MIN_VALUE) {
                systemTimers.computeIfAbsent(window.getValue().systemTimerTime, x -> new HashSet<>())
                      .add(tuple2((K) key, window.getKey()));
            }
        }
    }

    @Override
    public boolean finishSnapshotRestore() {
        currentWatermark = minRestoredCurrentWatermark;
        totalKeys.set(windowSets.size());
        logFine(getLogger(), "Restored currentWatermark from snapshot to: %s", currentWatermark);
        return true;
    }

    private boolean handleTimers(TreeMap<Long, Set<Tuple2<K, WindowDef>>> timers, long time,
                                 TriggerHandler<S> triggerHandler) {
        if (!emitFromTraverser(onTimerTraverser)) {
            return false;
        }
        SortedMap<Long, Set<Tuple2<K, WindowDef>>> timersToExecute = timers.headMap(time, true);
        onTimerTraverser = traverseStream(timersToExecute.entrySet().stream()
                .flatMap(entry -> entry.getValue().stream()
                             .map(keyAndWindow -> {
                                 try {
                                     WindowSet<IN, A, S> windowSet = windowSets.get(keyAndWindow.f0());
                                     Value<A, S> windowData = windowSet.getWindowData(keyAndWindow.f1());
                                     lazyTimersImpl.reset(keyAndWindow.f0(), keyAndWindow.f1(), windowData);
                                     TriggerAction action = triggerHandler.onTimer(entry.getKey(), keyAndWindow.f1(),
                                             windowData.triggerState, lazyTimersImpl);
                                     return handleTriggerAction(windowSet, action, lazyTimersImpl);
                                 } catch (Exception e) {
                                     throw sneakyThrow(e);
                                 }
                             })))
        .onFirstNull(() -> {
            timersToExecute.clear();
            lazyTimersImpl.applyLazyTimers();
        });

        return emitFromTraverser(onTimerTraverser);
    }

    private OUT handleTriggerAction(WindowSet<IN, A, S> windowSet, TriggerAction triggerAction,
                                    TimersExt timersImpl) {
        OUT out = null;
        if (triggerAction.fire) {
            R winResult = triggerAction.purge
                    ? aggrOp.finishFn().apply(timersImpl.windowData.accumulator)
                    : aggrOp.exportFn().apply(timersImpl.windowData.accumulator);
            out = mapToOutputFn.apply(timersImpl.windowDef.start, timersImpl.windowDef.end, timersImpl.key, winResult);
        }
        if (triggerAction.purge) {
            timersImpl.removeEventTimeTimer(timersImpl.windowData.eventTimerTime);
            timersImpl.removeSystemTimeTimer(timersImpl.windowData.systemTimerTime);
            windowSet.remove(timersImpl.windowDef);
            if (windowSet.isEmpty()) {
                windowSets.remove(timersImpl.key);
            }
        }
        return out;
    }

    private void unscheduleTimer(SortedMap<Long, Set<Tuple2<K, WindowDef>>> timers, K key, WindowDef windowDef,
                                 long time) {
        if (time != Long.MIN_VALUE) {
            Set<Tuple2<K, WindowDef>> timersForTime = timers.get(time);
            if (timersForTime != null) {
                if (timersForTime.size() == 1) {
                    timers.remove(time);
                } else {
                    timersForTime.remove(tuple2(key, windowDef));
                }
            }
        }
    }

    // TODO [viliam] better serialization
    public static class WindowDef implements Serializable {
        private final long start;
        private final long end;

        public WindowDef(long start, long end) {
            assert end >= start : "negative-size window. start=" + start + ", end=" + end;
            this.start = start;
            this.end = end;
        }

        public long start() {
            return start;
        }

        public long end() {
            return end;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            WindowDef windowDef = (WindowDef) o;
            return start == windowDef.start && end == windowDef.end;
        }

        @Override
        public int hashCode() {
            return Objects.hash(start, end);
        }

        @Override
        public String toString() {
            return "WindowDef{start=" + start + ", end=" + end + '}';
        }
    }

    private abstract class TimersExt implements Timers {
        K key;
        WindowDef windowDef;
        Value<A, S> windowData;

        void reset(K key, WindowDef windowDef, Value<A, S> windowData) {
            this.key = key;
            this.windowDef = windowDef;
            this.windowData = windowData;
        }

        abstract void removeEventTimeTimer(long time);
        abstract void removeSystemTimeTimer(long time);
    }

    private class ImmediateTimersImpl extends TimersExt {
        @Override
        public void scheduleEventTimeTimer(long time) {
            schedule(windowData.eventTimerTime, time, key, windowDef, eventTimers);
            windowData.eventTimerTime = time;
        }

        @Override
        public void scheduleSystemTimeTimer(long time) {
            schedule(windowData.systemTimerTime, time, key, windowDef, systemTimers);
            windowData.systemTimerTime = time;
        }

        @Override
        void removeEventTimeTimer(long time) {
            unscheduleTimer(eventTimers, key, windowDef, time);
        }

        @Override
        void removeSystemTimeTimer(long time) {
            unscheduleTimer(systemTimers, key, windowDef, time);
        }

        private void schedule(long oldTime, long newTime, K key, WindowDef windowDef,
                              SortedMap<Long, Set<Tuple2<K, WindowDef>>> timers) {
            if (newTime == Long.MIN_VALUE) {
                throw new IllegalArgumentException("Cannot schedule timer for MIN_VALUE");
            }
            unscheduleTimer(timers, key, windowDef, oldTime);
            timers.computeIfAbsent(newTime, x -> new HashSet<>())
                            .add(tuple2(key, windowDef));
        }
    }

    private class LazyTimersImpl extends TimersExt {
        private List<LazyTimer<K>> lazyEventTimers = new ArrayList<>();
        private List<LazyTimer<K>> lazySystemTimers = new ArrayList<>();

        @Override
        public void scheduleEventTimeTimer(long time) {
            lazyEventTimers.add(new LazyTimer(key, windowDef, windowData.eventTimerTime, time));
        }

        @Override
        public void scheduleSystemTimeTimer(long time) {
            lazySystemTimers.add(new LazyTimer(key, windowDef, windowData.systemTimerTime, time));
        }

        @Override
        void removeEventTimeTimer(long time) {
            unscheduleLazy(lazyEventTimers, time);
        }

        @Override
        void removeSystemTimeTimer(long time) {
            unscheduleLazy(lazySystemTimers, time);
        }

        private void applyLazyTimers() {
            for (LazyTimer<K> t : lazyEventTimers) {
                scheduleLazy(t.oldTime, t.newTime, t.key, t.windowDef, eventTimers);
            }
            for (LazyTimer<K> t : lazySystemTimers) {
                scheduleLazy(t.oldTime, t.newTime, t.key, t.windowDef, systemTimers);
            }
            lazyEventTimers.clear();
            lazySystemTimers.clear();
        }

        private void unscheduleLazy(List<LazyTimer<K>> timers, long time) {
            if (time != Long.MIN_VALUE) {
                timers.add(new LazyTimer<>(key, windowDef, time, Long.MIN_VALUE));
            }
        }

        private void scheduleLazy(long oldTime, long newTime, K key, WindowDef windowDef,
                                  SortedMap<Long, Set<Tuple2<K, WindowDef>>> timers) {
            if (newTime == Long.MIN_VALUE) {
                return;
            }
            unscheduleTimer(timers, key, windowDef, oldTime);
            timers.computeIfAbsent(newTime, x -> new HashSet<>())
                            .add(tuple2(key, windowDef));
        }
    }

    private static final class LazyTimer<K> {
        final K key;
        final WindowDef windowDef;
        final long oldTime;
        final long newTime;

        private LazyTimer(K key, WindowDef windowDef, long oldTime, long newTime) {
            this.key = key;
            this.windowDef = windowDef;
            this.oldTime = oldTime;
            this.newTime = newTime;
        }
    }

    // package-visible for test
    enum Keys {
        CURRENT_WATERMARK
    }

    private interface TriggerHandler<S> {
        TriggerAction onTimer(long time, WindowDef window, S state, Timers timers) throws Exception;
    }
}
