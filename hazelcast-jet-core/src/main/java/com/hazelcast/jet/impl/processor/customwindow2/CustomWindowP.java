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
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.AppendableTraverser;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.function.KeyedWindowResultFunction;
import com.hazelcast.jet.impl.processor.customwindow2.Trigger.Timers;
import com.hazelcast.jet.impl.processor.customwindow2.Trigger.TriggerAction;
import com.hazelcast.jet.impl.processor.customwindow2.WindowSet.Value;
import com.hazelcast.util.Clock;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.Util.lazyIncrement;
import static com.hazelcast.jet.impl.util.Util.logLateEvent;
import static com.hazelcast.util.Preconditions.checkTrue;
import static java.util.Collections.emptySet;

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

    private final Map<K, WindowSet<IN, A, S>> windowSets = new HashMap<>();
    private final SortedMap<Long, Set<Tuple2<K, WindowDef>>> eventTimers = new TreeMap<>();
    private final SortedMap<Long, Set<Tuple2<K, WindowDef>>> systemTimers = new TreeMap<>();
    private long currentWatermark = Long.MIN_VALUE;
    private ProcessingGuarantee processingGuarantee;
    private long minRestoredCurrentWatermark = Long.MAX_VALUE;
    private TimersImpl timersImpl = new TimersImpl();
    private final AppendableTraverser<OUT> appendableTraverser = new AppendableTraverser<>(16);
    private Traverser<OUT> onTimerTraverser = Traversers.empty();

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
            @Nonnull KeyedWindowResultFunction<? super K, ? super R, OUT> mapToOutputFn
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
            timersImpl.reset(key, windowDef, windowData);
            TriggerAction triggerAction = trigger.onItem(castedItem, timestamp, windowDef, windowData.triggerState,
                    timersImpl);
            OUT out = handleTriggerAction(windowSet, triggerAction);
            if (out != null) {
                appendableTraverser.append(out);
            }
        }

        return emitFromTraverser(appendableTraverser);
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark wm) {
        boolean res = handleTimers(eventTimers, wm.timestamp());
        if (res) {
            // TODO purge windows even if triggers didn't say so
        }
        return res;
    }

    @Override
    public boolean tryProcess() {
        return handleTimers(systemTimers, Clock.currentTimeMillis());
    }

    private boolean handleTimers(SortedMap<Long, Set<Tuple2<K, WindowDef>>> timers, long time) {
        if (!emitFromTraverser(onTimerTraverser)) {
            return false;
        }
        SortedMap<Long, Set<Tuple2<K, WindowDef>>> timersToExecute = timers.headMap(time);
        onTimerTraverser = traverseStream(timersToExecute.entrySet().stream()
                .flatMap(entry -> entry.getValue().stream()
                             .map(keyAndWindow -> {
                                 try {
                                     WindowSet<IN, A, S> windowSet = windowSets.get(keyAndWindow.f0());
                                     Value<A, S> windowData = windowSet.getWindowData(keyAndWindow.f1());
                                     timersImpl.reset(keyAndWindow.f0(), keyAndWindow.f1(), windowData);
                                     TriggerAction action = trigger.onEventTime(entry.getKey(), keyAndWindow.f1(),
                                             windowData.triggerState, timersImpl);
                                     return handleTriggerAction(windowSet, action);
                                 } catch (Exception e) {
                                     throw sneakyThrow(e);
                                 }
                             })))
        .onFirstNull(timersToExecute::clear);

        return emitFromTraverser(onTimerTraverser);
    }

    private OUT handleTriggerAction(WindowSet<IN, A, S> windowSet, TriggerAction triggerAction) {
        OUT out = null;
        if (triggerAction.fire) {
            R winResult = triggerAction.purge
                    ? aggrOp.finishFn().apply(timersImpl.windowData.accumulator)
                    : aggrOp.exportFn().apply(timersImpl.windowData.accumulator);
            out = mapToOutputFn.apply(timersImpl.windowDef.start, timersImpl.windowDef.end, timersImpl.key, winResult);
        }
        if (triggerAction.purge) {
            timersImpl.unschedule(eventTimers, timersImpl.windowData.eventTimerTime);
            timersImpl.unschedule(systemTimers, timersImpl.windowData.systemTimerTime);
            windowSet.remove(timersImpl.windowDef);
            if (windowSet.isEmpty()) {
                windowSets.remove(timersImpl.key);
            }
        }
        return out;
    }

    public static class WindowDef {
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

    private class TimersImpl implements Timers {

        private K key;
        private WindowDef windowDef;
        private Value<A, S> windowData;

        private void reset(K key, WindowDef windowDef, Value<A, S> windowData) {
            this.key = key;
            this.windowDef = windowDef;
            this.windowData = windowData;
        }

        @Override
        public void scheduleEventTimeTimer(long time) {
            schedule(windowData.eventTimerTime, time, eventTimers);
            windowData.eventTimerTime = time;
        }

        @Override
        public void scheduleSystemTimeTimer(long time) {
            schedule(windowData.systemTimerTime, time, systemTimers);
            windowData.systemTimerTime = time;
        }

        private void schedule(long oldTime, long newTime, SortedMap<Long, Set<Tuple2<K, WindowDef>>> timers) {
            if (newTime == Long.MIN_VALUE) {
                throw new IllegalArgumentException("Cannot schedule for MIN_VALUE");
            }
            unschedule(timers, oldTime);
            timers.computeIfAbsent(newTime, x -> new HashSet<>())
                            .add(tuple2(key, windowDef));
        }

        private void unschedule(SortedMap<Long, Set<Tuple2<K, WindowDef>>> timers, long time) {
            if (time != Long.MIN_VALUE) {
                timers.getOrDefault(time, emptySet())
                   .remove(tuple2(key, windowDef));
            }
        }
    }
}
