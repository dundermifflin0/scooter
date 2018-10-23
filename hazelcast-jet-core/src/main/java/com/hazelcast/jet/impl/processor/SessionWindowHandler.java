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
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.accumulator.MutableReference;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.function.KeyedWindowResultFunction;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.jet.impl.processor.SessionWindowHandler.Windows;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.QuickMath;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.impl.util.Util.lazyAdd;
import static com.hazelcast.jet.impl.util.Util.lazyIncrement;
import static com.hazelcast.jet.impl.util.Util.toLocalDateTime;
import static java.lang.Math.min;
import static java.lang.System.arraycopy;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class SessionWindowHandler<T, K, A, R, OUT> implements CustomWindowHandler<T, K, Windows<A>, OUT> {

    private final SortedMap<Long, Set<K>> deadlineToKeys = new TreeMap<>();
    private final long sessionTimeout;
    @Nonnull
    private final AggregateOperation<A, R> aggrOp;
    @Nonnull
    private final BiConsumer<? super A, ? super A> combineFn;
    @Nonnull
    private final KeyedWindowResultFunction<? super K, ? super R, OUT> mapToOutputFn;

    @Probe
    private AtomicLong totalKeys = new AtomicLong();
    @Probe
    private AtomicLong totalWindows = new AtomicLong();

    public SessionWindowHandler(
            long sessionTimeout,
            @Nonnull AggregateOperation<A, R> aggrOp,
            @Nonnull KeyedWindowResultFunction<? super K, ? super R, OUT> mapToOutputFn
    ) {
        this.aggrOp = aggrOp;
        this.combineFn = requireNonNull(aggrOp.combineFn());
        this.mapToOutputFn = mapToOutputFn;
        this.sessionTimeout = sessionTimeout;
    }

    @Override
    public void init(Map<K, Windows<A>> states) {
        assert deadlineToKeys.isEmpty();
        // populate deadlineToKeys
        for (Entry<K, Windows<A>> entry : states.entrySet()) {
            for (long end : entry.getValue().ends) {
                addToDeadlines(entry.getKey(), end);
            }
        }
        totalKeys.set(states.size());
    }

    @Nonnull @Override
    public Traverser<OUT> onItem(int ordinal, @Nonnull T item, K key, long timestamp,
                               @Nonnull MutableReference<Windows<A>> state) {
        addItem(ordinal,
                state.createIfAbsent(Windows::new),
                key, timestamp, item);
        return Traversers.empty();
    }

    private void addItem(int ordinal, Windows<A> w, K key, long timestamp, Object item) {
        aggrOp.accumulateFn(ordinal).accept(resolveAcc(w, key, timestamp), item);
    }

    private void addToDeadlines(K key, long deadline) {
        if (deadlineToKeys.computeIfAbsent(deadline, x -> new HashSet<>()).add(key)) {
            lazyIncrement(totalWindows);
        }
    }

    private void removeFromDeadlines(K key, long deadline) {
        Set<K> ks = deadlineToKeys.get(deadline);
        ks.remove(key);
        lazyAdd(totalWindows, -1);
        if (ks.isEmpty()) {
            deadlineToKeys.remove(deadline);
        }
    }

    private List<OUT> closeWindows(Map<K, Windows<A>> states, K key, long wm) {
        Windows<A> w = states.get(key);
        List<OUT> results = new ArrayList<>();
        int i = 0;
        for (; i < w.size && w.ends[i] < wm; i++) {
            OUT out = mapToOutputFn.apply(w.starts[i], w.ends[i], key, aggrOp.finishFn().apply(w.accs[i]));
            if (out != null) {
                results.add(out);
            }
        }
        if (i != w.size) {
            w.removeHead(i);
        } else {
            states.remove(key);
            totalKeys.set(states.size());
        }
        return results;
    }

    private A resolveAcc(Windows<A> w, K key, long timestamp) {
        long eventEnd = timestamp + sessionTimeout;
        int i = 0;
        for (; i < w.size && w.starts[i] <= eventEnd; i++) {
            // the window `i` is not after the event interval

            if (w.ends[i] < timestamp) {
                // the window `i` is before the event interval
                continue;
            }
            if (w.starts[i] <= timestamp && w.ends[i] >= eventEnd) {
                // the window `i` fully covers the event interval
                return w.accs[i];
            }
            // the window `i` overlaps the event interval

            if (i + 1 == w.size || w.starts[i + 1] > eventEnd) {
                // the window `i + 1` doesn't overlap the event interval
                w.starts[i] = min(w.starts[i], timestamp);
                if (w.ends[i] < eventEnd) {
                    removeFromDeadlines(key, w.ends[i]);
                    w.ends[i] = eventEnd;
                    addToDeadlines(key, w.ends[i]);
                }
                return w.accs[i];
            }
            // both `i` and `i + 1` windows overlap the event interval
            removeFromDeadlines(key, w.ends[i]);
            w.ends[i] = w.ends[i + 1];
            combineFn.accept(w.accs[i], w.accs[i + 1]);
            w.removeWindow(i + 1);
            return w.accs[i];
        }
        addToDeadlines(key, eventEnd);
        return insertWindow(w, i, timestamp, eventEnd);
    }

    private A insertWindow(Windows<A> w, int idx, long windowStart, long windowEnd) {
        w.expandIfNeeded();
        w.copy(idx, idx + 1, w.size - idx);
        w.size++;
        w.starts[idx] = windowStart;
        w.ends[idx] = windowEnd;
        w.accs[idx] = aggrOp.createFn().get();
        return w.accs[idx];
    }

    @Nonnull @Override
    public Traverser<OUT> onWatermark(@Nonnull Watermark wm, Map<K, Windows<A>> states) {
        assert totalWindows.get() == deadlineToKeys.values().stream().mapToInt(Set::size).sum()
                : "unexpected totalWindows. Expected=" + deadlineToKeys.values().stream().mapToInt(Set::size).sum()
                + ", actual=" + totalWindows.get();
        SortedMap<Long, Set<K>> windowsToClose = deadlineToKeys.headMap(wm.timestamp());
        lazyAdd(totalWindows, -windowsToClose.values().stream().mapToInt(Set::size).sum());

        List<K> distinctKeys = windowsToClose
                .values().stream()
                .flatMap(Set::stream)
                .distinct()
                .collect(toList());
        windowsToClose.clear();

        Stream<OUT> closedWindows = distinctKeys
                .stream()
                .map(key -> closeWindows(states, key, wm.timestamp()))
                .flatMap(List::stream);
        return traverseStream(closedWindows);
    }

    @Nonnull @Override
    public Traverser<OUT> onTryProcess(Map<K, Windows<A>> states) {
        return Traversers.empty();
    }

    public static class Windows<A> implements IdentifiedDataSerializable {
        private int size;
        private long[] starts = new long[2];
        private long[] ends = new long[2];
        private A[] accs = (A[]) new Object[2];

        private void removeWindow(int idx) {
            size--;
            copy(idx + 1, idx, size - idx);
        }

        private void removeHead(int count) {
            copy(count, 0, size - count);
            size -= count;
        }

        private void copy(int from, int to, int length) {
            arraycopy(starts, from, starts, to, length);
            arraycopy(ends, from, ends, to, length);
            arraycopy(accs, from, accs, to, length);
        }

        private void expandIfNeeded() {
            if (size == starts.length) {
                starts = Arrays.copyOf(starts, 2 * starts.length);
                ends = Arrays.copyOf(ends, 2 * ends.length);
                accs = Arrays.copyOf(accs, 2 * accs.length);
            }
        }

        @Override
        public int getFactoryId() {
            return JetInitDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getId() {
            return JetInitDataSerializerHook.SESSION_WINDOW_H_WINDOWS;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(size);
            for (int i = 0; i < size; i++) {
                out.writeLong(starts[i]);
                out.writeLong(ends[i]);
                out.writeObject(accs[i]);
            }
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            size = in.readInt();
            if (size > starts.length) {
                int newSize = QuickMath.nextPowerOfTwo(size);
                starts = new long[newSize];
                ends = new long[newSize];
                accs = (A[]) new Object[newSize];
            }

            for (int i = 0; i < size; i++) {
                starts[i] = in.readLong();
                ends[i] = in.readLong();
                accs[i] = in.readObject();
            }
        }

        @Override
        public String toString() {
            StringJoiner sj = new StringJoiner(", ", getClass().getSimpleName() + '{', "}");
            for (int i = 0; i < size; i++) {
                sj.add("[s=" + toLocalDateTime(starts[i]).toLocalTime()
                        + ", e=" + toLocalDateTime(ends[i]).toLocalTime()
                        + ", a=" + accs[i] + ']');
            }
            return sj.toString();
        }
    }
}
