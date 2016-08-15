/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.container.task.nio;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.actor.RingBufferActor;
import com.hazelcast.jet.impl.container.ContainerTask;
import com.hazelcast.jet.impl.container.JobManager;
import com.hazelcast.jet.impl.container.ProcessingContainer;
import com.hazelcast.jet.impl.data.io.JetPacket;
import com.hazelcast.jet.impl.data.io.ObjectIOStream;
import com.hazelcast.jet.impl.data.io.SocketReader;
import com.hazelcast.jet.impl.data.io.SocketWriter;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.impl.util.BooleanHolder;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DefaultSocketReader
        extends AbstractNetworkTask implements SocketReader {
    protected volatile ByteBuffer receiveBuffer;
    protected boolean isBufferActive;

    private final int chunkSize;
    private final Address jetAddress;
    private final JobContext jobContext;
    private final ObjectIOStream<JetPacket> buffer;
    private final List<RingBufferActor> consumers = new ArrayList<RingBufferActor>();
    private final Map<Address, SocketWriter> writers = new HashMap<Address, SocketWriter>();

    private JetPacket packet;
    private volatile boolean socketAssigned;
    private final byte[] jobNameBytes;

    public DefaultSocketReader(JobContext jobContext,
                               Address jetAddress) {
        super(jobContext.getNodeEngine(), jetAddress);

        this.jetAddress = jetAddress;
        this.jobContext = jobContext;
        InternalSerializationService serializationService =
                (InternalSerializationService) jobContext.getNodeEngine().getSerializationService();
        this.jobNameBytes = serializationService.toBytes(this.jobContext.getName());
        this.chunkSize = jobContext.getJobConfig().getChunkSize();
        this.buffer = new ObjectIOStream<JetPacket>(new JetPacket[this.chunkSize]);
    }

    public DefaultSocketReader(NodeEngine nodeEngine) {
        super(nodeEngine, null);
        this.jetAddress = null;
        this.socketAssigned = true;
        this.jobContext = null;
        this.chunkSize = JobConfig.DEFAULT_CHUNK_SIZE;
        this.jobNameBytes = null;
        this.buffer = new ObjectIOStream<JetPacket>(new JetPacket[this.chunkSize]);
    }

    public void init() {
        super.init();
        socketAssigned = false;
        isBufferActive = false;
    }

    @Override
    public boolean onExecute(BooleanHolder payload) throws Exception {
        if (checkInterrupted()) {
            return false;
        }

        if (socketAssigned) {
            if (processRead(payload)) {
                return true;
            }
        } else {
            if (waitingForFinish) {
                finished = true;
            } else {
                return true;
            }
        }
        return checkFinished();
    }

    private boolean processRead(BooleanHolder payload) throws Exception {
        if (!isFlushed()) {
            payload.set(false);
            return true;
        }

        if (isBufferActive) {
            if (!readBuffer()) {
                return true;
            }
        }

        readSocket(payload);

        if (waitingForFinish) {
            if ((!payload.get()) && (isFlushed())) {
                finished = true;
            }
        }
        return false;
    }

    private boolean checkInterrupted() {
        if (destroyed) {
            closeSocket();
            return true;
        }

        if (interrupted) {
            closeSocket();
            finalized = true;
            notifyAMTaskFinished();
            return true;
        }
        return false;
    }

    @Override
    protected void notifyAMTaskFinished() {
        jobContext.getJobManager().notifyNetworkTaskFinished();
    }

    private boolean readSocket(BooleanHolder payload) {
        if ((socketChannel != null) && (socketChannel.isConnected())) {
            try {
                if (socketChannel != null) {
                    int readBytes = socketChannel.read(receiveBuffer);

                    if (readBytes <= 0) {
                        return handleEmptyChannel(payload, readBytes);
                    } else {
                        totalBytes += readBytes;
                    }
                }

                receiveBuffer.flip();
                readBuffer();
                payload.set(true);
            } catch (IOException e) {
                closeSocket();
            } catch (Exception e) {
                throw JetUtil.reThrow(e);
            }

            return true;
        } else {
            payload.set(false);
            return true;
        }
    }

    private boolean handleEmptyChannel(BooleanHolder payload, int readBytes) {
        if (readBytes < 0) {
            return false;
        }

        payload.set(false);
        return readBytes == 0;
    }

    protected boolean readBuffer() throws Exception {
        while (receiveBuffer.hasRemaining()) {
            if (packet == null) {
                packet = new JetPacket();
            }

            if (!packet.readFrom(receiveBuffer)) {
                alignBuffer(receiveBuffer);
                isBufferActive = false;
                return true;
            }

            // False means this is threadAcceptor
            if (!consumePacket(packet)) {
                packet = null;
                return false;
            }

            packet = null;

            if (buffer.size() >= chunkSize) {
                flush();

                if (!isFlushed()) {
                    isBufferActive = true;
                    return false;
                }
            }
        }

        isBufferActive = false;
        alignBuffer(receiveBuffer);
        flush();
        return isFlushed();
    }

    protected boolean alignBuffer(ByteBuffer buffer) {
        if (buffer.hasRemaining()) {
            buffer.compact();
            return true;
        } else {
            buffer.clear();
            return false;
        }
    }

    protected boolean consumePacket(JetPacket packet) throws Exception {
        buffer.consume(packet);
        return true;
    }

    private void flush() throws Exception {
        if (buffer.size() > 0) {
            for (JetPacket packet : buffer) {
                int header = resolvePacket(packet);

                if (header > 0) {
                    sendResponse(packet, header);
                }
            }

            buffer.reset();
        }
    }

    private void sendResponse(JetPacket jetPacket, int header) throws Exception {
        jetPacket.reset();
        jetPacket.setHeader(header);
        writers.get(jetAddress).sendServicePacket(jetPacket);
    }

    @Override
    public boolean isFlushed() {
        boolean isFlushed = true;

        for (int i = 0; i < consumers.size(); i++) {
            isFlushed &= consumers.get(i).isFlushed();
        }

        return isFlushed;
    }

    @Override
    public void setSocketChannel(SocketChannel socketChannel,
                                 ByteBuffer receiveBuffer,
                                 boolean isBufferActive) {
        this.receiveBuffer = receiveBuffer;
        this.socketChannel = socketChannel;
        this.isBufferActive = isBufferActive;

        try {
            socketChannel.configureBlocking(false);
        } catch (IOException e) {
            throw JetUtil.reThrow(e);
        }

        socketAssigned = true;
    }

    @Override
    public void registerConsumer(RingBufferActor ringBufferActor) {
        consumers.add(ringBufferActor);
    }

    @Override
    public void assignWriter(Address writeAddress,
                             SocketWriter socketWriter) {
        writers.put(writeAddress, socketWriter);
    }

    public int resolvePacket(JetPacket packet) throws Exception {
        int header = packet.getHeader();

        switch (header) {
            /*Request - bytes for pair chunk*/
            /*Request shuffling channel closed*/
            case JetPacket.HEADER_JET_DATA_CHUNK:
            case JetPacket.HEADER_JET_SHUFFLER_CLOSED:
            case JetPacket.HEADER_JET_DATA_CHUNK_SENT:
                return notifyShufflingReceiver(packet);

            case JetPacket.HEADER_JET_EXECUTION_ERROR:
                packet.setRemoteMember(jetAddress);
                jobContext.getJobManager().notifyContainers(packet);
                return 0;

            case JetPacket.HEADER_JET_DATA_NO_APP_FAILURE:
            case JetPacket.HEADER_JET_DATA_NO_TASK_FAILURE:
            case JetPacket.HEADER_JET_DATA_NO_MEMBER_FAILURE:
            case JetPacket.HEADER_JET_CHUNK_WRONG_CHUNK_FAILURE:
            case JetPacket.HEADER_JET_DATA_NO_CONTAINER_FAILURE:
            case JetPacket.HEADER_JET_APPLICATION_IS_NOT_EXECUTING:
                invalidateAll();
                return 0;

            default:
                return 0;
        }
    }

    private void invalidateAll() throws Exception {
        for (SocketWriter sender : writers.values()) {
            JetPacket jetPacket = new JetPacket(
                    jobNameBytes
            );

            jetPacket.setHeader(JetPacket.HEADER_JET_EXECUTION_ERROR);
            sender.sendServicePacket(jetPacket);
        }
    }

    private int notifyShufflingReceiver(JetPacket packet) throws Exception {
        JobManager jobManager = jobContext.getJobManager();
        ProcessingContainer processingContainer = jobManager.getContainersCache().get(packet.getContainerId());

        if (processingContainer == null) {
            logger.warning("No such container with containerId="
                    + packet.getContainerId()
                    + " jetPacket="
                    + packet
                    + ". Job will be interrupted."
            );

            return JetPacket.HEADER_JET_DATA_NO_CONTAINER_FAILURE;
        }

        ContainerTask containerTask = processingContainer.getTasksCache().get(packet.getTaskID());

        if (containerTask == null) {
            logger.warning("No such task in container with containerId="
                    + packet.getContainerId()
                    + " taskId="
                    + packet.getTaskID()
                    + " jetPacket="
                    + packet
                    + ". Job will be interrupted."
            );

            return JetPacket.HEADER_JET_DATA_NO_TASK_FAILURE;
        }

        containerTask.getShufflingReceiver(jetAddress).consume(packet);
        return 0;
    }
}