/**
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package com.yahoo.omid.tso;

import com.yahoo.omid.tso.messages.Sequencable;

import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelState;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.util.ObjectSizeEstimator;
import org.jboss.netty.util.internal.ConcurrentIdentityWeakKeyHashMap;

import org.jboss.netty.handler.execution.MemoryAwareThreadPoolExecutor;
import org.jboss.netty.handler.execution.ChannelEventRunnable;

/**
 * Part of the source code is borrowed from netty
 */
public class DelayedOrderedExecutor extends MemoryAwareThreadPoolExecutor {

    private final ConcurrentMap<Object, Executor> childExecutors = newChildExecutorMap();

    /**
     * Creates a new instance.
     *
     * @param corePoolSize          the maximum number of active threads
     * @param maxChannelMemorySize  the maximum total size of the queued events per channel.
     *                              Specify {@code 0} to disable.
     * @param maxTotalMemorySize    the maximum total size of the queued events for this pool
     *                              Specify {@code 0} to disable.
     */
    public DelayedOrderedExecutor(
            int corePoolSize, long maxChannelMemorySize, long maxTotalMemorySize) {
        super(corePoolSize, maxChannelMemorySize, maxTotalMemorySize);
            }

    /**
     * Creates a new instance.
     *
     * @param corePoolSize          the maximum number of active threads
     * @param maxChannelMemorySize  the maximum total size of the queued events per channel.
     *                              Specify {@code 0} to disable.
     * @param maxTotalMemorySize    the maximum total size of the queued events for this pool
     *                              Specify {@code 0} to disable.
     * @param keepAliveTime         the amount of time for an inactive thread to shut itself down
     * @param unit                  the {@link TimeUnit} of {@code keepAliveTime}
     */
    public DelayedOrderedExecutor(
            int corePoolSize, long maxChannelMemorySize, long maxTotalMemorySize,
            long keepAliveTime, TimeUnit unit) {
        super(corePoolSize, maxChannelMemorySize, maxTotalMemorySize,
                keepAliveTime, unit);
            }

    /**
     * Creates a new instance.
     *
     * @param corePoolSize          the maximum number of active threads
     * @param maxChannelMemorySize  the maximum total size of the queued events per channel.
     *                              Specify {@code 0} to disable.
     * @param maxTotalMemorySize    the maximum total size of the queued events for this pool
     *                              Specify {@code 0} to disable.
     * @param keepAliveTime         the amount of time for an inactive thread to shut itself down
     * @param unit                  the {@link TimeUnit} of {@code keepAliveTime}
     * @param threadFactory         the {@link ThreadFactory} of this pool
     */
    public DelayedOrderedExecutor(
            int corePoolSize, long maxChannelMemorySize, long maxTotalMemorySize,
            long keepAliveTime, TimeUnit unit, ThreadFactory threadFactory) {
        super(corePoolSize, maxChannelMemorySize, maxTotalMemorySize,
                keepAliveTime, unit, threadFactory);
            }

    /**
     * Creates a new instance.
     *
     * @param corePoolSize          the maximum number of active threads
     * @param maxChannelMemorySize  the maximum total size of the queued events per channel.
     *                              Specify {@code 0} to disable.
     * @param maxTotalMemorySize    the maximum total size of the queued events for this pool
     *                              Specify {@code 0} to disable.
     * @param keepAliveTime         the amount of time for an inactive thread to shut itself down
     * @param unit                  the {@link TimeUnit} of {@code keepAliveTime}
     * @param threadFactory         the {@link ThreadFactory} of this pool
     * @param objectSizeEstimator   the {@link ObjectSizeEstimator} of this pool
     */
    public DelayedOrderedExecutor(
            int corePoolSize, long maxChannelMemorySize, long maxTotalMemorySize,
            long keepAliveTime, TimeUnit unit,
            ObjectSizeEstimator objectSizeEstimator, ThreadFactory threadFactory) {
        super(corePoolSize, maxChannelMemorySize, maxTotalMemorySize,
                keepAliveTime, unit, objectSizeEstimator, threadFactory);
            }

    protected ConcurrentMap<Object, Executor> newChildExecutorMap() {
        return new ConcurrentIdentityWeakKeyHashMap<Object, Executor>();
    }

    static String SEQUENCED_KEY = "sequenced_key";
    protected Object getChildExecutorKey(ChannelEvent e) {
        Object key = null;
        if (e instanceof MessageEvent) {
            Object msg = ((MessageEvent)e).getMessage();
            if (msg instanceof Sequencable)
                if (((Sequencable)msg).isSequenced())
                    key = SEQUENCED_KEY;
        }
        if (key == null)
            key = e.getChannel();
        return key;
    }

    protected Set<Object> getChildExecutorKeySet() {
        return childExecutors.keySet();
    }

    protected boolean removeChildExecutor(Object key) {
        // FIXME: Succeed only when there is no task in the ChildExecutor's queue.
        //        Note that it will need locking which might slow down task submission.
        return childExecutors.remove(key) != null;
    }

    /**
     * Executes the specified task concurrently while maintaining the event
     * order.
     */
    @Override
    protected void doExecute(Runnable task) {
        if (!(task instanceof ChannelEventRunnable)) {
            doUnorderedExecute(task);
        } else {
            ChannelEventRunnable r = (ChannelEventRunnable) task;
            getChildExecutor(r.getEvent()).execute(task);
        }
    }

    private Executor getChildExecutor(ChannelEvent e) {
        Object key = getChildExecutorKey(e);
        Executor executor = childExecutors.get(key);
        if (executor == null) {
            if (key.equals(SEQUENCED_KEY))
                executor = new SequencedChildExecutor();
            else
                executor = new ChildExecutor();
            Executor oldExecutor = childExecutors.putIfAbsent(key, executor);
            if (oldExecutor != null) {
                executor = oldExecutor;
            }
        }

        // Remove the entry when the channel closes.
        if (e instanceof ChannelStateEvent) {
            Channel channel = e.getChannel();
            ChannelStateEvent se = (ChannelStateEvent) e;
            if (se.getState() == ChannelState.OPEN &&
                    !channel.isOpen()) {
                childExecutors.remove(channel);
                    }
        }
        return executor;
    }

    @Override
    protected boolean shouldCount(Runnable task) {
        if (task instanceof ChildExecutor) {
            return false;
        }

        return super.shouldCount(task);
    }

    void onAfterExecute(Runnable r, Throwable t) {
        afterExecute(r, t);
    }

    private class ChildExecutor implements Executor, Runnable {
        private final LinkedList<Runnable> tasks = new LinkedList<Runnable>();

        ChildExecutor() {
            super();
        }

        public void execute(Runnable command) {
            boolean needsExecution;
            synchronized (tasks) {
                needsExecution = tasks.isEmpty();
                tasks.add(command);
            }

            if (needsExecution) {
                doUnorderedExecute(this);
            }
        }

        public void run() {
            Thread thread = Thread.currentThread();
            for (;;) {
                final Runnable task;
                synchronized (tasks) {
                    task = tasks.getFirst();
                }

                boolean ran = false;
                beforeExecute(thread, task);
                try {
                    task.run();
                    ran = true;
                    onAfterExecute(task, null);
                } catch (RuntimeException e) {
                    if (!ran) {
                        onAfterExecute(task, e);
                    }
                    throw e;
                } finally {
                    synchronized (tasks) {
                        tasks.removeFirst();
                        if (tasks.isEmpty()) {
                            break;
                        }
                    }
                }
            }
        }
    }

    private final class SequencedChildExecutor extends ChildExecutor {
        private final PriorityQueue<Runnable> tasks = 
            new PriorityQueue<Runnable>(100, new SequencedTaskComparator());
        boolean running = false;

        class SequencedTaskComparator implements java.util.Comparator<Runnable> {
            @Override
            public int compare(Runnable o1, Runnable o2) {
                Long l = getSequence(o1) - getSequence(o2);
                return l.intValue();
            }

            long getSequence(Runnable task) {
                ChannelEventRunnable e = (ChannelEventRunnable) task;
                ChannelEvent event = e.getEvent();
                Object msg = ((MessageEvent)event).getMessage();
                long seq =  (((Sequencable)msg).getSequence());
                return seq;
            }
        }

        SequencedChildExecutor() {
        }

        @Override
        public void execute(Runnable command) {
            boolean needsExecution;
            synchronized (tasks) {
                needsExecution = tasks.isEmpty() && !running;
                boolean res = tasks.add(command);
                assert(res == true);
            }

            if (needsExecution) {
                doUnorderedExecute(this);
            }
        }

        @Override
        public void run() {
            Thread thread = Thread.currentThread();
            for (;;) {
                final Runnable task;
                synchronized (tasks) {
                    //task = tasks.getFirst();
                    task = tasks.poll();
                    running = true;
                    assert(task != null);
                }

                boolean ran = false;
                beforeExecute(thread, task);
                try {
                    task.run();
                    ran = true;
                    onAfterExecute(task, null);
                } catch (RuntimeException e) {
                    if (!ran) {
                        onAfterExecute(task, e);
                    }
                    throw e;
                } finally {
                    synchronized (tasks) {
                        //tasks.removeFirst();
                        if (tasks.isEmpty()) {
                            running = false;
                            break;
                        }
                    }
                }
            }
        }
    }
}


