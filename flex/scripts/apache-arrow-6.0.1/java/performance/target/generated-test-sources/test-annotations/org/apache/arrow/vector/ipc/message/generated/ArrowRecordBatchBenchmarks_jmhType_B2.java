package org.apache.arrow.vector.ipc.message.generated;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
public class ArrowRecordBatchBenchmarks_jmhType_B2 extends ArrowRecordBatchBenchmarks_jmhType_B1 {
    public volatile int setupTrialMutex;
    public volatile int tearTrialMutex;
    public final static AtomicIntegerFieldUpdater<ArrowRecordBatchBenchmarks_jmhType_B2> setupTrialMutexUpdater = AtomicIntegerFieldUpdater.newUpdater(ArrowRecordBatchBenchmarks_jmhType_B2.class, "setupTrialMutex");
    public final static AtomicIntegerFieldUpdater<ArrowRecordBatchBenchmarks_jmhType_B2> tearTrialMutexUpdater = AtomicIntegerFieldUpdater.newUpdater(ArrowRecordBatchBenchmarks_jmhType_B2.class, "tearTrialMutex");

    public volatile int setupIterationMutex;
    public volatile int tearIterationMutex;
    public final static AtomicIntegerFieldUpdater<ArrowRecordBatchBenchmarks_jmhType_B2> setupIterationMutexUpdater = AtomicIntegerFieldUpdater.newUpdater(ArrowRecordBatchBenchmarks_jmhType_B2.class, "setupIterationMutex");
    public final static AtomicIntegerFieldUpdater<ArrowRecordBatchBenchmarks_jmhType_B2> tearIterationMutexUpdater = AtomicIntegerFieldUpdater.newUpdater(ArrowRecordBatchBenchmarks_jmhType_B2.class, "tearIterationMutex");

    public volatile int setupInvocationMutex;
    public volatile int tearInvocationMutex;
    public final static AtomicIntegerFieldUpdater<ArrowRecordBatchBenchmarks_jmhType_B2> setupInvocationMutexUpdater = AtomicIntegerFieldUpdater.newUpdater(ArrowRecordBatchBenchmarks_jmhType_B2.class, "setupInvocationMutex");
    public final static AtomicIntegerFieldUpdater<ArrowRecordBatchBenchmarks_jmhType_B2> tearInvocationMutexUpdater = AtomicIntegerFieldUpdater.newUpdater(ArrowRecordBatchBenchmarks_jmhType_B2.class, "tearInvocationMutex");

    public volatile boolean readyTrial;
    public volatile boolean readyIteration;
    public volatile boolean readyInvocation;
}
