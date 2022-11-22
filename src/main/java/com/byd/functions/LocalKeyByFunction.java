package com.byd.functions;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class LocalKeyByFunction extends RichFlatMapFunction<String, Tuple2<String, Long>> implements CheckpointedFunction {
    // checkpoint 时 为了保证Exactly once，将buffer的数据保存到该ListState中
    private ListState<Tuple2<String, Long>> localStateListState;

    // 本地buff，存储local端缓存的数据信息
    private HashMap<String, Long> localMap;

    // 缓存的数据量大小，即缓存多少再向下游发送
    private int bitchSize;

    //计数器
    private AtomicInteger currentSize;

    public LocalKeyByFunction(int bitchSize) {
        this.bitchSize = bitchSize;
    }

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
        Long cnt = localMap.getOrDefault(value, 0L);
        localMap.put(value, cnt + 1);
        // 当前数据量达到一定批次
        if (currentSize.incrementAndGet() >= bitchSize) {
            for (Map.Entry<String, Long> entry : localMap.entrySet()) {
                String key = entry.getKey();
                Long cntValue = entry.getValue();
                out.collect(Tuple2.of(key, cntValue));
            }
            localMap.clear();
            currentSize.set(0);
        }
    }


    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        localStateListState.clear();
        for (Map.Entry<String, Long> entry : localMap.entrySet()) {
            String key = entry.getKey();
            Long cntValue = entry.getValue();
            localStateListState.add(Tuple2.of(key, cntValue));
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        localStateListState = context.getOperatorStateStore()
                .getListState(
                        new ListStateDescriptor<Tuple2<String, Long>>("state",
                                Types.TUPLE(TypeInformation.of(String.class), TypeInformation.of(Long.class))
                        )
                );
        localMap = new HashMap<>();
        // 如果是恢复数据
        if (context.isRestored()) {
            for (Tuple2<String, Long> kv : localStateListState.get()) {
                localMap.put(kv.f0, kv.f1);
            }
            // 从状态恢复时，默认认为buff中的数据量达到了batchSize，需要向下游发送
            currentSize = new AtomicInteger(bitchSize);
        } else {
            currentSize = new AtomicInteger(0);
        }
    }
}
