package com.byd.utils;

import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class CheckpointUtils {
    public static void setCheckpoint(StreamExecutionEnvironment env, String checkpointStorage) {


        // RocksDBStateBackend
//        EmbeddedRocksDBStateBackend embeddedRocksDBStateBackend = new EmbeddedRocksDBStateBackend(true);
//        embeddedRocksDBStateBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM);

        env.setStateBackend(new HashMapStateBackend());

        // checkpoint配置
        env.enableCheckpointing(TimeUnit.SECONDS.toMillis(10), CheckpointingMode.EXACTLY_ONCE);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();

        checkpointConfig.setCheckpointStorage(checkpointStorage);

        checkpointConfig.setMinPauseBetweenCheckpoints(TimeUnit.SECONDS.toMillis(10));

        checkpointConfig.setTolerableCheckpointFailureNumber(3);

        checkpointConfig.setCheckpointTimeout(TimeUnit.MINUTES.toMillis(2));

        checkpointConfig.setMaxConcurrentCheckpoints(1);

        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

    }
}
