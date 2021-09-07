package com.github.mdc.common;

import java.io.Closeable;

public sealed interface HeartBeatCloseable extends Closeable permits HeartBeatServer,HeartBeatServerStream,HeartBeatTaskScheduler,HeartBeatTaskSchedulerStream {

}
