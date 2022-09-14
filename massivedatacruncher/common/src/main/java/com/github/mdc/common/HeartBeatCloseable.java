package com.github.mdc.common;

import java.io.Closeable;
/**
 * The closeable interface for HeartBeat 
 * @author arun
 *
 */
public sealed interface HeartBeatCloseable extends Closeable,HeartBeatMBean permits HeartBeat,HeartBeatStream,HeartBeatTaskScheduler,HeartBeatTaskSchedulerStream {

}
