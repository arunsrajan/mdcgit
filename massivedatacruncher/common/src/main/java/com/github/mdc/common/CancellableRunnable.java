package com.github.mdc.common;
/**
 * 
 * @author Arun
 * Runnable with cancel methods
 */
public interface CancellableRunnable extends Runnable {
    public void cancel();
}
