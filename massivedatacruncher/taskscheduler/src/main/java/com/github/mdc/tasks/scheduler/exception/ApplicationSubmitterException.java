package com.github.mdc.tasks.scheduler.exception;

public class ApplicationSubmitterException extends Exception{
    public ApplicationSubmitterException(Exception ex, String message){
        super(message,ex);
    }
}
