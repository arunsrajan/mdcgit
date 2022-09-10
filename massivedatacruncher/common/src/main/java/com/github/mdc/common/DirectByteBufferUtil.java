package com.github.mdc.common;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;

import sun.misc.Unsafe;

public final class DirectByteBufferUtil {

    //various buffer creation utility functions etc. etc.

    protected static final sun.misc.Unsafe unsafe = AccessController.doPrivileged(new PrivilegedAction<sun.misc.Unsafe>() {
        @Override
        public Unsafe run() {
            try {
                Field f = Unsafe.class.getDeclaredField("theUnsafe");
                f.setAccessible(true);
                return (Unsafe) f.get(null);
            } catch(NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException ex) {
                throw new RuntimeException(ex);
            }
        }
    });

    /** Frees the specified buffer's direct memory allocation.<br>
     * The buffer should not be used after calling this method; you should
     * instead allow it to be garbage-collected by removing all references of it
     * from your program.
     * 
     * @param directBuffer The direct buffer whose memory allocation will be
     *            freed
     * @return Whether or not the memory allocation was freed */
    public static final boolean freeDirectBufferMemory(ByteBuffer directBuffer) {
        if(!directBuffer.isDirect()) {
            return false;
        }
        try {
            unsafe.invokeCleaner(directBuffer);
            return true;
        } catch(IllegalArgumentException ex) {
            ex.printStackTrace();
            return false;
        }
    }

}