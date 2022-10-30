package com.github.mdc.common;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface StreamDataCruncher extends Remote{
	public Object postObject(Object object)throws RemoteException;
}
