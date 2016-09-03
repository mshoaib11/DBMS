package dbs_project.util;

import java.net.InetAddress;
import java.security.Permission;

import sun.misc.Unsafe;

public class DBMSecurityManager extends SecurityManager {
		
	@Override
	public void checkMemberAccess(Class<?> clazz, int which) {
		//super.checkMemberAccess(clazz, which);
		
		if(clazz.equals(Unsafe.class) || clazz.equals(System.class) || clazz.equals(Utils.class)) {
			throw new SecurityException("You are not allowed to use Unsafe in the project! Read description!");
		}
	}
	
	@Override
	public void checkPermission(Permission perm) {
		//super.checkPermission(perm);
	
		if(perm.getName().equals("setSecurityManager")) {
			throw new SecurityException("You are not allowed to change the SecurityManager!");		
		}
	}
	
	@Override
	public void checkPermission(Permission perm, Object context) {
		//super.checkPermission(perm, context);
		
		if(perm.getName().equals("setSecurityManager")) {
			throw new SecurityException("You are not allowed to change the SecurityManager!");			
		}
	}
	
	@Override
	public void checkExec(String cmd) {
		throw new SecurityException("Your are not allowed to create a subprocess!");
	}
	
	@Override
	public void checkConnect(String host, int port) {
		throw new SecurityException("Your are not allowed to use sockets!");
	}
	
	@Override
	public void checkConnect(String host, int port, Object context) {
		throw new SecurityException("Your are not allowed to use sockets!");
	}
	
	@Override
	public void checkListen(int port) {
		throw new SecurityException("Your are not allowed to use sockets!");
	}
	
	@Override
	public void checkAccept(String host, int port) {
		throw new SecurityException("Your are not allowed to use sockets!");
	}
	
	@Override
	public void checkMulticast(InetAddress maddr) {
		throw new SecurityException("Your are not allowed to use multicast!");
	}

}
