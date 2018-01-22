package org.rabix.backend.service;

public class RemoteServiceException extends Exception {

  private static final long serialVersionUID = -3341213832183821325L;
  
  public RemoteServiceException(String message) {
    super(message);
  }

  public RemoteServiceException(String message, Throwable e) {
    super(message, e);
  }

  public RemoteServiceException(Throwable e) {
    super(e);
  }

}
