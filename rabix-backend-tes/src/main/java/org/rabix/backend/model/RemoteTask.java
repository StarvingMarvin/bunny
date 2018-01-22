package org.rabix.backend.model;

import java.net.URI;

public interface RemoteTask {
  URI getOutputLocation();

  boolean isSuccess();

  boolean isFinished();
  
  String getError();
}
