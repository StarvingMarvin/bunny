package org.rabix.backend.tes.service.impl;

import java.net.URI;

import org.rabix.backend.model.RemoteTask;
import org.rabix.bindings.model.Job;

class WorkPair {
  Job job;
  RemoteTask task;

  public WorkPair(Job job, RemoteTask tesTask) {
    super();
    this.job = job;
    this.task = tesTask;
  }

  public WorkPair(Job job) {
    this.job = job;
    this.task = new RemoteTask() {

      @Override
      public boolean isSuccess() {
        return true;
      }

      @Override
      public URI getOutputLocation() {
        return null;
      }

      @Override
      public boolean isFinished() {
        return true;
      }

      @Override
      public String getError() {
        // TODO Auto-generated method stub
        return null;
      }
    };
  }
}
