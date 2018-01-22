package org.rabix.backend.tes.service.impl;

import org.rabix.backend.service.RemoteStorageService;
import org.rabix.backend.tes.client.TESHttpClient;
import org.rabix.bindings.model.Job;

import com.google.inject.Inject;

public class TaskCallableFactory {
  @Inject
  RemoteStorageService storage;
  @Inject
  TESHttpClient tesHttpClient;

  public TaskRunCallable get(Job job) {
    //return new TESCallable(job, storage, tesHttpClient);
    return new GoogleCallable(job, storage);
  }
}
