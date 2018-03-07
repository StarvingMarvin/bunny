package org.rabix.backend.service;

import org.apache.commons.configuration.Configuration;
import org.rabix.backend.google.service.GoogleCallable;
import org.rabix.backend.tes.client.TESHttpClient;
import org.rabix.backend.tes.service.impl.TESCallable;
import org.rabix.bindings.model.Job;

import com.google.inject.Inject;

public class TaskCallableFactory {
  @Inject
  RemoteStorageService storage;
  @Inject
  TESHttpClient tesHttpClient;
  @Inject
  Configuration config;

  public TaskRunCallable get(Job job) {
    String subtype = config.getString("backend.embedded.subtypes");
    if (subtype.equals("GOOGLE"))
      return new GoogleCallable(job, storage, config.getString("googlepipelines.projectid"));
    if (subtype.equals("TES"))
      return new TESCallable(job, storage, tesHttpClient);
    return null;
  }
}
