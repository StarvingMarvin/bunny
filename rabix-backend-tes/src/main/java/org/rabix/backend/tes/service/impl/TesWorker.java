package org.rabix.backend.tes.service.impl;

import org.rabix.backend.tes.client.TESHttpClient;

import com.google.inject.Inject;

public class TesWorker extends LocalWorkerServiceImpl {
  
  @Inject
  TESHttpClient tesHttpClient;

  @Override
  public String getType() {
    return "TES";
  }

}
