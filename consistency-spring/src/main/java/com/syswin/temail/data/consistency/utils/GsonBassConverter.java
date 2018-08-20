package com.syswin.temail.data.consistency.utils;

import com.google.gson.Gson;
import org.springframework.stereotype.Service;

@Service
public class GsonBassConverter implements JsonConverter {

  private final Gson gson = new Gson();

  @Override
  public String toString(Object o) {
    return gson.toJson(o);
  }
}
