package com.rahullokurte.pulsario;

import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

public class LocalFileSink implements Sink<String> {

  public static final String FILENAME_PREFIX_CONFIG_KEY = "filenamePrefix";
  public static final String FILENAME_SUFFIX_CONFIG_KEY = "filenameSuffix";
  private String filenamePrefix;
  private String filenameSuffix;
  private BufferedWriter bw = null;
  private FileWriter fw = null;

  public void close() throws Exception {
    try {
      if (bw != null) bw.close();
      if (fw != null) fw.close();
    } catch (IOException ex) {
      ex.printStackTrace();
    }
  }

  public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
    filenamePrefix = (String) config.getOrDefault(FILENAME_PREFIX_CONFIG_KEY, "test-out");
    filenameSuffix = (String) config.getOrDefault(FILENAME_SUFFIX_CONFIG_KEY, ".tmp");
    File file = File.createTempFile(filenamePrefix, filenameSuffix);
    fw = new FileWriter(file.getAbsoluteFile(), true);
    bw = new BufferedWriter(fw);
  }

  public void write(Record<String> record) throws Exception {
    try {
      bw.write(record.getValue());
      bw.flush();
      record.ack();
    } catch (IOException e) {
      record.fail();
      throw new RuntimeException(e);
    }
  }
}
