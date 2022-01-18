package com.cloudside.bqprotodemo;

import java.io.IOException;
import java.time.Duration;
import java.util.Random;
import java.util.logging.Logger;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import com.cloudside.bqprotodemo.Students.Subjects;
import com.cloudside.bqprotodemo.Students.Subjects.Teachers;
// [START bigquerystorage_jsonstreamwriter_parallelcommitted]
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BQTableSchemaToProtoDescriptor;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.CreateWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.google.cloud.bigquery.storage.v1.ProtoSchemaConverter;
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;

public class ParallelWriteCommittedStream {

  private static final Logger LOG = Logger.getLogger(ParallelWriteCommittedStream.class.getName());

  // Total amount of test time.
  private static final Duration TEST_TIME = Duration.ofSeconds(10);

  // How often to publish append stats.
  private static final Duration METRICS_GAP = Duration.ofSeconds(5);

  // Size of each row to append.
  private static final int ROW_SIZE = 1024;

  // The number of rows in each append request.
  private static final long BATCH_SIZE = 10;

  // If true, switch to a new stream when append fails.
  // If false, do not switch to a new stream.
  private static final boolean SUPPORT_STREAM_SWITCH = false;

  @GuardedBy("this")
  private long inflightCount = 0;

  @GuardedBy("this")
  private long successCount = 0;

  @GuardedBy("this")
  private long failureCount = 0;

  @GuardedBy("this")
  private Throwable error = null;

  @GuardedBy("this")
  private long lastMetricsTimeMillis = 0;

  @GuardedBy("this")
  private long lastMetricsSuccessCount = 0;

  @GuardedBy("this")
  private long lastMetricsFailureCount = 0;


  public static void main(String... args) throws Exception {
    String projectId = "GCP-PROJECT-ID";
    String datasetName = "GCP-BQ-DATASET-NAME";
    String tableName = "GCP-BQ-TABLE-NAME";
    writeCommittedStream(projectId, datasetName, tableName);

  }


  public void writeLoop(
      String projectId, String datasetName, String tableName, BigQueryWriteClient client) {
    LOG.info("Start writeLoop");
    long streamSwitchCount = 0;
    long successRowCount = 0;
    long failureRowCount = 0;
    Throwable loggedError = null;
    long deadlineMillis = System.currentTimeMillis() + TEST_TIME.toMillis();
    while (System.currentTimeMillis() < deadlineMillis) {
      try {
        WriteStream writeStream = createStream(projectId, datasetName, tableName, client);
        writeToStream(client, writeStream, deadlineMillis);
      } catch (Throwable e) {
        LOG.warning("unexpected error writing to stream: " + e.toString());
      }
      waitForInflightToReachZero(Duration.ofMinutes(1));
      synchronized (this) {
        successRowCount += successCount * BATCH_SIZE;
        failureRowCount += failureCount * BATCH_SIZE;
        if (loggedError == null) {
          loggedError = error;
        }
      }
      if (!SUPPORT_STREAM_SWITCH) {
        // If stream switch is disabled, break.
        break;
      }
      LOG.info("Sleeping before switching stream.");
      sleepIgnoringInterruption(Duration.ofMinutes(1));
      streamSwitchCount++;
    }
    LOG.info(
        "Finish writeLoop. Success row count: "
            + successRowCount
            + " Failure row count: "
            + failureRowCount
            + " Logged error: "
            + loggedError
            + " Stream switch count: "
            + streamSwitchCount);
    if (successRowCount > 0 && failureRowCount == 0 && loggedError == null) {
      System.out.println("All records are appended successfully.");
    }
  }

  private WriteStream createStream(
      String projectId, String datasetName, String tableName, BigQueryWriteClient client) {
    LOG.info("Creating a new stream");
    // Initialize a write stream for the specified table.
    // For more information on WriteStream.Type, see:
    // https://googleapis.dev/java/google-cloud-bigquerystorage/latest/com/google/cloud/bigquery/storage/v1beta2/WriteStream.Type.html
    WriteStream stream = WriteStream.newBuilder().setType(WriteStream.Type.COMMITTED).build();
    TableName parentTable = TableName.of(projectId, datasetName, tableName);
    CreateWriteStreamRequest createWriteStreamRequest =
        CreateWriteStreamRequest.newBuilder()
            .setParent(parentTable.toString())
            .setWriteStream(stream)
            .build();
    return client.createWriteStream(createWriteStreamRequest);
  }

  private void writeToStream(
      BigQueryWriteClient client, WriteStream writeStream, long deadlineMillis) throws Throwable {
    LOG.info("Start writing to new stream:" + writeStream.getName());
    synchronized (this) {
      inflightCount = 0;
      successCount = 0;
      failureCount = 0;
      error = null;
      lastMetricsTimeMillis = System.currentTimeMillis();
      lastMetricsSuccessCount = 0;
      lastMetricsFailureCount = 0;
    }
    Descriptor descriptor =
        BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(
            writeStream.getTableSchema());
    ProtoSchema protoSchema = ProtoSchemaConverter.convert(descriptor);
    try (StreamWriter writer =
        StreamWriter.newBuilder(writeStream.getName())
            .setWriterSchema(protoSchema)
            .setTraceId("SAMPLE:parallel_append")
            .build()) {
      while (System.currentTimeMillis() < deadlineMillis) {
        synchronized (this) {
          if (error != null) {
            // Stop writing once we get an error.
            throw error;
          }
        }
        ApiFuture<AppendRowsResponse> future = writer.append(createAppendRows(), -1);
        synchronized (this) {
          inflightCount++;
        }
        ApiFutures.addCallback(
            future, new AppendCompleteCallback(this), MoreExecutors.directExecutor());
      }
    }
  }

  private void waitForInflightToReachZero(Duration timeout) {
    LOG.info("Waiting for inflight count to reach zero.");
    long deadlineMillis = System.currentTimeMillis() + timeout.toMillis();
    System.out.println(deadlineMillis);
    System.out.println(System.currentTimeMillis());
    System.out.println("************************");

    while (System.currentTimeMillis() < deadlineMillis) {
      synchronized (this) {
        if (inflightCount == 0) {
          LOG.info("Inflight count has reached zero.");
          return;
        }
      }
      sleepIgnoringInterruption(Duration.ofSeconds(1));
    }
    throw new RuntimeException("Timeout waiting for inflight count to reach 0");
  }

  private ProtoRows createAppendRows() {

    Students.Builder student = Students.newBuilder();
    student.setId(4);
    student.setName("Student 7");
    student.setAge(34);
    student.setEnrollTime("23423423");

    Subjects.Builder subject = Subjects.newBuilder();
    subject.setId(6);
    subject.setName("Subject 7");
    subject.setMarksObtained(100);
    subject.setMaxMarks(100);

    Subjects.Builder subject8 = Subjects.newBuilder();
    subject8.setId(8);
    subject8.setName("Subject 8");
    subject8.setMarksObtained(77);
    subject8.setMaxMarks(100);


    Teachers.Builder teacher = Teachers.newBuilder();
    teacher.setId(4);
    teacher.setName("Teacher 4");

    subject.setTeacher(teacher.build());
    subject8.setTeacher(teacher.build());

    student.addSubject(subject.build());
    student.addSubject(subject8.build());

    ProtoRows.Builder protorowbuilder = ProtoRows.newBuilder();
    ProtoRows protorow = protorowbuilder.addSerializedRows(student.build().toByteString()).build();

    return protorow;
  }


  private ProtoRows createAppendRows(Descriptor descriptor) {
    ProtoRows.Builder rowsBuilder = ProtoRows.newBuilder();

      // SAMPLE PROTO OBJECTS. FEEL FREE TO MODIFY   
      Students.Builder student = Students.newBuilder();
      student.setId(new Random().nextInt(500));
      student.setName("Student 1");
      student.setAge(new Random().nextInt(22));
      student.setEnrollTime(String.valueOf(System.currentTimeMillis()));
      
      Subjects.Builder subject1 = Subjects.newBuilder();
      subject1.setId(new Random().nextInt(10));
      subject1.setName("Subject 1");
      subject1.setMarksObtained(new Random().nextInt(100));
      subject1.setMaxMarks(100);
    
      Subjects.Builder subject2 = Subjects.newBuilder();
      subject1.setId(new Random().nextInt(10));
      subject1.setName("Subject 1");
      subject1.setMarksObtained(new Random().nextInt(100));
      subject1.setMaxMarks(100);
      
      Teachers.Builder teacher = Teachers.newBuilder();
      teacher.setId(new Random().nextInt(10));
      teacher.setName("Teacher 1");
      
      subject1.setTeacher(teacher.build());
      subject2.setTeacher(teacher.build());
  
      student.addSubject(subject1.build());
      student.addSubject(subject2.build());
  
      rowsBuilder.addSerializedRows(student.build().toByteString());
      System.out.println("************************");
      System.out.println(rowsBuilder.toString());
      System.out.println("************************");

    // }
    System.out.println("************************");
    System.out.println(rowsBuilder.toString());
    System.out.println("************************");

    return rowsBuilder.build();
  }

  private void sleepIgnoringInterruption(Duration duration) {
    try {
      Thread.sleep(duration.toMillis());
    } catch (InterruptedException e) {
      LOG.warning("Sleep is interrupted.");
    }
  }

  /*
   * Callback when Append request is completed.
   *
   * It keeps track of count.
   */
  private class AppendCompleteCallback implements ApiFutureCallback<AppendRowsResponse> {

    private final ParallelWriteCommittedStream parent;

    AppendCompleteCallback(ParallelWriteCommittedStream parent) {
      this.parent = parent;
    }

    @Override
    public void onSuccess(@Nullable AppendRowsResponse response) {
      synchronized (parent) {
        parent.inflightCount--;
        if (!response.hasError()) {
          parent.successCount++;
        } else {
          parent.failureCount++;
        }
        long nowMillis = System.currentTimeMillis();
        if (nowMillis >= parent.lastMetricsTimeMillis + METRICS_GAP.toMillis()) {
          long successCountInIteration = parent.successCount - parent.lastMetricsSuccessCount;
          long failureCountInIteration = parent.failureCount - parent.lastMetricsFailureCount;
          long metricsTimeMillis = nowMillis - parent.lastMetricsTimeMillis;
          LOG.info(
              "Success append: "
                  + successCountInIteration
                  + " failure append: "
                  + failureCountInIteration
                  + " in "
                  + metricsTimeMillis
                  + "ms. Successful MB Per Second: "
                  + (double) (successCountInIteration * BATCH_SIZE * ROW_SIZE)
                      / metricsTimeMillis
                      / 1000
                  + " Current inflight: "
                  + parent.inflightCount);
          parent.lastMetricsTimeMillis = System.currentTimeMillis();
          parent.lastMetricsSuccessCount = parent.successCount;
          parent.lastMetricsFailureCount = parent.failureCount;
        }
      }
    }

    @Override
    public void onFailure(Throwable throwable) {
      synchronized (parent) {
        parent.inflightCount--;
        parent.error = throwable;
        LOG.warning("Found failure: " + throwable.toString());
      }
    }
  }

  public static void writeCommittedStream(String projectId, String datasetName, String tableName)
      throws DescriptorValidationException, InterruptedException, IOException {
    try (BigQueryWriteClient client = BigQueryWriteClient.create()) {
      new ParallelWriteCommittedStream().writeLoop(projectId, datasetName, tableName, client);
    } catch (Exception e) {
      System.out.println("Failed to append records. \n" + e.toString());
    }
  }
}
// [END bigquerystorage_jsonstreamwriter_parallelcommitted]