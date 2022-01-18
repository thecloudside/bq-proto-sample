package com.cloudside.bqprotodemo;

import java.io.IOException;

import com.cloudside.bqprotodemo.Students.Subjects;
import com.cloudside.bqprotodemo.Students.Subjects.Teachers;
import com.google.api.core.ApiFuture;
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
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;

public class BQStreamingProtoCommittedStream {

  public static void main(String... args) throws Exception {

    // SAMPLE PROTO OBJECTS. FEEL FREE TO MODIFY   
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

    // ProtoRows protorow = ProtoRows.parseFrom(student.build().toByteArray());
    System.out.println(protorow.toString());  
    runWriteCommittedStream(protorow);

  }

  public static void runWriteCommittedStream(ProtoRows protoRow)
      throws DescriptorValidationException, InterruptedException, IOException {
    String projectId = "GCP-PROJECT-ID";
    String datasetName = "GCP-BQ-DATASET-NAME";
    String tableName = "GCP-BQ-TABLE-NAME";
    writeCommittedStream(projectId, datasetName, tableName, protoRow);
  }

  public static void writeCommittedStream(String projectId, String datasetName, String tableName, ProtoRows protoRow)
      throws DescriptorValidationException, InterruptedException, IOException {

    try (BigQueryWriteClient client = BigQueryWriteClient.create()) {
      WriteStream stream = WriteStream.newBuilder().setType(WriteStream.Type.COMMITTED).build();
      TableName parentTable = TableName.of(projectId, datasetName, tableName);
      CreateWriteStreamRequest createWriteStreamRequest = CreateWriteStreamRequest.newBuilder()
          .setParent(parentTable.toString())
          .setWriteStream(stream)
          .build();
      WriteStream writeStream = client.createWriteStream(createWriteStreamRequest);

      Descriptor descriptor =
      BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(
          writeStream.getTableSchema());
  ProtoSchema protoSchema = ProtoSchemaConverter.convert(descriptor);


      try (StreamWriter writer = StreamWriter.newBuilder(writeStream.getName(), client)
          .setWriterSchema(protoSchema)
          .build()) {
        // To detect duplicate records, pass the index as the record offset.
        // To disable deduplication, omit the offset or use WriteStream.Type.DEFAULT.
        ApiFuture<AppendRowsResponse> future = writer.append(protoRow, WriteStream.Type.TYPE_UNSPECIFIED_VALUE);

        AppendRowsResponse response = future.get();
        System.out.println("************************");
        System.out.println(response.toString());
        System.out.println("************************");
        System.out.println("************************");

      }

      System.out.println("Appended records successfully.");
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("Failed to append records. \n" + e.toString());
    }
  }

}