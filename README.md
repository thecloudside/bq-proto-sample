# Google BigQuery Streaming Inserts

Java code for Streaming Inserts - CommittedWrite and ParallelWriteCommitted. This uses sample code from Google BQ Java repository [https://github.com/googleapis/java-bigquerystorage/tree/main/samples/snippets](https://github.com/googleapis/java-bigquerystorage/tree/main/samples/snippets)


# Pre-requisites

- Setup GOOGLE_APPLICATION_CREDENTIALS by following the link below
https://cloud.google.com/docs/authentication/getting-started

# Steps to Generate Proto Class Files

Once *.proto is designed, go to the diretory where the .proto file is available and run the below command 

> protoc -I=. --java_out=. student_py_nested.proto

This will generate the necessary class files needed for the proto schema in the corresponding package folder as mentioned in the proto file options java_package, java_outer_classname and package.

Copy the files into the right folder in an existing java project or use this folder structure as base for the new java project


## Steps to Generate Google BigQuery Schema file

Install Go lang 

Steps for installing in Mac

> ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
> brew update&& brew install golang


Install the Go lang based proto file to BQ schema converter file from the below link
[protoc-gen-bq-schema/README.md at master · GoogleCloudPlatform/protoc-gen-bq-schema (github.com)](https://github.com/GoogleCloudPlatform/protoc-gen-bq-schema/blob/master/README.md)
> go get github.com/GoogleCloudPlatform/protoc-gen-bq-schema

Add Go lang and protoc to the Path variable by using the below command

> export PATH=$PATH:/Users/<user-name>/go/bin

Follow the instructions mentioned in the link to generate the BQ schema file

eg: 

> sudo protoc --bq-schema_out=. --bq-schema_opt=single-message proto_file_name.proto

Use the schema file generated to create the table with schema in BigQuery via GCP console

Reference: [Creating and using tables | BigQuery | Google Cloud](https://cloud.google.com/bigquery/docs/tables#creating_an_empty_table_with_a_schema_definition)

