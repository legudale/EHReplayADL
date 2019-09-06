# EHReplayADL

## Purpose

The goal of the sample is to demonstrate how to replay archive of the Event Hub Archive to the new EventHub.

EventHub supports archival option where the events passed through the Event Hub are persisted either in the Azure Log Store or Azure Data Lake. This sample demonstrates how the events can be retrieved from the Avro files (which is a storage format used by the EventArchive) and replayed into the new Event Hub. This sample demonstrated the case where the archive is stored in the Azure Data Lake, but has sufficient abstractions to easily be extended to support the other storage option, Azure Blob Storage.

## Build

The sample is build using the .net core SDK 2.2. It uses C# 7 syntax.

To build the sample run:

```

dotnet build

```

## Run

To run the sample execute:

```
dotnet run
```

Note that the configuration file appsettings.json must be present. One can find the template of this file in this repository.

## Configuration

The configuration options are:

- **dryRun**, when set to true, does not actually send events to the destination Event Hub

- **noisy**, when true, outputs the progress log to the console

**minItemSize**, the lowest size boundary of the file in the archive to be considered as containing actual events. If the size of the file is maller than this setting, it will be skipped.

- **boundaries**, upper boudaries of the sequenceNumbers to be replayed. If the event in archive is higher than the upperBoundary, it will be skipped.

- **adlClientId** the client id of the security principal on behalf of which the sample will be run. must have, read and execute permissions, recursively applied to the whole data lake.

- **adlClientSecret** client secret of the security principal.

- **adlPath** the url of the data lake in the form `xxx.azuredatalakestore.net`

- **adlRoot** the data lake folder considered to be root of the archive

- **destinationEHConnectionString**. The connection string to the destination Event Hub
- **maxBatchSize**, to increase efficiency, before sending events to the destination Event Hub, the sample tries to batch them, if the batching fails, the sample reverts to sending the items individually. This option controls the s
