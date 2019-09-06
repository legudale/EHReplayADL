using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;

namespace EHReplay
{



    public class BlobArchive : IEventHubArchive
    {
        private CloudBlobContainer Container { get; set; }

        public IEnumerable<ArchiveItem> GetItems()
        {
            return Container.ListBlobs(useFlatBlobListing: true).Select(x => new BlobArchiveItem(x));
        }

        public static IEventHubArchive FromConfig(string configFile)
        {
            var config = new ConfigurationBuilder()
                .AddJsonFile(configFile, true, true)
                .Build();

            var connectionString = config["blobArchiveConnectionString"];
            var containerName = config["blobArchiveContainerName"];
            var blobArchive = new BlobArchive();
            if (!CloudStorageAccount.TryParse(connectionString, out var storageAccount))
                throw new ApplicationException("Wrong config");

            var client = storageAccount.CreateCloudBlobClient();
            blobArchive.Container = client.GetContainerReference(containerName);
            return blobArchive;
        }
    }
}