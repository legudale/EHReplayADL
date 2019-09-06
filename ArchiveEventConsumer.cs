using Microsoft.Azure.EventHubs;
using Microsoft.Extensions.Configuration;
using System;

namespace EHReplay
{
    class ArchiveEventConsumer
    {
        private EventHubClient EventHubClient { get; set; }

        public static ArchiveEventConsumer FromConfig(string configFile)
        {
            var config = new ConfigurationBuilder()
                .AddJsonFile(configFile, true, true)
                .Build();

            var connectionString = config["destinationEventHubConnectionString"];
            var client = EventHubClient.CreateFromConnectionString(connectionString);
            return new ArchiveEventConsumer()
            {
                EventHubClient = client
            };
        }

        public bool TryConsumeEvent(ArchiveItem archiveItem, ArchiveEvent archiveEvent)
        {
            var eventData = new EventData(archiveEvent.Body);
            foreach (var entry in archiveEvent.Properties)
            {
                eventData.Properties[entry.Key] = entry.Value;

            }

            foreach (var entry in archiveEvent.SystemProperties)
            {
                eventData.SystemProperties[entry.Key] = entry.Value;

            }

            try
            {
                EventHubClient.SendAsync(eventData, archiveItem.Partition.ToString()).Wait();
                return true;
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                return false;
            }

        }
    }
}
