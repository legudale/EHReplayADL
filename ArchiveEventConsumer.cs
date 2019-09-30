using Microsoft.Azure.EventHubs;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;

namespace EHReplayADL
{
    internal class ArchiveEventConsumer
    {
        private EventHubClient EventHubClient { get; set; }
        private ExecutionContext Ctx { get; set; }

        public static ArchiveEventConsumer FromConfig(ExecutionContext ctx, IConfigurationRoot config)
        {
            var connectionString = config["destinationEHConnectionString"];
            var client = EventHubClient.CreateFromConnectionString(connectionString);
            return new ArchiveEventConsumer
            {
                EventHubClient = client,
                Ctx = ctx
            };
        }


        public bool TryConsumeBatch(ArchiveItem item, List<ArchiveEvent> events,
            PartitionWatermarkManager watermarkManager)
        {
            var batch = new EventDataBatch(Ctx.MaxBatchSize, item.Partition.ToString());
            foreach (var ev in events)
            {
                if (!watermarkManager.ShouldProceedWith(item, ev))
                {
                    if (Ctx.Noisy)
                        Console.WriteLine($"skipping sequenceNumber {ev.SequenceNumber} as below the watermark");
                    continue;
                }

                var eventData = new EventData(ev.Body);
                foreach (var entry in ev.Properties) eventData.Properties[entry.Key] = entry.Value;
                if (!batch.TryAdd(eventData)) return TryConsumeBatchIndividually(item, events, watermarkManager);
            }

            try
            {
                EventHubClient.SendAsync(batch).Wait();
            }
            catch (Exception)
            {
                return TryConsumeBatchIndividually(item, events, watermarkManager);
            }

            watermarkManager.UpdateWatermark(item, events[events.Count - 1]);
            return true;
        }

        public bool TryConsumeBatchIndividually(ArchiveItem item, List<ArchiveEvent> events,
            PartitionWatermarkManager watermarkManager)
        {
            foreach (var ev in events)
            {
                if (!watermarkManager.ShouldProceedWith(item, ev))
                {
                    if (Ctx.Noisy)
                        Console.WriteLine($"skipping sequenceNumber {ev.SequenceNumber} as below the watermark");
                    continue;
                }

                var eventData = new EventData(ev.Body);
                foreach (var entry in ev.Properties) eventData.Properties[entry.Key] = entry.Value;
                try
                {
                    EventHubClient.SendAsync(eventData, item.Partition.ToString()).Wait();
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.ToString());
                    return false;
                }

                watermarkManager.UpdateWatermark(item, ev);
            }

            return true;
        }
    }
}