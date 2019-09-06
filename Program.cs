using System;
using System.Collections.Generic;

namespace EHReplay
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            var blobArchive = BlobArchive.FromConfig("appsettings.json");
            var watermarkStorage = new FileWatermarkStorage();
            var watermarkManagers = new Dictionary<int, PartitionWatermarkManager>();
            var producer = new ArchiveEventProducer(blobArchive);
            var consumer = ArchiveEventConsumer.FromConfig("appsettings.json");
            long sent = 0;
            foreach (var (item, ev) in producer.ProduceEvents())
            {
                PartitionWatermarkManager manager = null;
                if (watermarkManagers.ContainsKey(item.Partition))
                {
                    manager = watermarkManagers[item.Partition];
                }
                else
                {
                    manager = new PartitionWatermarkManager(watermarkStorage, item.Partition);
                    watermarkManagers[item.Partition] = manager;

                }

                if (manager.ShouldProceedWith(item, ev))
                {
                    if (consumer.TryConsumeEvent(item, ev))
                    {
                        manager.UpdateWatermark(item, ev);
                    }

                    if (++sent % 1000 == 0)
                    {
                        Console.WriteLine($"sent {sent} events");
                    }
                }
            }
        }
    }
}