using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;

namespace EHReplayADL
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            const string configFile = "appsettings.json";
            var config = new ConfigurationBuilder()
                .AddJsonFile(configFile, true, true)
                .Build();
            var ctx = ExecutionContext.FromConfig(config);
            var archive = AdlArchive.FromConfig(ctx, config);
            var producer = new ArchiveEventProducer(ctx, archive);
            var consumer = ArchiveEventConsumer.FromConfig(ctx, config);
            var watermarkStorage = new FileWatermarkStorage();
            var watermarkManagers = new Dictionary<int, PartitionWatermarkManager>();


            foreach (var item in producer.ProduceItems())
            {
                if (ctx.Noisy) Console.WriteLine($"processing {item}...");
                PartitionWatermarkManager manager = null;
                if (!watermarkManagers.ContainsKey(item.Partition))
                {
                    manager = new PartitionWatermarkManager(watermarkStorage, item.Partition);
                    watermarkManagers[item.Partition] = manager;
                }

                manager = watermarkManagers[item.Partition];
                if (!manager.ShouldProceedWith(item, null))
                {
                    if (ctx.Noisy) Console.WriteLine("Skipping as below the watermark");
                }
                else

                {
                    var eventsPerItem = 0;
                    long sizePerItem = 0;
                    var itemStart = DateTime.UtcNow;
                    var batch = new List<ArchiveEvent>();
                    foreach (var ev in producer.ProduceEvents(item))

                    {
                        eventsPerItem++;
                        sizePerItem += ev.Body.Length;

                        if (ctx.Boundaries != null && item.Partition < ctx.Boundaries.Count)
                            if (ev.SequenceNumber > ctx.Boundaries[item.Partition])
                            {
                                if (ctx.Noisy)
                                    Console.WriteLine(
                                        $"Skipping sequence number {ev.SequenceNumber} for partiton {item.Partition} as higher than the boundary");
                                continue;
                            }


                        if (manager.ShouldProceedWith(item, ev))
                            batch.Add(ev);
                        else if (ctx.Noisy)
                            Console.WriteLine($"skipping sequenceNumber {ev.SequenceNumber} as below the watermark");
                    }

                    if (!ctx.DryRun)
                    {
                        if (!consumer.TryConsumeBatch(item, batch))
                        {
                            if (ctx.Noisy) Console.WriteLine("Failed to send events to the Event Hub");
                        }
                        else
                        {
                            manager.UpdateWatermark(item, batch[batch.Count - 1]);
                        }
                    }

                    if (ctx.Noisy)
                    {
                        if (eventsPerItem == 0)
                        {
                            Console.WriteLine("Item size too small");
                        }
                        else
                        {
                            Console.WriteLine($"events: {eventsPerItem}, size: {sizePerItem}");
                            var secs = (DateTime.UtcNow - itemStart).TotalSeconds;
                            Console.WriteLine($"time per item: {secs} sec");
                            Console.WriteLine($"Throughput: {eventsPerItem / secs} events/sec");
                            Console.WriteLine($"Throughput: {sizePerItem / secs} bytes/sec");
                        }
                    }
                }
            }
        }
    }
}