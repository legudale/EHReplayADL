using Microsoft.Hadoop.Avro.Container;
using System;
using System.Collections.Generic;
using System.IO;

namespace EHReplayADL
{
    internal class AvroStreamReader : IDisposable
    {
        internal AvroStreamReader(ExecutionContext ctx, Stream stream)
        {
            Stream = stream;
            Ctx = ctx;
        }

        internal Stream Stream { get; }
        internal ExecutionContext Ctx { get; }

        public void Dispose()
        {
            Stream.Dispose();
        }

        internal IEnumerable<ArchiveEvent> GetEvents()
        {
            if (Stream.Length >= Ctx.MinItemSize)

            {
                using (var reader = AvroContainer.CreateGenericReader(Stream))
                {
                    while (reader.MoveNext())
                        foreach (dynamic result in reader.Current.Objects)
                        {
                            var record = new ArchiveEvent(result);
                            yield return record;
                        }
                }
            }
        }
    }
}