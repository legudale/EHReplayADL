using Microsoft.Hadoop.Avro.Container;
using System;
using System.Collections.Generic;
using System.IO;

namespace EHReplay
{
    internal class AvroStreamReader : IDisposable
    {
        internal AvroStreamReader(Stream stream)
        {
            Stream = stream;
        }

        public Stream Stream { get; }

        public void Dispose()
        {
            Stream.Dispose();
        }

        internal IEnumerable<ArchiveEvent> GetEvents()
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