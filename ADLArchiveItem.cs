using Microsoft.Azure.DataLake.Store;
using System;
using System.IO;
using System.Text.RegularExpressions;

namespace EHReplayADL
{
    public class ADLArchiveItem : ArchiveItem
    {
        private static readonly Regex Rx =
            new Regex(
                @".+/(?<partition>\d+)/(?<year>\d+)/(?<month>\d+)/(?<day>\d+)/(?<hour>\d+)/(?<minute>\d+)/(?<second>\d+)\.avro");

        private readonly AdlsClient _client;
        private readonly DirectoryEntry _entry;

        public ADLArchiveItem(AdlsClient client, DirectoryEntry entry)
        {
            _client = client;
            _entry = entry;
            var match = Rx.Match(entry.FullName);
            if (!match.Success) throw new ApplicationException("Cannot parse the DirectoryEntry");
            Second = int.Parse(match.Groups["second"].ToString());
            Minute = int.Parse(match.Groups["minute"].ToString());
            Hour = int.Parse(match.Groups["hour"].ToString());
            Day = int.Parse(match.Groups["day"].ToString());
            Month = int.Parse(match.Groups["month"].ToString());
            Year = int.Parse(match.Groups["year"].ToString());
            Partition = int.Parse(match.Groups["partition"].ToString());
        }

        public override int Second { get; }

        public override int Minute { get; }

        public override int Hour { get; }

        public override int Day { get; }

        public override int Month { get; }

        public override int Year { get; }

        public override int Partition { get; }

        public override Stream GetStream()
        {
            return _client.GetReadStream(_entry.FullName);
        }

        public override string ToString()
        {
            return _entry.FullName;
        }
    }
}