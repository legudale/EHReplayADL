using Microsoft.Azure.Storage.Blob;
using System;
using System.Diagnostics;
using System.IO;
using System.Text.RegularExpressions;

namespace EHReplay
{
    public class BlobArchiveItem : ArchiveItem
    {
        private static readonly Regex Rx =
            new Regex(
                @".+/(?<partition>\d+)/(?<year>\d+)/(?<month>\d+)/(?<day>\d+)/(?<hour>\d+)/(?<minute>\d+)/(?<second>\d+)\.avro");

        private readonly CloudBlockBlob _blob;

        public BlobArchiveItem(IListBlobItem blobItem)
        {
            Debug.Assert(blobItem is CloudBlockBlob);
            _blob = (CloudBlockBlob)blobItem;
            var match = Rx.Match(blobItem.Uri.ToString());
            if (!match.Success) throw new ApplicationException("Cannot parse the blobUri");
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
            return _blob.OpenRead();
        }

        public override string ToString()
        {
            return _blob.Uri.ToString();
        }
    }
}