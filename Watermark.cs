using System;

namespace EHReplayADL
{
    internal class Watermark
    {
        public Watermark()
        {
        }

        public Watermark(ArchiveItem item, ArchiveEvent ev)
        {
            Year = item.Year;
            Month = item.Month;
            Day = item.Day;
            Hour = item.Hour;
            Minute = item.Minute;
            Second = item.Second;
            SequenceNumber = ev.SequenceNumber;
        }

        public int Year { get; set; }
        public int Month { get; set; }
        public int Day { get; set; }
        public int Hour { get; set; }
        public int Minute { get; set; }
        public int Second { get; set; }


        public long SequenceNumber { get; set; }


        public bool IsHigherOrEqualThan(ArchiveItem item, ArchiveEvent? ev)
        {
            var itemDt = new DateTime(item.Year, item.Month, item.Day, item.Hour, item.Minute, item.Second);
            var myDt = new DateTime(Year, Month, Day, Hour, Minute, Second);
            if (myDt > itemDt)
                return true;
            if (myDt < itemDt)
                return false;
            if (ev.HasValue)
                return SequenceNumber >= ev.Value.SequenceNumber;
            return true;
        }
    }
}