namespace EHReplay
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
            Day = item.Year;
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

        public bool IsHigherOrEqualThan(ArchiveItem item, ArchiveEvent ev)
        {
            if (Year < item.Year) return false;

            if (Month < item.Month) return false;

            if (Day < item.Day) return false;

            if (Hour < item.Hour) return false;

            if (Minute < item.Minute) return false;

            if (Second < item.Second) return false;

            if (SequenceNumber < ev.SequenceNumber) return false;

            return true;
        }
    }
}