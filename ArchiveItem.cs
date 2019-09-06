using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace EHReplayADL
{
    public abstract class ArchiveItem : IComparable
    {
        public abstract int Second { get; }
        public abstract int Minute { get; }
        public abstract int Hour { get; }
        public abstract int Day { get; }
        public abstract int Month { get; }
        public abstract int Year { get; }
        public abstract int Partition { get; }

        public int CompareTo(object obj)
        {
            switch (obj)
            {
                case ArchiveItem other:
                    if (this == other)
                    {
                        return 0;
                    }
                    else
                    {
                        var thisList = AsList();
                        var otherList = other.AsList();
                        Debug.Assert(thisList.Count == otherList.Count);
                        foreach (var item in thisList.Zip(otherList, (a, b) => new { a, b }))
                        {
                            var comp = item.a.CompareTo(item.b);
                            if (comp != 0) return comp;
                        }

                        return 0;
                    }

                default:
                    return 1;
            }
        }

        public IList<int> AsList()
        {
            return new List<int>
                {Partition, Year, Month, Day, Hour, Minute, Second};
        }

        public abstract Stream GetStream();
    }
}