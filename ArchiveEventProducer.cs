using System.Collections.Generic;
using System.Linq;

namespace EHReplay
{
    public class ArchiveEventProducer
    {
        private readonly IEventHubArchive _archive;

        public ArchiveEventProducer(IEventHubArchive archive)
        {
            _archive = archive;

        }

        public IEnumerable<(ArchiveItem, ArchiveEvent)> ProduceEvents()
        {
            var items = _archive.GetItems().ToList();
            items.Sort();
            foreach (var item in items)
            {
                using (var reader = new AvroStreamReader(item.GetStream()))
                {
                    foreach (var ev in reader.GetEvents())
                    {
                        yield return (item, ev);
                    }
                }
            }
        }
    }

}