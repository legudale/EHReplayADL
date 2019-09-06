using System.Collections.Generic;

namespace EHReplayADL
{
    internal class ArchiveEventProducer
    {
        private readonly IEventHubArchive _archive;
        private readonly ExecutionContext _ctx;

        public ArchiveEventProducer(ExecutionContext ctx, IEventHubArchive archive)
        {
            _archive = archive;
            _ctx = ctx;
        }

        public IEnumerable<ArchiveItem> ProduceItems()
        {
            return _archive.GetItems();
        }

        public IEnumerable<ArchiveEvent> ProduceEvents(ArchiveItem item)
        {
            using (var reader = new AvroStreamReader(_ctx, item.GetStream()))
            {
                foreach (var ev in reader.GetEvents()) yield return ev;
            }
        }
    }
}