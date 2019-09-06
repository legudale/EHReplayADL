using System.Collections.Generic;

namespace EHReplay
{
    public interface IEventHubArchive
    {
        IEnumerable<ArchiveItem> GetItems();
    }
}