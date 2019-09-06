using System.Collections.Generic;

namespace EHReplayADL
{
    public interface IEventHubArchive
    {
        IEnumerable<ArchiveItem> GetItems();
    }
}