namespace EHReplayADL
{
    internal class PartitionWatermarkManager
    {
        private readonly int _partitionId;

        private readonly IWatermarkStorage _storage;
        private Watermark _watermark;

        public PartitionWatermarkManager(IWatermarkStorage storage, int partitionId)
        {
            _partitionId = partitionId;
            _storage = storage;
            _storage.TryLoadWatermarkForPartition(partitionId, out _watermark);
        }

        public bool ShouldProceedWith(ArchiveItem item, ArchiveEvent? ev)
        {
            if (item.Partition != _partitionId) return false;
            return _watermark == null || !_watermark.IsHigherOrEqualThan(item, ev);
        }

        public void UpdateWatermark(ArchiveItem item, ArchiveEvent ev)
        {
            _watermark = new Watermark(item, ev);
            _storage.SaveWatermarkForPartition(_partitionId, _watermark);
        }
    }
}