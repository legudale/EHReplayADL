namespace EHReplayADL
{
    internal interface IWatermarkStorage
    {
        bool TryLoadWatermarkForPartition(int partitionId, out Watermark watermark);

        void SaveWatermarkForPartition(int partitionId, Watermark watermark);
    }
}