using Newtonsoft.Json;
using System;
using System.IO;

namespace EHReplayADL
{
    internal class FileWatermarkStorage : IWatermarkStorage
    {
        public void SaveWatermarkForPartition(int partitionId, Watermark watermark)
        {
            var fileName = FilenameForPartition(partitionId);
            var json = JsonConvert.SerializeObject(watermark);
            File.WriteAllText(fileName, json);
        }

        public bool TryLoadWatermarkForPartition(int partitionId, out Watermark watermark)
        {
            var fileName = FilenameForPartition(partitionId);
            watermark = null;
            if (File.Exists(fileName))
                try
                {
                    var json = File.ReadAllText(fileName);
                    watermark = JsonConvert.DeserializeObject<Watermark>(json);
                    return true;
                }
                catch (Exception)
                {
                    return false;
                }

            return false;
        }

        private static string FilenameForPartition(int partitionId)
        {
            return $"watermark_{partitionId}.json";
        }
    }
}
