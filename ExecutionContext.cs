using Microsoft.Extensions.Configuration;
using System.Collections.Generic;
using System.Linq;

namespace EHReplayADL
{
    internal class ExecutionContext
    {
        public bool DryRun { get; private set; }
        public bool Noisy { get; private set; }
        public int MinItemSize { get; private set; }
        public int MaxBatchSize { get; private set; }
        public List<long> Boundaries { get; private set; }

        public static ExecutionContext FromConfig(IConfigurationRoot config)
        {
            var ctx = new ExecutionContext();

            var dryRunString = config["dryRun"];
            ctx.DryRun = true;

            if (!string.IsNullOrEmpty(dryRunString))
                if (bool.TryParse(dryRunString, out var dryRun))
                    ctx.DryRun = dryRun;

            var noisyString = config["noisy"];
            ctx.Noisy = true;
            if (!string.IsNullOrEmpty(noisyString))
                if (bool.TryParse(noisyString, out var noisy))
                    ctx.Noisy = noisy;

            var minItemSizeString = config["minItemSize"];
            ctx.MinItemSize = 1024;
            if (!string.IsNullOrEmpty(minItemSizeString) && int.TryParse(minItemSizeString, out var minItemSize))
                ctx.MinItemSize = minItemSize;

            var maxBatchSizeString = config["maxBatchSize"];
            ctx.MaxBatchSize = 1024 * 1024;
            if (!string.IsNullOrEmpty(maxBatchSizeString) && int.TryParse(maxBatchSizeString, out var maxBatchSize))
                ctx.MaxBatchSize = maxBatchSize;

            var boundariesString = config["boundaries"];
            ctx.Boundaries = null;
            if (!string.IsNullOrEmpty(boundariesString))
            {
                var strings = boundariesString.Split(',').Select(s => s.Trim()).ToList();
                ctx.Boundaries = strings
                    .Select(s => long.TryParse(s, out var n) ? n : (long?)null)
                    .Where(n => n.HasValue)
                    .Select(n => n.Value)
                    .ToList();
            }

            return ctx;
        }
    }
}