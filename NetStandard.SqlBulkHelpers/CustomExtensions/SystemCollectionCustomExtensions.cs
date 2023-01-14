using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlBulkHelpers.CustomExtensions
{
    internal static class SystemCollectionCustomExtensions
    {
        /// <summary>
        /// ForEach processing loop that supports full asynchronous processing of all values via the supplied async function!
        /// This will control the async threshold according the maximum number of concurrent tasks.
        /// Based on the original post by Jason Toub:https://devblogs.microsoft.com/pfxteam/implementing-a-simple-foreachasync-part-2/
        /// More info also here: https://stackoverflow.com/questions/11564506/nesting-await-in-parallel-foreach
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <param name="maxDegreesOfConcurrency"></param>
        /// <param name="asyncFunc"></param>
        /// <returns></returns>
        public static async Task ForEachAsync<T>(this IEnumerable<T> source, int maxDegreesOfConcurrency, Func<T, Task> asyncFunc)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));
            if (asyncFunc == null) throw new ArgumentNullException(nameof(asyncFunc));
            if (maxDegreesOfConcurrency <= 0) throw new ArgumentException($"{nameof(maxDegreesOfConcurrency)} must be an integer value greater than zero.");

            #if NET6_0
            
            //BBernard - Implemented optimization now in .NET6 using the new OOTB support now with Parallel.ForEachAsync()!
            var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = maxDegreesOfConcurrency };
            await Parallel.ForEachAsync(source, parallelOptions, async (item, token) =>
            {
                await asyncFunc(item).ConfigureAwait(false);
            }).ConfigureAwait(false);

            #else //NetStandard2.1 will use the following custom implementation!

            var partitions = Partitioner.Create(source).GetPartitions(maxDegreesOfConcurrency);
            var asyncTasksEnumerable = partitions.Select(async partition =>
            {
                using (partition)
                {
                    while (partition.MoveNext()) await asyncFunc(partition.Current).ConfigureAwait(false);
                }
            });
            await Task.WhenAll(asyncTasksEnumerable).ConfigureAwait(false);

            #endif
        }
    }
}
