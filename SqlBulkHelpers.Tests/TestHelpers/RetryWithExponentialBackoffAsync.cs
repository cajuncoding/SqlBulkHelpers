using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace SqlBulkHelpers.Utilities
{
    /// <summary>
    /// Simple but effective Retry mechanism for C# with Exponential backoff and support for validating each result to determine if it should continue trying or accept the result.
    /// https://en.wikipedia.org/wiki/Exponential_backoff
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="maxRetries">The Max number of attempts that will be made.</param>
    /// <param name="action">The Func&lt;T&gt; process/action that will be attempted and will return generic type &lt;T&gt; when successful.</param>
    /// <param name="validationAction">A dynamic validation rule that can determine if a given result of generic type &lt;T&gt; is acceptable or if the action should be re-attempted.</param>
    /// <param name="initialRetryWaitTimeMillis">The initial delay time if the action fails; after which it will be exponentially expanded for longer delays with each iteration.</param>
    /// <returns>Generic type &lt;T&gt; result from the Func&lt;T&gt; action specified.</returns>
    /// <exception cref="AggregateException">Any and all exceptions that occur from all attempts made before the max number of retries was encountered.</exception>
    public class Retry
    {
        public static async Task<T> WithExponentialBackoffAsync<T>(
            int maxRetries,
            Func<Task<T>> action,
            Func<T, bool> validationAction = null,
            int initialRetryWaitTimeMillis = 1000
        )
        {
            var exceptions = new List<Exception>();
            var maxRetryValidatedCount = Math.Max(maxRetries, 1);

            //NOTE: We always make an initial attempt (index = 0, with NO delay) + the max number of retries attempts with 
            //      exponential back-off delays; so for example with a maxRetries specified of 3 + 1 for the initial
            //      we will make a total of 4 attempts!
            for (var failCount = 0; failCount <= maxRetryValidatedCount; failCount++)
            {
                try
                {
                    //If we are retrying then we wait using an exponential back-off delay...
                    if (failCount > 0)
                    {
                        var powerFactor = Math.Pow(failCount, 2); //This is our Exponential Factor
                        var waitTimeSpan = TimeSpan.FromMilliseconds(powerFactor * initialRetryWaitTimeMillis); //Total Wait Time
                        await Task.Delay(waitTimeSpan).ConfigureAwait(false);
                    }

                    //Attempt the Action...
                    var result = await action().ConfigureAwait(false);

                    //If successful and specified then validate the result; if invalid then continue retrying...
                    var isValid = validationAction?.Invoke(result) ?? true;
                    if (isValid)
                        return result;
                }
                catch (Exception exc)
                {
                    exceptions.Add(exc);
                }
            }

            //If we have Exceptions that were handled then we attempt to re-throw them so calling code can handle...
            if (exceptions.Count > 0)
                throw new AggregateException(exceptions);

            //Finally if no exceptions were handled (e.g. all failures were due to validateResult Func failing them) then we return the default (e.g. null)...
            return default;
        }
    }
}