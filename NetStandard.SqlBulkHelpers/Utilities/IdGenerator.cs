using System;
using System.Linq;
using System.Text;

namespace SqlBulkHelpers
{
    /// <summary>
    /// BBernard / CajunCoding
    /// A simple, relatively efficient, class for generating very unique Ids of arbitrary length.
    /// They are not guaranteed to universally unique but the risk of collisions is of no practical implication for many uses (e.g. temporary names, copied names, etc.).
    /// With the ability to control the length (longer lengths will be more unique) it becomes suitable for many use cases where a full GUID is simply too long.
    /// NOTE: Inspired by the Stack Overflow Answer here: https://stackoverflow.com/a/44960751/7293142
    ///        The Original author claims 0.001% duplicates in 100 million.
    /// </summary>
    public static class IdGenerator
    {
        private static readonly string[] allCharsArray = Enumerable
            //Start with the Full Uppercase Alphabet (26 chars) starting at 'A'
            .Range(65, 26).Select(e => ((char)e).ToString())
            //Append Full Lowercase Alphabet (26 chars) starting at 'a'
            .Concat(Enumerable.Range(97, 26).Select(e => ((char)e).ToString()))
            //Append Integer values 0-9
            .Concat(Enumerable.Range(0, 10).Select(e => e.ToString()))
            .ToArray();


        /// <summary>
        /// Generates a unique short ID that is much smaller than a GUID while being very unique --
        ///     but still not 100% unique because neither is a GUID.
        /// </summary>
        /// <returns></returns>
        public static string NewId(int length = 10)
        {
            var stringBuilder = new StringBuilder();
            allCharsArray
                //Randomize by Sorting on Completely Unique GUID values (that change with every request);
                //  effectively deriving the uniqueness from GUIDs!
                .OrderBy(e => Guid.NewGuid())
                .Take(length)
                .ToList()
                .ForEach(e => stringBuilder.Append(e));

            var linqId = stringBuilder.ToString();
            return linqId;
        }
    }
}
