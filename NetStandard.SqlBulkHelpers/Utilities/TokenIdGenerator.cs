using System;
using System.Linq;

namespace SqlBulkHelpers
{
    /// <summary>
    /// BBernard / CajunCoding
    /// A simple, relatively efficient, class for generating very unique Ids of arbitrary length.
    /// They are not guaranteed to universally unique but the risk of collisions is of no practical implication for many uses 
    ///     (e.g. temporary names, copied names, etc.).
    /// With the ability to control the length (longer lengths will be more unique) it becomes suitable for many use cases 
    ///     where a full GUID is simply too long.
    /// NOTE: Inspired by the Stack Overflow Answer here: https://stackoverflow.com/a/44960751/7293142
    ///        The Original author claims 0.001% duplicates in 100 million.
    ///        
    /// Taken directly from the Open Source Gist Here: https://gist.github.com/cajuncoding/9d11518e388082e0520676012bb94f5e
    /// </summary>
    public static class TokenIdGenerator
    {
        private static readonly char[] UppercaseCharsArray = Enumerable
            //Start with the Full Uppercase Alphabet (26 chars) starting at 'A'
            .Range(65, 26).Select(e => (char)e)
            //Append Integer values 0-9
            .Concat(Enumerable.Range(0, 10).Select(i => i.ToString()[0]))
            .ToArray();

        private static readonly char[] AllCharsArray = UppercaseCharsArray
            //Append Full Lowercase Alphabet (26 chars) starting at 'a'
            .Concat(Enumerable.Range(97, 26).Select(e => (char)e))
            .ToArray();

        /// <summary>
        /// Generates a unique short ID that can be much smaller than a GUID while being very unique --
        ///     but still not 100% unique because neither is a GUID. It may also be much longer makint it 
        ///     highly unique and far more alphanumerically complex than GUIDs.
        /// </summary>
        /// <returns></returns>
        public static string NewTokenId(int length = 10, bool useOnlyUppercaseCharacters = false)
        {
            var sourceCharsArray = useOnlyUppercaseCharacters ? UppercaseCharsArray : AllCharsArray;

            var charsArray = (length <= sourceCharsArray.Length)
                ? sourceCharsArray
                : Enumerable
                    .Repeat(sourceCharsArray, (int)Math.Ceiling((decimal)length / sourceCharsArray.Length))
                    .SelectMany(charArray => charArray)
                    .ToArray();

            //Randomize by Sorting on Completely Unique GUID values (that change with every request)
            //  effectively deriving the uniqueness from GUIDs!
            var charSet = charsArray
                .OrderBy(c => Guid.NewGuid())
                .Take(length);

            //Populate a Span for maximum performance and efficiency...
            //NOTE: Our CharSet has not yet been enumerated so we iterate over the resulting unique set only one time!
            int i = 0;
            Span<char> resultSpan = stackalloc char[length];
            foreach (var c in charSet) resultSpan[i++] = c;

            var tokenId = resultSpan.ToString();
            return tokenId;
        }
    }
}
