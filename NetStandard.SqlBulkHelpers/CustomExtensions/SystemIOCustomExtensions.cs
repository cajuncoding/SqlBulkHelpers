using System;
using System.IO;
using System.Threading.Tasks;

namespace SqlBulkHelpers.CustomExtensions
{
    internal static class SystemIOCustomExtensions
    {
        public static byte[] ToByteArray(this Stream stream)
        {
            byte[] bytes = null;
            if (stream is MemoryStream existingMemoryStream)
            {
                //Memory stream is easy to work with and natively supports converting to ByteArray.
                bytes = existingMemoryStream.ToArray();
            }
            else
            {
                //For all other stream types we need to validate that we can Read Them!
                if (!stream.CanRead) throw new ArgumentException("Stream specified does not support Read operations.");

                using (var memoryStream = new MemoryStream())
                {
                    stream.CopyTo(memoryStream);
                    bytes = memoryStream.ToArray();
                }
            }

            return bytes;
        }

        public static async Task<byte[]> ToByteArrayAsync(this Stream stream)
        {
            byte[] bytes;
            if (stream is MemoryStream existingMemoryStream)
            {
                //Memory stream is easy to work with and natively supports converting to ByteArray.
                bytes = existingMemoryStream.ToArray();
            }
            else
            {
                //For all other stream types we need to validate that we can Read Them!
                if (!stream.CanRead) throw new ArgumentException("Stream specified does not support Read operations.");

                using (var memoryStream = new MemoryStream())
                {
                    await stream.CopyToAsync(memoryStream);
                    bytes = memoryStream.ToArray();
                }
            }

            return bytes;
        }
    }
}
