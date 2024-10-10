using System;
using System.Text;
using System.Threading.Tasks;

public class BufferHelper
{
    // Converts ArrayBuffer (byte[]) to a base64 encoded string
    public static string Ab2Str(byte[] buf)
    {
        string str1 = Ab2StrNotBase64(buf);
        return Convert.ToBase64String(Encoding.GetEncoding("ISO-8859-1").GetBytes(str1));
    }

    public static string Ab2StrNotBase64(byte[] buffer)
    {
        // Create a string from the byte array
        char[] chars = new char[buffer.Length];

        // Convert each byte back to its character representation
        for (int i = 0; i < buffer.Length; i++)
        {
            chars[i] = (char)buffer[i];
        }

        return new string(chars);
    }

    public static byte[] Str2Ab(string str)
    {
        // Decode the Base64 string
        byte[] decodedBytes = Convert.FromBase64String(str);

        // Convert the decoded bytes to a string
        string decodedStr = Encoding.GetEncoding("ISO-8859-1").GetString(decodedBytes);

        // Use the Str2AbNotBase64 method to convert the decoded string to byte array
        return Str2AbNotBase64(decodedStr);
    }

    public static byte[] Str2AbNotBase64(string str)
    {
        // Create a byte array with the same length as the input string
        byte[] buffer = new byte[str.Length];

        // Convert each character to its byte representation
        for (int i = 0; i < str.Length; i++)
        {
            buffer[i] = (byte)str[i];
        }

        return buffer;
    }

    // Prepares the buffer by checking the type of data
    public static async Task<byte[]> PrepareBufferAsync(object data)
    {
        if (data is string strData)
        {
            return Str2AbNotBase64(strData);
        }
        else if (data is byte[] byteArrayData)
        {
            return byteArrayData;
        }
        else if (data is System.IO.MemoryStream memoryStream)
        {
            return memoryStream.ToArray();
        }
        else if (data is System.IO.Stream streamData)
        {
            return await StreamToArrayBufferAsync(streamData);
        }
        throw new ArgumentException("Unsupported data type.");
    }

    // Converts a stream (similar to Blob) to ArrayBuffer (byte[])
    private static async Task<byte[]> StreamToArrayBufferAsync(System.IO.Stream stream)
    {
        using (var memoryStream = new System.IO.MemoryStream())
        {
            await stream.CopyToAsync(memoryStream);
            return memoryStream.ToArray();
        }
    }
}
