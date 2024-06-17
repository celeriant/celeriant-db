using Microsoft.Azure.Cosmos;
using System.Security.Cryptography.X509Certificates;
using UtilityDelta.WebAPI.Entities;

namespace UtilityDelta.WebAPI
{
    public static class Utils
    {
        public static byte[] ToByteArray(this string str)
        {
            byte[] byteArray = new byte[str.Length];
            for (int i = 0; i < str.Length; i++)
            {
                byteArray[i] = (byte)str[i];
            }
            return byteArray;
        }

        public static string CalculateHash(this string contents)
        {
            using System.Security.Cryptography.SHA256 SHA256 = System.Security.Cryptography.SHA256.Create();
            var str = Convert.ToBase64String(SHA256.ComputeHash(System.Text.Encoding.UTF8.GetBytes(contents)));
            return str.Replace('+','-').Replace('/', '_');
        }

        public static bool NotExpired(this AccessLinkItem accessLinkItem)
        {
            return accessLinkItem.validUntil == 0 || accessLinkItem.validUntil > DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        }

        public static void AddIndexIfNotExist(this ContainerResponse containerResponse, string path)
        {
            if (containerResponse.Resource.IndexingPolicy.IncludedPaths.Any(x => x.Path == path))
            {
                return;
            }
            containerResponse.Resource.IndexingPolicy.IncludedPaths.Add(new IncludedPath { Path = path });
        }
    }
}
