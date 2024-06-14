using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;

namespace UtilityDelta.Api.Shared
{
    public static class Utility
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long ToUnixTimeSeconds(this DateTime input)
        {
            return new DateTimeOffset(input, TimeSpan.Zero).ToUnixTimeSeconds();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static DateTime FromUnixTimeSeconds(this long input)
        {
            return DateTimeOffset.FromUnixTimeSeconds(input).DateTime;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IncreasesAccessLevel(this AccessLevel currentAccessLevel, AccessLevel? potentialAccessLevel) 
            => potentialAccessLevel.HasValue ? 
                ((int)currentAccessLevel) > ((int)potentialAccessLevel.Value) : 
                false;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IncreasesAccessLevel(this AccessLevel? currentAccessLevel, AccessLevel? potentialAccessLevel) 
            => currentAccessLevel.HasValue && potentialAccessLevel.HasValue ? 
                ((int)currentAccessLevel.Value) > ((int)potentialAccessLevel.Value) : 
                potentialAccessLevel.HasValue;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsServerEvent(this ProjectEventType projectEventType) 
            => projectEventType switch
            {
                ProjectEventType.AddShareLink or 
                ProjectEventType.AddSingleUseShareLink or 
                ProjectEventType.ProvideAccess or 
                ProjectEventType.DisableShareLink => true,
                _ => false,
            };

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ProjectAccess ToProjectAccess(this AccessLevel accessLevel) 
            => accessLevel switch
            {
                AccessLevel.Owner => ProjectAccess.OwnerAccess,
                AccessLevel.Contributor => ProjectAccess.WriteAccess,
                AccessLevel.Viewer => ProjectAccess.ReadOnlyAccess,
                _ => throw new NotSupportedException(),
            };

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteNullable(this BinaryWriter binaryWriter, double? input)
        {
            if (input == null)
            {
                binaryWriter.Write(false);
                return;
            }

            binaryWriter.Write(true);
            binaryWriter.Write(input.Value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteNullable(this BinaryWriter binaryWriter, string? input)
        {
            if (input == null)
            {
                binaryWriter.Write(false);
                return;
            }

            binaryWriter.Write(true);
            binaryWriter.Write(input);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static string? ReadStringNullable(this BinaryReader reader) 
            => reader.ReadBoolean() ? reader.ReadString() : null;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static double? ReadDoubleNullable(this BinaryReader reader) 
            => reader.ReadBoolean() ? reader.ReadDouble() : null;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static string CalculateHash(this string contents)
        {
            using SHA256 SHA256 = System.Security.Cryptography.SHA256.Create();
            var str = Convert.ToBase64String(SHA256.ComputeHash(System.Text.Encoding.UTF8.GetBytes(contents)));
            return str.Replace('+', '-').Replace('/', '_');
        }

        //TODO: Why do this? Can't use built in functions?
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte[] ToByteArray(this string str)
        {
            byte[] byteArray = new byte[str.Length];
            for (int i = 0; i < str.Length; i++)
            {
                byteArray[i] = (byte)str[i];
            }
            return byteArray;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static string ContainerPath(this string container, string folder) 
            => Path.Combine(folder, container);
    }
}
