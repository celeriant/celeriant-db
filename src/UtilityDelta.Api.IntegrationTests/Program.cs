using System.Diagnostics;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using UtilityDelta.Api.Shared;

namespace UtilityDelta.Api.IntegrationTests
{
    internal class Program
    {
        private static string baseUrl = "http://localhost:5196";
        private static string endpoint = "/api/write";
        private static string endpointRead = "/api/read";


        static async Task Main(string[] args)
        {
            var assymetric = RSA.Create();
            var publicKeyPEM = assymetric.ExportRSAPublicKeyPem();
            var createdBy = MD5.HashData(Encoding.UTF8.GetBytes(publicKeyPEM));
            var createdByString = Encoding.UTF8.GetString(createdBy);

            var iteration = 0;
            var result = new List<long>();

            var tasks = new List<Task>();
            for (var j = 0; j < 2; j++)
            {
                for (var i = 0; i < 100; i++)
                {
                    var pi = "bbb6_" + i;
                    var task = Task.Run(async () =>
                    {
                        var client = new HttpClient();
                        while (true)
                        {
                            var writeTime = await addEvents(client, assymetric, publicKeyPEM, pi, createdBy);
                            result.Add(writeTime);
                            Console.WriteLine($"{iteration},{result.Average()}");
                            iteration++;
                        }
                    });
                    tasks.Add(task);
                }
            }

            Task.WaitAll(tasks.ToArray());
        }

        private static void NonceAndSignIt(RSA assymetric, out string nonce, out string nonceSigned)
        {
            nonce = DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString();
            nonceSigned = Convert.ToBase64String(assymetric.SignData(Encoding.UTF8.GetBytes(nonce), HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1));
        }

        private static async Task<long> addEvents(HttpClient client, RSA assymetric, string publicKeyPEM, string pi, byte[] createdBy)
        {
            var timer = new Stopwatch();
            timer.Start();

            NonceAndSignIt(assymetric, out var nonce, out var nonceSigned);

            var rand = new Random();
            var eventsList = new List<ProjectEventItem>();
            for (var i = 0; i < 2000; i++)
            {
                var iv = new byte[12];
                Array.Fill<byte>(iv, (byte)(rand.Next() * 255));
                var ivStr = Encoding.UTF8.GetString(iv);

                eventsList.Add(new ProjectEventItem(0, null, 0, ivStr, ProjectEventType.AddTask,
                    $"jasfkjl {rand.Next()}akjlasfd jlkasfdjklasfdjklafsd kaskjldfsajklajklfadsjklsdafjklafsdjklasdfjkl dkjaljs",
                    $"lkjsdjlkdf {rand.Next()}ljksdfjkldf jklafjklasdjklafdsjklajkladfjklfadsjklfdsajkldfsjklafdsjklasdf",
                    $"lkjsdjlkdf {rand.Next()}ljksdfjkldf saaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    i));
            }

            await SendViaWeb(client, pi, publicKeyPEM, nonce, nonceSigned, eventsList);

            timer.Stop();
            var elapsed = timer.ElapsedMilliseconds;
            return elapsed;
        }

        private static HashSet<string> _readFirst = new HashSet<string>();

        private static async Task SendViaWeb(HttpClient client, string pi, string publicKey, string nonce, string sign, List<ProjectEventItem> events)
        {
            var uriBuilder = new UriBuilder(baseUrl + endpoint);
            var query = System.Web.HttpUtility.ParseQueryString(uriBuilder.Query);
            query["pi"] = pi;
            query["publicKey"] = publicKey;
            query["nonce"] = nonce;
            query["sign"] = sign;
            query["fromTime"] = "0";
            query["createIfNotExist"] = "true";
            uriBuilder.Query = query.ToString();

            var doReadfirst = false;
            lock (_readFirst)
            {
                if (!_readFirst.Contains(pi))
                {
                    doReadfirst = true;
                    _readFirst.Add(pi);
                }
            }
            if (doReadfirst)
            {
                var uriBuilder2 = new UriBuilder(baseUrl + endpointRead);
                var query2 = System.Web.HttpUtility.ParseQueryString(uriBuilder2.Query);
                query2["pi"] = pi;
                query2["publicKey"] = publicKey;
                query2["nonce"] = nonce;
                query2["sign"] = sign;
                query2["fromTime"] = "0";
                query2["createIfNotExist"] = "true";
                uriBuilder2.Query = query2.ToString();

                HttpResponseMessage response = await client.GetAsync(uriBuilder2.Uri);
            }

            var json = JsonSerializer.Serialize(events);
            var content = new StringContent(json, Encoding.UTF8, "application/json");

            try
            {
                HttpResponseMessage response = await client.PostAsync(uriBuilder.Uri, content);
                response.EnsureSuccessStatusCode();
                string responseBody = await response.Content.ReadAsStringAsync();
                Console.WriteLine("Response received: " + responseBody);
            }
            catch (HttpRequestException e)
            {
                Console.WriteLine("Request error: " + e.Message);
            }
        }
    }

}