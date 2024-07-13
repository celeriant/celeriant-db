using OpenAI_API.Chat;
using OpenAI_API.Completions;
using OpenAI_API.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using static OpenAI_API.EndpointBase;

namespace UtilityDelta.ChatGpt
{
    [JsonSerializable(typeof(ChatRequest))]
    [JsonSerializable(typeof(CompletionRequest))]
    [JsonSerializable(typeof(ApiErrorResponse))]
    [JsonSerializable(typeof(Model))]
    [JsonSerializable(typeof(ChatResult))]
    [JsonSourceGenerationOptions(DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
    public partial class SourceGenerationContext : JsonSerializerContext
    {
    }
}
