using OpenAI_API.Chat;
using OpenAI_API.Completions;
using OpenAI_API.Models;
using System.Net.Http;

namespace OpenAI_API
{
	/// <summary>
	/// Entry point to the OpenAPI API, handling auth and allowing access to the various API endpoints
	/// </summary>
	public class OpenAIAPI : IOpenAIAPI
	{
		/// <summary>
		/// Base url for OpenAI
		/// for OpenAI, should be "https://api.openai.com/{0}/{1}"
		/// for Azure, should be "https://(your-resource-name.openai.azure.com/openai/deployments/(deployment-id)/{1}?api-version={0}"
		/// </summary>
		public string ApiUrlFormat { get; set; } = "https://api.openai.com/{0}/{1}";

		/// <summary>
		/// Version of the Rest Api
		/// </summary>
		public string ApiVersion { get; set; } = "v1";

		/// <summary>
		/// The API authentication information to use for API calls
		/// </summary>
		public APIAuthentication Auth { get; set; }

		/// <summary>
		/// Optionally provide an IHttpClientFactory to create the client to send requests.
		/// </summary>
		public System.Net.Http.IHttpClientFactory HttpClientFactory { get; set; }

		/// <summary>
		/// Creates a new entry point to the OpenAPI API, handling auth and allowing access to the various API endpoints
		/// </summary>
		/// <param name="apiKeys">The API authentication information to use for API calls, or <see langword="null"/> to attempt to use the <see cref="APIAuthentication.Default"/>, potentially loading from environment vars or from a config file.</param>
		public OpenAIAPI(APIAuthentication apiKeys = null)
		{
			this.Auth = apiKeys.ThisOrDefault();
			Completions = new CompletionEndpoint(this);
			Models = new ModelsEndpoint(this);
			Chat = new ChatEndpoint(this);
		}

		/// <summary>
		/// Instantiates a version of the API for connecting to the Azure OpenAI endpoint instead of the main OpenAI endpoint.
		/// </summary>
		/// <param name="YourResourceName">The name of your Azure OpenAI Resource</param>
		/// <param name="deploymentId">The name of your model deployment. You're required to first deploy a model before you can make calls.</param>
		/// <param name="apiKey">The API authentication information to use for API calls, or <see langword="null"/> to attempt to use the <see cref="APIAuthentication.Default"/>, potentially loading from environment vars or from a config file.  Currently this library only supports the api-key flow, not the AD-Flow.</param>
		/// <returns></returns>
		public static OpenAIAPI ForAzure(string YourResourceName, string deploymentId, APIAuthentication apiKey = null)
		{
			OpenAIAPI api = new OpenAIAPI(apiKey);
			api.ApiVersion = "2023-05-15";
			api.ApiUrlFormat = $"https://{YourResourceName}.openai.azure.com/openai/deployments/{deploymentId}/" + "{1}?api-version={0}";
			return api;
		}

		/// <summary>
		/// Text generation is the core function of the API. You give the API a prompt, and it generates a completion. The way you “program” the API to do a task is by simply describing the task in plain english or providing a few written examples. This simple approach works for a wide range of use cases, including summarization, translation, grammar correction, question answering, chatbots, composing emails, and much more (see the prompt library for inspiration).
		/// </summary>
		public ICompletionEndpoint Completions { get; }


		/// <summary>
		/// Text generation in the form of chat messages. This interacts with the ChatGPT API.
		/// </summary>
		public IChatEndpoint Chat { get; }


		/// <summary>
		/// The API endpoint for querying available Engines/models
		/// </summary>
		public IModelsEndpoint Models { get; }

	}
}
