using OpenAI_API.Chat;
using OpenAI_API.Completions;
using OpenAI_API.Models;

namespace OpenAI_API
{
	/// <summary>
	/// An interface for <see cref="OpenAIAPI"/>, for ease of mock testing, etc
	/// </summary>
	public interface IOpenAIAPI
    {
        /// <summary>
        /// Base url for OpenAI
        /// for OpenAI, should be "https://api.openai.com/{0}/{1}"
        /// for Azure, should be "https://(your-resource-name.openai.azure.com/openai/deployments/(deployment-id)/{1}?api-version={0}"
        /// </summary>
        string ApiUrlFormat { get; set; }

        /// <summary>
        /// Version of the Rest Api
        /// </summary>
        string ApiVersion { get; set; }

        /// <summary>
        /// The API authentication information to use for API calls
        /// </summary>
        APIAuthentication Auth { get; set; }

        /// <summary>
        /// Text generation in the form of chat messages. This interacts with the ChatGPT API.
        /// </summary>
        IChatEndpoint Chat { get; }

        /// <summary>
        /// Text generation is the core function of the API. You give the API a prompt, and it generates a completion. The way you “program” the API to do a task is by simply describing the task in plain english or providing a few written examples. This simple approach works for a wide range of use cases, including summarization, translation, grammar correction, question answering, chatbots, composing emails, and much more (see the prompt library for inspiration).
        /// </summary>
        ICompletionEndpoint Completions { get; }

        /// <summary>
        /// The API endpoint for querying available Engines/models
        /// </summary>
        IModelsEndpoint Models { get; }

	}
}