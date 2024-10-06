using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using UtilityDelta.AiTooling.Dtos;
using UtilityDelta.AiTooling.Interfaces;
using UtilityDelta.AiTooling.Services;
using UtilityDelta.Projects.Interfaces;
using UtilityDelta.Projects.Services;
using UtilityDelta.Projects.Shared;

namespace UtilityDelta.AiTooling
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var app = SetupApplication(args);

            var api = app.MapGroup("/api");

            var endpoints = app.Services.GetService<IEndpoints>()!;

            api.MapPost("/breakdown", endpoints.Breakdown);
            api.MapPost("/breakdownquestions", endpoints.BreakdownQuestions);
            api.MapPost("/unknowns", endpoints.Unknowns);
            api.MapPost("/roles", endpoints.Roles);
            api.MapPost("/assignroles", endpoints.AssignRoles);
            api.MapPost("/grouptasks", endpoints.GroupTasks);

            api.MapPost("/UploadFile", endpoints.UploadFile).DisableAntiforgery();
            api.MapPost("/DeleteFile", endpoints.DeleteFile);
            api.MapPost("/DeleteAllFiles", endpoints.DeleteAllFiles);

            api.MapPost("/assistant", async ([FromBody] DtoQuestion question, [FromQuery] string pi, [FromQuery] string publicKey, [FromQuery] string nonce, [FromQuery] string sign, HttpContext context, [FromServices] IUtilityDeltaAssistant utilityDeltaAssistant, [FromServices] IAccessLogic accessLogic, CancellationToken cancellationToken) =>
            {
                var accessInfo = accessLogic.IsProjectExistAndHasAccess(
                    projectId: pi,
                    createProjectIfNotExists: false,
                    shareKey: null,
                    publicKey: publicKey,
                    nonce: nonce,
                    sign: sign,
                    cancellationToken: cancellationToken);

                context.Response.ContentType = "text/plain";

                // Use the input text to generate a stream response
                await foreach (var item in utilityDeltaAssistant.AskAssistant(null, false, accessInfo.CurrentUserHash, question.question, cancellationToken))
                {
                    Console.WriteLine(item);
                    await context.Response.WriteAsync(item);
                    await context.Response.Body.FlushAsync(); // Flush the stream to ensure data is sent immediately
                }
            });

            api.MapGet("/ping", endpoints.Ping);
            api.MapGet("/laksfdksaefja", endpoints.PingResults);
            api.MapGet("/read", endpoints.Read);
            api.MapPost("/disableuser", endpoints.DisableUser);
            api.MapPost("/disableshare", endpoints.DisableShare);
            api.MapPost("/share", endpoints.Share);
            api.MapPost("/write", endpoints.Write);

            var udConfig = app.Services.GetService<IOptions<ConfigurationEntry>>()!;
            Directory.CreateDirectory(udConfig.Value.SUB_DIR_CONTAINERS);

            var writeAndBackup = app.Services.GetService<IWriteAndBackup>()!;
            _ = Task.Run(writeAndBackup.ProcessQueue);

            app.Run();
        }

        private static WebApplication SetupApplication(string[] args)
        {
            var builder = WebApplication.CreateBuilder(args);

            var isDevelopment = builder.Environment.IsDevelopment();

            builder.Services.AddCors(
                (options) => options.AddPolicy("CorsDevelopment",
                        builder =>
                        {
                            if (isDevelopment)
                            {
                                builder
                                    .WithOrigins("http://localhost:5173")
                                    .AllowAnyMethod()
                                    .AllowAnyHeader()
                                    .AllowCredentials();
                            }

                            builder
                                .WithOrigins("https://app.utilitydelta.io")
                                .AllowAnyMethod()
                                .AllowAnyHeader()
                                .AllowCredentials();

                            builder
                                .WithOrigins("https://test.utilitydelta.io")
                                .AllowAnyMethod()
                                .AllowAnyHeader()
                                .AllowCredentials();
                        }));

            builder.Services.AddSingleton<ICrypto, Crypto>();
            builder.Services.AddSingleton<IReadEvents, ReadEvents>();
            builder.Services.AddSingleton<IWriteEvents, WriteEvents>();
            builder.Services.AddSingleton<IWriteAndBackup, WriteAndBackup>();
            builder.Services.AddSingleton<IAccessLogic, AccessLogic>();
            builder.Services.AddSingleton<IShareKeyCache, ShareKeyCache>();
            builder.Services.AddSingleton<IAssistantCache, AssistantCache>();
            builder.Services.AddSingleton<IUserAccessCache, UserAccessCache>();
            builder.Services.AddSingleton<IFileHandlesManager, FileHandlesManager>();
            builder.Services.AddSingleton<ILlmProcessing, LlmProcessing>();
            builder.Services.AddSingleton<IEndpoints, Endpoints>();
            builder.Services.AddSingleton<IUtilityDeltaAssistant, UtilityDeltaAssistant>();
            builder.Services.AddSingleton<ISelectLLMProvider, SelectLLMProvider>();
            builder.Services.AddSingleton<IAssistantLlmProcessing, AssistantLlmProcessing>();
            builder.Services.AddSingleton<IOpenAiAssistantCommands, OpenAiAssistantCommands>();
            builder.Services.AddSingleton<IAssistantManager, AssistantManager>();

            builder.Services.Configure<FormOptions>(options =>
            {
                options.MultipartBodyLengthLimit = 104857600; // Example: Set limit to 100MB
            });

            var utilityDeltaConfiguration = builder.Configuration.GetSection("UtilityDelta");
            builder.Services.Configure<ConfigurationEntry>(utilityDeltaConfiguration);
            builder.Services.Configure<SystemSettings>(utilityDeltaConfiguration);

            var app = builder.Build();

            app.UseCors("CorsDevelopment");
            return app;
        }
    }
}
