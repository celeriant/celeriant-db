using UtilityDelta.AiTooling.Interfaces;
using UtilityDelta.AiTooling.Services;
using UtilityDelta.Projects.Interfaces;
using UtilityDelta.Projects.Services;

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
            api.MapPost("/unknowns", endpoints.Unknowns);
            api.MapPost("/roles", endpoints.Roles);
            api.MapPost("/assignroles", endpoints.AssignRoles);
            api.MapPost("/grouptasks", endpoints.GroupTasks);

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
            builder.Services.AddSingleton<IUserAccessCache, UserAccessCache>();
            builder.Services.AddSingleton<IFileHandlesManager, FileHandlesManager>();
            builder.Services.AddSingleton<ILlmProcessing, LlmProcessing>();
            builder.Services.AddSingleton<IEndpoints, Endpoints>();

            var utilityDeltaConfiguration = builder.Configuration.GetSection("UtilityDelta");
            builder.Services.Configure<ConfigurationEntry>(utilityDeltaConfiguration);

            var app = builder.Build();
            app.UseCors("CorsDevelopment");

            return app;
        }
    }
}
