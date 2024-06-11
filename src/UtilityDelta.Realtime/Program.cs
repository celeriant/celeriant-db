using UtilityDelta.Realtime;

internal class Program
{
    private static void Main(string[] args)
    {
        var app = SetupApplication(args);
        app.Run();
    }

    private static WebApplication SetupApplication(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);

        builder.Services.AddCors(
            (options) => options.AddPolicy("CorsDevelopment",
                    builder =>
                    {
                        builder
                        .WithOrigins("http://localhost:5173")
                        .AllowAnyMethod()
                        .AllowAnyHeader()
                        .AllowCredentials();

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

        var buildSignalR = builder.Services.AddSignalR();

        var app = builder.Build();

        app.UseCors("CorsDevelopment");
        app.MapHub<UtilityDeltaHub>("/realtime");

        return app;
    }
}
