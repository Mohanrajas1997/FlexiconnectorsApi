using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.Extensions.Hosting;

namespace MysqlEfCoreDemo
{
    public class Program
    {
        public static void Main(string[] args)
        {
            CreateHostBuilder(args).Build().Run();
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureWebHostDefaults(webBuilder =>
                {
                    webBuilder.ConfigureKestrel(options =>
                    {
                        // Set the maximum request body size (in bytes)
                        options.Limits.MaxRequestBodySize = 104857600; // 100 MB
                    });
                    webBuilder.UseStartup<Startup>();
                });
    }

}