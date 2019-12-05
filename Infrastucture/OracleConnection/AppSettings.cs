using System.IO;
using Microsoft.Extensions.Configuration;

namespace IntegratorNetCore.Infrastucture.OracleConnection
{
    public class AppSettings
    {

        static AppSettings()
        {
            Configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json")
                .Build();
        }

        private static IConfiguration Configuration { get; set; }


        public static string GetConnectionString(string name)
        {
            return Configuration.GetConnectionString(name);
        }

        public static IConfigurationSection GetSection(string key)
        {
            return Configuration.GetSection(key);
        }

        public static T Get<T>()
        {
            return Configuration.Get<T>();
        }

        public static T GetValue<T>(string key)
        {
            return Configuration.GetValue<T>(key);
        }

        public static T GetValue<T>(string key, T defaultValue)
        {
            return (T)Configuration.GetValue(typeof(T), key, defaultValue);
        }
    }
}