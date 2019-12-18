using IntegratorCore.Cmd.Domain.Entities;
using IntegratorCore.Cmd.Domain.Repository;
using IntegratorCore.Cmd.Infrastructure.IoC;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace IntegratorCore.Console
{
    class Program
    {
        static void Main(string[] args)
        {
            //Setup Dependency Injection
            var services = new ServiceCollection()
                .AddLogging()
                .AddRepositoryDepedencyInjection()
                .BuildServiceProvider();
            
            //Configure Logger
            services.GetService<ILoggerFactory>()
                .AddConsole(LogLevel.Error);
            
            var logger = services.GetService<ILoggerFactory>()
                .CreateLogger<Program>();

            logger.LogDebug("Starting Application");

            //Process
            var sibelCategoriaAbrasceRepository = services.GetService<IGenerateData<SibelCategoriaAbrasceOracle>>();
            var sibelClienteRepository = services.GetService<IGenerateData<SibelClienteOracle>>();
            var sibelGrpEconomico = services.GetService<IGenerateData<SibelGrpEconomicoOracle>>();
            var sibelMarcas = services.GetService<IGenerateData<SibelMarcasOracle>>();
            var sibelNegociacao = services.GetService<IGenerateData<SibelNegociacaoOracle>>();
            var sibelPropriedade = services.GetService<IGenerateData<SibelPropriedadeOracle>>();
            var sibelSegmentoAbrasce = services.GetService<IGenerateData<SibelSegmtoAbrasceOracle>>();

            // Execute functions
            sibelCategoriaAbrasceRepository.GetResult();
            sibelClienteRepository.GetResult();
            sibelGrpEconomico.GetResult();
            sibelMarcas.GetResult();
            sibelNegociacao.GetResult();
            sibelPropriedade.GetResult();
            sibelSegmentoAbrasce.GetResult();
        }
    }
}
