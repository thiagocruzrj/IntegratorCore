using IntegratorCore.Cmd.Domain.Entities;
using IntegratorCore.Cmd.Domain.Repository;
using IntegratorCore.Cmd.Infrastructure.Repository;
using IntegratorNet.Infrastructure.Repository;
using Microsoft.Extensions.DependencyInjection;

namespace IntegratorCore.Cmd.Infrastructure.IoC
{
    public static class MyDependencyInjection
    {
        public static IServiceCollection AddRepositoryDepedencyInjection(this IServiceCollection services)
        {
            services.AddSingleton<IGenerateData<SibelCategoriaAbrasceOracle>, SibelCategoriaAbrasceImpl>();
            services.AddSingleton<IGenerateData<SibelClienteOracle>, SibelClienteImpl>();
            services.AddSingleton<IGenerateData<SibelGrpEconomicoOracle>, SibelGrpEconomicoImpl>();
            services.AddSingleton<IGenerateData<SibelMarcasOracle>, SibelMarcasImpl>();
            services.AddSingleton<IGenerateData<SibelNegociacaoOracle>, SibelNegociacaoImpl>();
            services.AddSingleton<IGenerateData<SibelPropriedadeOracle>, SibelPropriedadeImpl>();
            services.AddSingleton<IGenerateData<SibelSegmtoAbrasceOracle>, SibelSegmtoAbrasceImpl>();

            return services;
        }
    }
}