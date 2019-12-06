﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using IntegratorNet.Domain.Repository;
using IntegratorNet.Infrastructure.Repository;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.HttpsPolicy;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Oracle.ManagedDataAccess.Client;

namespace IntegratorNet
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddSingleton<IConfiguration>(Configuration);
            services.AddTransient<ISibelCategoriaAbrasce, SibelCategoriaAbrasceImpl>();
            services.AddTransient<ISibelCliente, SibelClienteImpl>();
            services.AddTransient<ISibelGrpEconomico, SibelGrpEconomicoImpl>();
            services.AddTransient<ISibelMarcas, SibelMarcasImpl>();
            services.AddTransient<ISibelNegociacao, SibelNegociacaoImpl>();
            services.AddTransient<ISibelPropriedade, SibelPropriedadeImpl>();
            services.AddTransient<ISibelSegmtoAbrasce, SibelSegmtoAbrasceImpl>();
            services.AddMvc().SetCompatibilityVersion(CompatibilityVersion.Version_2_1);
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IHostingEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }
            else
            {
                app.UseHsts();
            }

            app.UseHttpsRedirection();
            app.UseMvc();
        }
    }
}
