FROM microsoft/dotnet:2.1-aspnetcore-runtime
LABEL version="1.0" maintainer="Thiago"
WORKDIR /app
COPY . .
ENTRYPOINT ["dotnet", "IntegratorNet.dll"]