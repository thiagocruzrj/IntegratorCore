# step 1 - building you app with SDK as base image
FROM microsoft/dotnet:2.1-sdk AS build-env 
WORKDIR /build 

COPY . . 

RUN ls -al #

RUN cd /

RUN cd /. && ls -al

FROM microsoft/dotnet:2.1-aspnetcore-runtime AS runtime
WORKDIR /app
COPY ./publish .

RUN ls -al 

ENTRYPOINT ["dotnet", "IntegratorNet.Cmd.dll"]