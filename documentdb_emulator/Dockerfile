FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY ["DocumentDbEmulator.csproj", "./"]
RUN dotnet restore "DocumentDbEmulator.csproj"
COPY . .
RUN dotnet publish "DocumentDbEmulator.csproj" -c Release -o /app/publish

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS final
WORKDIR /app
COPY --from=build /app/publish .
EXPOSE 8081
ENV ASPNETCORE_URLS=http://+:8081
ENTRYPOINT ["dotnet", "DocumentDbEmulator.dll"]
