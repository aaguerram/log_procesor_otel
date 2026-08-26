# ==========================================
# Etapa 1: Build y Publicación (.NET 10 SDK)
# ==========================================
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Copiar archivos de proyecto para restauración de dependencias
COPY ["src/KafkaDemo.Domain/KafkaDemo.Domain.csproj", "src/KafkaDemo.Domain/"]
COPY ["src/KafkaDemo.Application/KafkaDemo.Application.csproj", "src/KafkaDemo.Application/"]
COPY ["src/KafkaDemo.Infrastructure/KafkaDemo.Infrastructure.csproj", "src/KafkaDemo.Infrastructure/"]
COPY ["src/KafkaDemo.ConsoleApp/KafkaDemo.ConsoleApp.csproj", "src/KafkaDemo.ConsoleApp/"]

RUN dotnet restore "src/KafkaDemo.ConsoleApp/KafkaDemo.ConsoleApp.csproj"

# Copiar todo el código fuente y compilar la aplicación de consola
COPY src/ src/
WORKDIR "/src/src/KafkaDemo.ConsoleApp"
RUN dotnet publish "KafkaDemo.ConsoleApp.csproj" -c Release -o /app/publish /p:UseAppHost=false

# ==========================================
# Etapa 2: Runtime (.NET 10 Runtime)
# ==========================================
FROM mcr.microsoft.com/dotnet/runtime:10.0 AS runtime
WORKDIR /app

COPY --from=build /app/publish .

# Variables de entorno por defecto (sobrescribibles por docker-compose)
ENV DOTNET_ENVIRONMENT=Production \
    Kafka__BootstrapServers=kafka:29092 \
    Kafka__ClientId=KafkaDemo-ConsoleDocker

ENTRYPOINT ["dotnet", "KafkaDemo.ConsoleApp.dll"]
