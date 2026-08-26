# ==============================================================================
# STAGE 1: Build & Native AOT Compilation (.NET 10 SDK + Clang toolchain)
# ==============================================================================
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Instalar dependencias nativas para el compilador Native AOT en Linux
RUN apt-get update && \
    apt-get install -y --no-install-recommends clang zlib1g-dev libssl-dev && \
    rm -rf /var/lib/apt/lists/*

# Copiar csproj y restaurar dependencias
COPY ["src/ConsumerStreams.Domain/ConsumerStreams.Domain.csproj", "src/ConsumerStreams.Domain/"]
COPY ["src/ConsumerStreams.Application/ConsumerStreams.Application.csproj", "src/ConsumerStreams.Application/"]
COPY ["src/ConsumerStreams.Infrastructure/ConsumerStreams.Infrastructure.csproj", "src/ConsumerStreams.Infrastructure/"]
COPY ["src/ConsumerStreams.Worker/ConsumerStreams.Worker.csproj", "src/ConsumerStreams.Worker/"]

RUN dotnet restore "src/ConsumerStreams.Worker/ConsumerStreams.Worker.csproj"

# Copiar código fuente y compilar binario ejecutable Native AOT
COPY src/ src/
WORKDIR "/src/src/ConsumerStreams.Worker"
RUN dotnet publish "ConsumerStreams.Worker.csproj" \
    -c Release \
    -r linux-x64 \
    -o /app/publish \
    /p:PublishAot=true \
    /p:StripSymbols=true

# ==============================================================================
# STAGE 2: Ultra-lightweight Native Runtime Container (.NET 10 runtime-deps)
# ==============================================================================
FROM mcr.microsoft.com/dotnet/runtime-deps:10.0 AS runtime
WORKDIR /app

# Copiar binario nativo ejecutable generado por Native AOT
COPY --from=build /app/publish .

ENV DOTNET_EnableDiagnostics=0

ENTRYPOINT ["./ConsumerStreams.Worker"]
