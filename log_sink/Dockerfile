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
COPY ["src/LogSink.Domain/LogSink.Domain.csproj", "src/LogSink.Domain/"]
COPY ["src/LogSink.Application/LogSink.Application.csproj", "src/LogSink.Application/"]
COPY ["src/LogSink.Infrastructure/LogSink.Infrastructure.csproj", "src/LogSink.Infrastructure/"]
COPY ["src/LogSink.Worker/LogSink.Worker.csproj", "src/LogSink.Worker/"]

RUN dotnet restore "src/LogSink.Worker/LogSink.Worker.csproj"

# Copiar código fuente y compilar binario ejecutable Native AOT
COPY src/ src/
WORKDIR "/src/src/LogSink.Worker"
RUN dotnet publish "LogSink.Worker.csproj" \
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

ENTRYPOINT ["./LogSink.Worker"]
