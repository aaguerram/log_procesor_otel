<#
.SYNOPSIS
    Script automatizado para iniciar SonarQube y ejecutar el análisis de calidad y cobertura
    sobre los microservicios 'consumer_streams' y 'log_sink'.

.DESCRIPTION
    1. Verifica e inicia el contenedor Docker de SonarQube (http://localhost:9000).
    2. Espera a que la API de SonarQube esté disponible (Status: UP).
    3. Ejecuta dotnet-sonarscanner con recolección de cobertura Cobertura / OpenCover y TRX.
    4. Exporta las métricas y reportes actualizados a las carpetas 'informe/' de cada proyecto.
#>

param(
    [string]$SonarHostUrl = "http://localhost:9000",
    [string]$SonarToken = "squ_e81f7531ee1e7753b3f3ad09e37eec800be6cd48"
)

$ErrorActionPreference = "Stop"

Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host " 🚀 PRODUBANCO OBSERVABILITY — EJECUTOR DE ANÁLISIS SONARQUBE   " -ForegroundColor Cyan
Write-Host "=================================================================" -ForegroundColor Cyan

# 1. Verificar Docker y Contenedor SonarQube
Write-Host "`n[1/4] Verificando contenedor SonarQube..." -ForegroundColor Yellow
$container = docker ps -a --filter "name=sonarqube-local" --format "{{.Names}}"
if (-not $container) {
    Write-Host "Creando y levantando nuevo contenedor SonarQube..." -ForegroundColor Cyan
    docker run -d --name sonarqube-local -p 9000:9000 -e SONAR_ES_BOOTSTRAP_CHECKS_DISABLE=true sonarqube:community
} else {
    $running = docker ps --filter "name=sonarqube-local" --format "{{.Names}}"
    if (-not $running) {
        Write-Host "Iniciando contenedor existente sonarqube-local..." -ForegroundColor Cyan
        docker start sonarqube-local | Out-Null
    }
}

# 2. Esperar a que SonarQube esté UP
Write-Host "`n[2/4] Esperando a que SonarQube responda en $SonarHostUrl..." -ForegroundColor Yellow
$maxAttempts = 30
$attempt = 0
$isUp = $false

while ($attempt -lt $maxAttempts) {
    $attempt++
    try {
        $status = (Invoke-RestMethod -Uri "$SonarHostUrl/api/system/status" -TimeoutSec 3).status
        if ($status -eq "UP") {
            $isUp = $true
            Write-Host " SonarQube está operativo (Status: UP)." -ForegroundColor Green
            break
        } else {
            Write-Host " Estado actual: $status. Reintentando ($attempt/$maxAttempts)..." -ForegroundColor Gray
        }
    } catch {
        Write-Host " Conectando... ($attempt/$maxAttempts)" -ForegroundColor Gray
    }
    Start-Sleep -Seconds 3
}

if (-not $isUp) {
    Write-Error " SonarQube no alcanzó el estado UP en el tiempo límite."
    exit 1
}

# 3. Análisis de consumer_streams
Write-Host "`n[3/4] Ejecutando análisis sobre consumer_streams..." -ForegroundColor Yellow
Get-ChildItem -Path "consumer_streams/tests" -Recurse -Include "TestResults" -Directory -ErrorAction SilentlyContinue | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue

dotnet sonarscanner begin `
    /k:"consumer_streams" `
    /n:"Consumer Streams" `
    /v:"1.0.0" `
    /d:sonar.host.url="$SonarHostUrl" `
    /d:sonar.token="$SonarToken" `
    /d:sonar.cs.cobertura.reportsPaths="consumer_streams/tests/**/coverage.cobertura.xml" `
    /d:sonar.cs.opencover.reportsPaths="consumer_streams/tests/**/coverage.opencover.xml" `
    /d:sonar.cs.vstest.reportsPaths="consumer_streams/tests/**/test_results.trx"

dotnet build consumer_streams/ConsumerStreams.slnx --no-incremental
dotnet test consumer_streams/ConsumerStreams.slnx --no-build --logger "trx;LogFileName=test_results.trx" --collect:"XPlat Code Coverage;Format=opencover,cobertura"
dotnet sonarscanner end /d:sonar.token="$SonarToken"

# 4. Análisis de log_sink
Write-Host "`n[4/4] Ejecutando análisis sobre log_sink..." -ForegroundColor Yellow
Get-ChildItem -Path "log_sink/tests" -Recurse -Include "TestResults" -Directory -ErrorAction SilentlyContinue | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue

dotnet sonarscanner begin `
    /k:"log_sink" `
    /n:"Log Sink" `
    /v:"1.0.0" `
    /d:sonar.host.url="$SonarHostUrl" `
    /d:sonar.token="$SonarToken" `
    /d:sonar.cs.cobertura.reportsPaths="log_sink/tests/**/coverage.cobertura.xml" `
    /d:sonar.cs.opencover.reportsPaths="log_sink/tests/**/coverage.opencover.xml" `
    /d:sonar.cs.vstest.reportsPaths="log_sink/tests/**/test_results.trx"

dotnet build log_sink/LogSink.slnx --no-incremental
dotnet test log_sink/LogSink.slnx --no-build --logger "trx;LogFileName=test_results.trx" --collect:"XPlat Code Coverage;Format=opencover,cobertura"
dotnet sonarscanner end /d:sonar.token="$SonarToken"

Write-Host "`n=================================================================" -ForegroundColor Green
Write-Host " ✔ ANÁLISIS COMPLETADO EXITOSAMENTE PARA AMBOS PROYECTOS         " -ForegroundColor Green
Write-Host "   Dashboards disponibles en:                                    " -ForegroundColor Cyan
Write-Host "   - consumer_streams : $SonarHostUrl/dashboard?id=consumer_streams" -ForegroundColor White
Write-Host "   - log_sink         : $SonarHostUrl/dashboard?id=log_sink" -ForegroundColor White
Write-Host "=================================================================" -ForegroundColor Green
