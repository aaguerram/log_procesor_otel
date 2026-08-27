namespace ConsumerStreams.Domain.Tests.TestSupport;

/// <summary>Contrato para las pruebas del enmascarador: cubre parámetros de ruta/consulta y esquema de cuerpo.</summary>
public static class MaskingContract
{
    public const string Yaml = """
openapi: 3.0.0
info:
  title: Masking.Test
  version: 1.0.0
paths:
  /accounts/{numCuenta}/detail:
    get:
      operationId: GetAccountDetail
      parameters:
        - name: numCuenta
          in: path
          required: true
          schema:
            type: string
            x-log-data-protection: '@Log.Partial(LAST_4)'
        - name: token
          in: query
          schema:
            type: string
            x-log-data-protection: '@Log.Hash(SHA256)'
      responses:
        '200':
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Acc'
components:
  schemas:
    Acc:
      type: object
      properties:
        identificacion:
          type: string
          x-log-data-protection: '@Log.Hash(SHA256)'
        cuenta:
          type: string
          x-log-data-protection: '@Log.Partial(LAST_4)'
        objJsonResponse:
          type: string
          x-log-data-protection: '@Log.Remove'
        nombreCliente:
          type: string
          x-log-data-protection: '@Log.Full'
""";
}
