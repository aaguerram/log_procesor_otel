namespace ConsumerStreams.Domain.Tests.TestSupport;

/// <summary>Contratos OpenAPI mínimos con la indentación que espera el compilador de un solo pase.</summary>
public static class SampleContracts
{
    public const string TransferManagement = """
openapi: 3.0.0
info:
  title: Transfer.Mspx.Prometeus.Management
  version: 2.5.0
paths:
  /contacts/by-id/{idClient}/{channel}:
    get:
      operationId: GetContact
      parameters:
        - name: idClient
          in: path
          required: true
          schema:
            type: integer
            x-log-data-protection: '@Log.Hash(SHA256)'
        - name: channel
          in: path
          required: true
          schema:
            type: string
        - name: numCuenta
          in: query
          schema:
            type: string
            x-log-data-protection: '@Log.Partial(LAST_4)'
      responses:
        '200':
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ContactResult'
  /contacts/local:
    post:
      operationId: InsertContact
      requestBody:
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/ContactRequest'
      responses:
        '200':
          description: OK
components:
  schemas:
    ContactRequest:
      type: object
      properties:
        identificacion:
          type: string
          x-log-data-protection: '@Log.Partial(LAST_4)'
        nombreCliente:
          type: string
          x-log-data-protection: '@Log.Full'
        clientePrincipal:
          type: object
          properties:
            idCliente:
              type: integer
              x-log-data-protection: '@Log.Hash(SHA256)'
        objJsonResponse:
          type: string
          x-log-data-protection: '@Log.Remove'
    ContactResult:
      type: object
      properties:
        saldo:
          type: number
          x-log-data-protection: '@Log.Full'
""";
}
