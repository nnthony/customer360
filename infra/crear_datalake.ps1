<#
.SYNOPSIS

.DESCRIPTION
    - Grupo de recursos con etiquetas (permite filtrar costos y borrar todo junto).
    - Cuenta StorageV2, Standard_LRS, hierarchical namespace habilitado (ADLS Gen2).
    - Seguridad: sin acceso público, solo HTTPS, TLS 1.2
    - Contenedor `customer360`, creado por el plano de control (ARM).
    - Rol "Storage Blob Data Contributor" para el usuario actual, SOLO sobre el
      contenedor (mínimo privilegio), no sobre toda la cuenta.

    Requisitos: `az login` hecho, y ser Owner (o User Access Administrator) de la suscripción.
    Se puede ejecutar más de una vez: las operaciones son idempotentes.

.EXAMPLE
    .\infra\crear_datalake.ps1 -Cuenta stc360acp2026
#>
param(
    [Parameter(Mandatory = $true)] [string] $Cuenta,
    [string] $GrupoRecursos = "rg-customer360-lab",
    [string] $Region        = "eastus",
    [string] $Contenedor    = "customer360"
)

function Verificar([string] $paso) {
    # Los comandos `az` no lanzan excepciones en PowerShell: hay que revisar el código de salida.
    if ($LASTEXITCODE -ne 0) { throw "Falló: $paso (código $LASTEXITCODE)" }
    Write-Host "OK  $paso" -ForegroundColor Green
}

$etiquetas = @("proyecto=customer360", "entorno=lab")

# 1. Grupo de recursos ----------------------------------------------------------
az group create --name $GrupoRecursos --location $Region --tags $etiquetas --output none
Verificar "Grupo de recursos $GrupoRecursos"

# 2. Cuenta de almacenamiento (ADLS Gen2) ------------------------------------------
az storage account create `
    --name $Cuenta `
    --resource-group $GrupoRecursos `
    --location $Region `
    --kind StorageV2 `
    --sku Standard_LRS `
    --access-tier Hot `
    --enable-hierarchical-namespace true `
    --allow-blob-public-access false `
    --https-only true `
    --min-tls-version TLS1_2 `
    --allow-shared-key-access false `
    --tags $etiquetas `
    --output none
Verificar "Cuenta de almacenamiento $Cuenta"

# 3. Contenedor ------------------------------
az storage container-rm create `
    --storage-account $Cuenta `
    --resource-group $GrupoRecursos `
    --name $Contenedor `
    --public-access off `
    --output none
Verificar "Contenedor $Contenedor"

# 4. Rol de DATOS para el usuario actual, con alcance al contenedor -------------------

$suscripcion = az account show --query id --output tsv
Verificar "Leer suscripción"
$usuario = az ad signed-in-user show --query id --output tsv
Verificar "Leer usuario actual"

$alcance = "/subscriptions/$suscripcion/resourceGroups/$GrupoRecursos" +
           "/providers/Microsoft.Storage/storageAccounts/$Cuenta" +
           "/blobServices/default/containers/$Contenedor"

az role assignment create `
    --assignee-object-id $usuario `
    --assignee-principal-type User `
    --role "Storage Blob Data Contributor" `
    --scope $alcance `
    --output none
Verificar "Rol Storage Blob Data Contributor sobre el contenedor"

Write-Host ""
Write-Host "Listo. La asignación de rol puede tardar unos minutos en hacer efecto." -ForegroundColor Yellow
