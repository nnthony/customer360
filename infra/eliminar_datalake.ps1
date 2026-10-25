<#
.SYNOPSIS
    Elimina TODO el laboratorio (grupo de recursos completo). Irreversible.

.EXAMPLE
    .\infra\eliminar_datalake.ps1
#>
param([string] $GrupoRecursos = "rg-customer360-lab")

Write-Host "Se eliminará el grupo '$GrupoRecursos' con TODOS sus recursos y datos." -ForegroundColor Red
$confirmacion = Read-Host "Escribe el nombre del grupo para confirmar"
if ($confirmacion -ne $GrupoRecursos) {
    Write-Host "Cancelado." ; exit 1
}

az group delete --name $GrupoRecursos --yes
if ($LASTEXITCODE -ne 0) { throw "No se pudo eliminar $GrupoRecursos" }
Write-Host "Eliminado. Verifica en Cost Management que no queden cargos." -ForegroundColor Green
