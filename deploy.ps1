param(
    [string]$Message = "deploy update"
)

$ErrorActionPreference = "Stop"

$Repo = "C:\Project"
$Remote = "origin"
$Branch = "main"
$Server = "root@176.124.222.183"
$KeyPath = Join-Path $env:USERPROFILE ".ssh\codex_kvm_ed25519"

Set-Location -LiteralPath $Repo

git add --all

@(git diff --cached --name-status) | ForEach-Object {
    $parts = $_ -split "`t", 2
    $status = $parts[0]
    $path = $parts[-1]
    $isSensitive =
        $path -match '(^|/)\.env$' -or
        $path -match '\.session(-journal)?$' -or
        $path -match '\.(sqlite3|sqlite3-.*|db|log|bak)$' -or
        $path -match '(^|/)reports/'
    if ($isSensitive -and $status -ne "D") {
        git reset -q HEAD -- $path 2>$null
    }
}

$stagedRows = @(git diff --cached --name-status)
$staged = @($stagedRows | ForEach-Object { ($_ -split "`t", 2)[-1] })
$blocked = @(
    $stagedRows | Where-Object {
        $parts = $_ -split "`t", 2
        $status = $parts[0]
        $path = $parts[-1]
        $isSensitive =
            $path -match '(^|/)\.env$' -or
            $path -match '\.session(-journal)?$' -or
            $path -match '\.(sqlite3|db|log)$' -or
            $path -match '(^|/)reports/'
        $isSensitive -and $status -ne "D"
    }
)

if ($blocked.Count -gt 0) {
    Write-Host "Blocked sensitive files from deploy:"
    $blocked | ForEach-Object { Write-Host " - $_" }
    exit 1
}

if ($staged.Count -gt 0) {
    git commit -m $Message
    git push $Remote "HEAD:$Branch"
} else {
    Write-Host "No safe code changes to commit."
}

ssh -i $KeyPath -o BatchMode=yes $Server "systemctl restart vol29app && systemctl is-active vol29app && tail -n 30 /root/vol29app/deploy/update_from_github.log"
