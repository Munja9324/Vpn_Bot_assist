param(
    [string]$Message = "deploy update"
)

$ErrorActionPreference = "Stop"

$Repo = "C:\Project"
$Remote = "origin"
$Branch = "main"
$Server = "root@176.124.222.183"
$KeyPath = Join-Path $env:USERPROFILE ".ssh\codex_kvm_ed25519"
$PrecheckLog = Join-Path $Repo "deploy-precheck.log"

Set-Location -LiteralPath $Repo

function Resolve-PythonExe {
    $candidates = @(
        "C:\Users\VOL29\AppData\Local\Programs\Python\Python314\python.exe",
        "C:\Users\VOL29\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe"
    )
    foreach ($candidate in $candidates) {
        if (Test-Path -LiteralPath $candidate) {
            return $candidate
        }
    }
    throw "Python executable not found."
}

function Run-PrecheckStep {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][scriptblock]$Script,
        [Parameter(Mandatory = $true)][string]$CommandLabel
    )

    Add-Content -LiteralPath $PrecheckLog -Value "[$((Get-Date).ToString('yyyy-MM-dd HH:mm:ss'))] STEP: $Name"
    Add-Content -LiteralPath $PrecheckLog -Value "CMD: $CommandLabel"

    & $Script *>> $PrecheckLog
    if ($LASTEXITCODE -ne 0) {
        throw "Predeploy check failed on step: $Name. See $PrecheckLog"
    }
}

if (Test-Path -LiteralPath $PrecheckLog) {
    Remove-Item -LiteralPath $PrecheckLog -Force
}
"Predeploy checks started at $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" | Set-Content -LiteralPath $PrecheckLog -Encoding UTF8

$Python = Resolve-PythonExe

Run-PrecheckStep -Name "Compile package" -CommandLabel "$Python -c <py_compile>" -Script {
    & $Python -c "import pathlib,py_compile; [py_compile.compile(str(p), doraise=True) for p in pathlib.Path('kbrbot').rglob('*.py')]; py_compile.compile('vpn_kbr.py', doraise=True)"
}
Run-PrecheckStep -Name "Pytest smoke" -CommandLabel "$Python -m pytest -q" -Script {
    & $Python -m pytest -q
}
Run-PrecheckStep -Name "Mojibake guard" -CommandLabel "$Python tools/check_mojibake.py" -Script {
    & $Python tools/check_mojibake.py
}

function Test-SensitivePath {
    param([string]$Path)

    $normalized = ($Path -replace '\\', '/')
    if ($normalized -eq ".env.example") {
        return $false
    }

    return (
        $normalized -match '(^|/)\.env($|[._-].*)' -or
        $normalized -match '\.session(-journal)?$' -or
        $normalized -match '\.(sqlite3|sqlite3-.*|db|log|bak)$' -or
        $normalized -match '(^|/)reports/' -or
        $normalized -match '(^|/)(id_rsa|id_ed25519|.+\.pem|.+\.key)$'
    )
}

git add --all

@(git diff --cached --name-status) | ForEach-Object {
    $parts = $_ -split "`t", 2
    $status = $parts[0]
    $path = $parts[-1]
    if ((Test-SensitivePath $path) -and $status -ne "D") {
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
        (Test-SensitivePath $path) -and $status -ne "D"
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
