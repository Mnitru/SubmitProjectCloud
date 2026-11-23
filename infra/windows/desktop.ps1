<#
  Desktop-side orchestration (Windows 11 Pro + Hyper-V + Multipass).

  Usage (from the project folder, normal or admin PowerShell):
    powershell -ExecutionPolicy Bypass -File infra\windows\desktop.ps1 <command>

  Commands:
    up        create master/worker1/worker2, set up SSH + WireGuard,
              publish worker3's config for the laptop
    install   (after worker3 joined) install Hadoop + Spark + monitoring,
              start everything, run the health check
    start     start the desktop VMs after a reboot and start all services
    stop      stop all services and the desktop VMs
    status    list VMs and run the health check
    push      copy the project code to master (~/study)
    pull      copy ~/study/results from master into .\results
    shell     open a shell on master
    destroy   delete the desktop VMs (add -Force to skip the question)
#>
param(
  [Parameter(Position = 0)][string]$Command = "help",
  [switch]$Force
)

$ErrorActionPreference = "Stop"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$env:Path = [Environment]::GetEnvironmentVariable("Path", "Machine") + ";" + [Environment]::GetEnvironmentVariable("Path", "User")

# ---------------------------------------------------------------- helpers
function Write-Step([string]$Msg) { Write-Host ("[{0}] {1}" -f (Get-Date -Format HH:mm:ss), $Msg) -ForegroundColor Cyan }

function Read-ClusterConf {
  $conf = @{}
  foreach ($line in Get-Content (Join-Path $Root "cluster.conf")) {
    $l = $line.Trim()
    if ($l -eq "" -or $l.StartsWith("#")) { continue }
    $i = $l.IndexOf("=")
    if ($i -lt 1) { continue }
    $k = $l.Substring(0, $i).Trim()
    $v = $l.Substring($i + 1).Trim()
    if ($v.Length -ge 2 -and $v.StartsWith('"') -and $v.EndsWith('"')) { $v = $v.Substring(1, $v.Length - 2) }
    $conf[$k] = $v
  }
  return $conf
}

$C = Read-ClusterConf
$DesktopNodes = @($C["DESKTOP_NODES"] -split '\s+' | Where-Object { $_ })
$AllNodes = @($C["NODES"] -split '\s+' | Where-Object { $_ })
$MasterLan = $C["LAN_IP_master"]

function Invoke-Mp {
  & multipass @args
  if ($LASTEXITCODE -ne 0) { throw ("multipass {0} failed (exit {1})" -f ($args -join ' '), $LASTEXITCODE) }
}

# Run a bash script inside a VM. The script travels base64-encoded so that
# PowerShell 5.1 cannot mangle quotes, and runs from a temp file with stdin
# closed so that ssh calls inside it cannot swallow the rest of the script.
function Invoke-VmBash([string]$Node, [string]$Script) {
  $clean = $Script.Replace("`r", "")
  $b64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($clean))
  $cmd = 'f=$(mktemp); echo ' + $b64 + ' | base64 -d > $f; bash $f < /dev/null; rc=$?; rm -f $f; exit $rc'
  & multipass exec $Node -- bash -c $cmd
  if ($LASTEXITCODE -ne 0) { throw ("command on {0} failed (exit {1})" -f $Node, $LASTEXITCODE) }
}

function Get-Instances {
  $json = (& multipass list --format json) | Out-String
  $h = @{}
  foreach ($i in ($json | ConvertFrom-Json).list) { $h[$i.name] = $i }
  return $h
}

# ---------------------------------------------------------------- VM creation
function New-CloudInit([string]$Node) {
  $tpl = Get-Content -Raw (Join-Path $Root "infra\cloud-init\node.yaml.tpl")
  $hostLines = ($AllNodes | ForEach-Object { "      {0} {1}" -f $C["WG_IP_$_"], $_ }) -join "`n"
  $mac = $C["LAN_MAC_$Node"]
  $ip = $C["LAN_IP_$Node"]
  $prefix = $C["LAN_PREFIX"]
  $files = @(
    "  # static address on the bridged home LAN (WireGuard endpoint)",
    "  - path: /etc/netplan/60-cluster-lan.yaml",
    "    permissions: '0600'",
    "    content: |",
    "      network:",
    "        version: 2",
    "        ethernets:",
    "          lan0:",
    "            match:",
    "              macaddress: `"$mac`"",
    "            dhcp4: false",
    "            addresses: [$ip/$prefix]"
  ) -join "`n"
  $runcmd = "  - netplan apply"
  $out = $tpl.Replace("`r`n", "`n").Replace("__HOSTS__", $hostLines).Replace("__EXTRA_FILES__", $files).Replace("__EXTRA_RUNCMD__", $runcmd)
  $path = Join-Path $env:TEMP "cloud-init-$Node.yaml"
  [IO.File]::WriteAllText($path, $out, (New-Object Text.UTF8Encoding($false)))
  return $path
}

function New-DesktopVm([string]$Node) {
  if ($Node -eq "master") { $cpus = $C["MASTER_CPUS"]; $mem = $C["MASTER_MEM"]; $disk = $C["MASTER_DISK"] }
  else { $cpus = $C["WORKER_CPUS"]; $mem = $C["WORKER_MEM"]; $disk = $C["WORKER_DISK"] }
  $ci = New-CloudInit $Node
  $net = "name={0},mode=manual,mac={1}" -f $C["DESKTOP_BRIDGE"], $C["LAN_MAC_$Node"]
  Write-Step "Launching $Node ($cpus vCPU, $mem RAM, $disk disk, LAN $($C["LAN_IP_$Node"])) - takes a few minutes"
  Invoke-Mp launch $C["UBUNTU_IMAGE"] --name $Node --cpus $cpus --memory $mem --disk $disk --network $net --cloud-init $ci --timeout 1800
}

# ---------------------------------------------------------------- code sync
function Push-Code {
  Write-Step "Copying project code to master:~/study"
  $tgz = Join-Path $env:TEMP "study.tgz"
  if (Test-Path $tgz) { Remove-Item $tgz -Force }
  $items = @("cluster.conf", "infra", "apps", "experiments", "analysis", "codec-bench") | Where-Object { Test-Path (Join-Path $Root $_) }
  & tar.exe -czf $tgz -C $Root @items
  if ($LASTEXITCODE -ne 0) { throw "tar failed" }
  Push-Location $env:TEMP
  try { Invoke-Mp transfer study.tgz master:/home/ubuntu/study.tgz } finally { Pop-Location }
  Invoke-VmBash master @'
set -e
mkdir -p ~/study
tar --warning=no-timestamp -xzf ~/study.tgz -C ~/study
rm -f ~/study.tgz
find ~/study -type f \( -name '*.sh' -o -name '*.py' -o -name '*.conf' -o -name '*.tpl' -o -name '*.yaml' -o -name '*.yml' -o -name '*.xml' -o -name '*.java' \) -exec sed -i 's/\r$//' {} +
find ~/study -name '*.sh' -exec chmod +x {} +
'@
}

# ---------------------------------------------------------------- laptop help
function Show-LaptopSteps {
  $w = $C["WORKER_CPUS"]; $m = $C["WORKER_MEM"]; $d = $C["WORKER_DISK"]; $p = $C["JOIN_PORT"]
  Write-Host ""
  Write-Host "=================== NEXT: run on the LAPTOP (admin PowerShell) ===================" -ForegroundColor Yellow
  Write-Host "multipass delete --purge nettest-laptop"
  Write-Host "Invoke-WebRequest -UseBasicParsing http://${MasterLan}:$p/worker3.yaml -OutFile `$env:TEMP\worker3.yaml"
  Write-Host "multipass launch $($C["UBUNTU_IMAGE"]) --name worker3 --cpus $w --memory $m --disk $d --cloud-init `$env:TEMP\worker3.yaml --timeout 1800"
  Write-Host "==================================================================================" -ForegroundColor Yellow
  Write-Host "Then on the DESKTOP: powershell -ExecutionPolicy Bypass -File infra\windows\desktop.ps1 install"
}

# ---------------------------------------------------------------- commands
function Do-Up {
  $inst = Get-Instances
  if ($inst.ContainsKey("nettest-desktop")) {
    Write-Step "Deleting the network test VM nettest-desktop"
    Invoke-Mp delete --purge nettest-desktop
  }
  foreach ($n in $DesktopNodes) {
    if ($inst.ContainsKey($n)) {
      Write-Step "$n already exists - not recreating it"
      if ($inst[$n].state -ne "Running") { Invoke-Mp start $n }
    } else {
      New-DesktopVm $n
    }
  }
  Push-Code

  Write-Step "Cluster SSH key (master -> all nodes)"
  Invoke-VmBash master 'test -f ~/.ssh/id_ed25519 || ssh-keygen -q -t ed25519 -N "" -C ubuntu@master -f ~/.ssh/id_ed25519'
  $pub = ((& multipass exec master -- cat /home/ubuntu/.ssh/id_ed25519.pub) | Out-String).Trim()
  foreach ($n in $DesktopNodes) {
    Invoke-VmBash $n "grep -qF '$pub' ~/.ssh/authorized_keys || echo '$pub' >> ~/.ssh/authorized_keys"
  }

  Write-Step "WireGuard mesh on the desktop VMs + worker3 config for the laptop"
  Invoke-VmBash master 'bash ~/study/infra/linux/wg-setup.sh'
  Show-LaptopSteps
}

function Do-Install {
  Push-Code
  Invoke-VmBash master 'bash ~/study/infra/linux/install-all.sh'
}

function Do-Start {
  foreach ($n in $DesktopNodes) { Write-Step "Starting $n"; Invoke-Mp start $n }
  Write-Host "Make sure worker3 is running on the laptop:  multipass start worker3" -ForegroundColor Yellow
  Invoke-VmBash master 'bash ~/study/infra/linux/cluster-start.sh'
}

function Do-Stop {
  $inst = Get-Instances
  if ($inst.ContainsKey("master") -and $inst["master"].state -eq "Running") {
    try { Invoke-VmBash master 'bash ~/study/infra/linux/cluster-stop.sh' } catch { Write-Warning $_ }
  }
  foreach ($n in $DesktopNodes) { Write-Step "Stopping $n"; & multipass stop $n }
  Write-Host "On the laptop you can now run:  multipass stop worker3" -ForegroundColor Yellow
}

function Do-Status {
  & multipass list
  Invoke-VmBash master 'bash ~/study/infra/linux/healthcheck.sh'
}

function Do-Pull {
  Write-Step "Copying master:~/study/results into $Root\results"
  Invoke-VmBash master 'mkdir -p ~/study/results && tar -czf ~/results.tgz -C ~/study results'
  $remote = [long]((& multipass exec master -- stat -c %s /home/ubuntu/results.tgz) | Out-String).Trim()
  $local = Join-Path $env:TEMP "results.tgz"
  Remove-Item $local -Force -ErrorAction SilentlyContinue
  # multipass exits 2 on Windows because it cannot chmod the local file; the copy
  # itself is fine, so check the size instead of the exit code
  Push-Location $env:TEMP
  $eap = $ErrorActionPreference; $ErrorActionPreference = "Continue"
  try { & multipass transfer master:/home/ubuntu/results.tgz results.tgz 2>&1 | Out-Null }
  finally { $ErrorActionPreference = $eap; Pop-Location }
  if (-not (Test-Path $local) -or (Get-Item $local).Length -ne $remote) { throw "results transfer incomplete" }
  & tar.exe -xzf $local -C $Root
  if ($LASTEXITCODE -ne 0) { throw "tar extract failed" }
  Write-Host ("  {0:N1} MB copied" -f ($remote / 1MB))
}

function Do-Destroy {
  if (-not $Force) {
    $a = Read-Host "Delete VMs $($DesktopNodes -join ', ') and all their data? Type YES"
    if ($a -ne "YES") { Write-Host "Cancelled."; return }
  }
  foreach ($n in $DesktopNodes) { & multipass delete --purge $n }
}

switch ($Command.ToLower()) {
  "up"      { Do-Up }
  "install" { Do-Install }
  "start"   { Do-Start }
  "stop"    { Do-Stop }
  "status"  { Do-Status }
  "push"    { Push-Code }
  "pull"    { Do-Pull }
  "shell"   { & multipass shell master }
  "destroy" { Do-Destroy }
  default   { Get-Help $PSCommandPath; Write-Host "Commands: up | install | start | stop | status | push | pull | shell | destroy" }
}
