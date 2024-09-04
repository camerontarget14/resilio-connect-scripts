[CmdletBinding()]
param
(
	[switch]$NoX86exeCheck,
	[switch]$NoExtensionUpgrade,
	[switch]$NoFLDriverUpgrade,
	[switch]$NoDiskSpaceCheck,
	[switch]$Verify,
	[switch]$CreateUpgradeTask,
	[switch]$RunUpgradeTask
)

<#
.SYNOPSIS
The script is intended to upgrade current installation of Resilio Connect Agent to required version.

.DESCRIPTION
If done via Connect Job, upgrade script should advised to be started by TaskScheduler service to 
completely detach from launching agent. The script can start from any folder. Script expects next 
files to be present in the script folder: 
  oldstorage.path" - should contain path to current storage folder. Used when migrating 2.4 agent
                     to 2.5 or newer agent
  Resilio-Connect-Agent.exe - x86 version of executable. (only for x86 Win upgrades)
  Resilio-Connect-Agent_x64.exe - x64 version of executable (only for x64 Win upgrades)
  Resilio-File-Locking-Driver_x64.msi - file locking driver msi package

Proper version is selected automatically.

Script stops the service and waits 10 minutes to get service shut down. After that it kills the
service process and proceed with the replacement.

Script compares the version of existing binary and a new one. If there's a 2.5 version crossed,
script will take care to transfer old storage folder to it's new position 
("C:\ProgramData\Resilio\Connect Agent"). If file "oldstorage.path" not specified, old storage 
is taken from 
"C:\Windows\System32\config\systemprofile\AppData\Roaming\Resilio Connect Agent Service\"

Run without parameters to actually perform an upgrade

.PARAMETER Verify
Runs the script in verification mode. Only checks if all the pre-requisites for the upgrade are
met.

.PARAMETER NoX86exeCheck
Use with -Verify only. Skips verification of x86 version of binary and do not count it as 
terminating error.

.PARAMETER CreateUpgradeTask
Forces the script to create upgrade task in Task Scheduler service. Call it before running with
-RunUpgradeTask. Upgrade via Task Scheduler service is mandatory to detach from Agent's
command prompt.

.PARAMETER RunUpgradeTask
Runs Windows Task Scheduler task named "ResilioUpgrade" to actually perform an upgrade. Upgrade 
via Task Scheduler service is mandatory to detach from Agent's command prompt.

.PARAMETER NoExtensionUpgrade
Prevents script from upgrading Explorer extensions (used for selective sync)

.PARAMETER NoFLDriverUpgrade
Prevents script from upgrading file locking driver

.LINK
https://github.com/resilio-inc/connect-scripts/tree/master/Agent%20Upgrade%20Pack

.OUTPUTS
Script populates verify.log in the folders it started from with all the necessary checks. If any
of checks fail, it won't perform the upgrade.
Script drops the upgrade.log to the folder it started from
#>

$extractor = @"
using System;
using System.IO;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;

public class ExtractData
{
    [System.Flags]
    enum LoadLibraryFlags : uint
    {
        None = 0,
        DONT_RESOLVE_DLL_REFERENCES = 0x00000001,
        LOAD_IGNORE_CODE_AUTHZ_LEVEL = 0x00000010,
        LOAD_LIBRARY_AS_DATAFILE = 0x00000002,
        LOAD_LIBRARY_AS_DATAFILE_EXCLUSIVE = 0x00000040,
        LOAD_LIBRARY_AS_IMAGE_RESOURCE = 0x00000020,
        LOAD_LIBRARY_SEARCH_APPLICATION_DIR = 0x00000200,
        LOAD_LIBRARY_SEARCH_DEFAULT_DIRS = 0x00001000,
        LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR = 0x00000100,
        LOAD_LIBRARY_SEARCH_SYSTEM32 = 0x00000800,
        LOAD_LIBRARY_SEARCH_USER_DIRS = 0x00000400,
        LOAD_WITH_ALTERED_SEARCH_PATH = 0x00000008
    }

    [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Ansi)]
    private static extern IntPtr LoadLibrary([MarshalAs(UnmanagedType.LPStr)]string lpFileName);


    [DllImport("kernel32.dll", SetLastError = true)]
    static extern IntPtr LoadLibraryEx(string lpFileName, IntPtr hReservedNull, LoadLibraryFlags dwFlags);

    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool FreeLibrary(IntPtr hModule);

    [DllImport("kernel32.dll")]
    static extern IntPtr FindResource(IntPtr hModule, IntPtr lpName, IntPtr lpType);


    [DllImport("kernel32.dll", SetLastError = true)]
    static extern IntPtr LoadResource(IntPtr hModule, IntPtr hResInfo);

    [DllImport("Kernel32.dll", EntryPoint = "SizeofResource", SetLastError = true)]

    private static extern uint SizeofResource(IntPtr hModule, IntPtr hResource);


    public byte[] ExtractDLLFromEXE(string file, int number)
    {
        IntPtr lib = IntPtr.Zero;
        lib = LoadLibraryEx(file, IntPtr.Zero, LoadLibraryFlags.LOAD_LIBRARY_AS_DATAFILE);

        String type = "BIN";
        IntPtr strPtr = new IntPtr(number);
        IntPtr p = FindResource(lib, strPtr, Marshal.StringToHGlobalAnsi(type));
        int size = (int)SizeofResource(lib, p);

        p = LoadResource(lib, p);

        byte[] dll = new byte[size];
        Marshal.Copy(p, dll, 0, size);

        FreeLibrary(lib);

        return dll;
    }
}
"@

$explorerxmlpart1 = '<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
<RegistrationInfo>
<Date>2019-08-08T05:20:27.7653597</Date>
<Author>ResilioInc</Author>
</RegistrationInfo>
<Triggers />
<Principals>
<Principal id="Author">
<UserId>'

$explorerxmlpart2 = '</UserId>
<LogonType>InteractiveToken</LogonType>
<RunLevel>LeastPrivilege</RunLevel>
</Principal>
</Principals>
<Settings>
<MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
<DisallowStartIfOnBatteries>true</DisallowStartIfOnBatteries>
<StopIfGoingOnBatteries>true</StopIfGoingOnBatteries>
<AllowHardTerminate>true</AllowHardTerminate>
<StartWhenAvailable>false</StartWhenAvailable>
<RunOnlyIfNetworkAvailable>false</RunOnlyIfNetworkAvailable>
<IdleSettings>
<StopOnIdleEnd>true</StopOnIdleEnd>
<RestartOnIdle>false</RestartOnIdle>
</IdleSettings>
<AllowStartOnDemand>true</AllowStartOnDemand>
<Enabled>true</Enabled>
<Hidden>false</Hidden>
<RunOnlyIfIdle>false</RunOnlyIfIdle>
<WakeToRun>false</WakeToRun>
<ExecutionTimeLimit>P3D</ExecutionTimeLimit>
<Priority>7</Priority>
</Settings>
<Actions Context="Author">
<Exec>
<Command>explorer.exe</Command>
</Exec>
</Actions>
</Task>'

$upgradexmlpart1 = '<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Date>2018-11-02T23:15:32</Date>
    <Author>ResilioInc</Author>
    <URI>\ResilioUpgrade</URI>
  </RegistrationInfo>
  <Triggers>
    <TimeTrigger>
      <StartBoundary>1910-01-01T00:00:00</StartBoundary>
      <Enabled>true</Enabled>
    </TimeTrigger>
  </Triggers>
  <Principals>
    <Principal id="Author">
      <UserId>S-1-5-18</UserId>
      <RunLevel>LeastPrivilege</RunLevel>
    </Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>true</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <AllowHardTerminate>true</AllowHardTerminate>
    <StartWhenAvailable>false</StartWhenAvailable>
    <RunOnlyIfNetworkAvailable>false</RunOnlyIfNetworkAvailable>
    <IdleSettings>
      <Duration>PT10M</Duration>
      <WaitTimeout>PT1H</WaitTimeout>
      <StopOnIdleEnd>true</StopOnIdleEnd>
      <RestartOnIdle>false</RestartOnIdle>
    </IdleSettings>
    <AllowStartOnDemand>true</AllowStartOnDemand>
    <Enabled>true</Enabled>
    <Hidden>false</Hidden>
    <RunOnlyIfIdle>false</RunOnlyIfIdle>
    <WakeToRun>false</WakeToRun>
    <ExecutionTimeLimit>PT72H</ExecutionTimeLimit>
    <Priority>7</Priority>
  </Settings>
  <Actions Context="Author">
    <Exec>
      <Command>powershell.exe  </Command>
      <Arguments>-NoProfile -ExecutionPolicy Bypass -File '
$upgradexmlpart2 = '</Arguments>
    </Exec>
  </Actions>
</Task>
'
# --------------------------------------------------------------------------------------------------------------------------------

function Verify-UpgradePossible
{
	$errcode = 0
	$VerbosePreference = 'Continue'
	try
	{
		Add-Type -Assembly System.Windows.Forms
		######### Check 0 - files and paths
		$filecheckfailure = $false
		Write-Verbose "Checking upgradeables"
		if (!(Test-Path ".\Resilio-Connect-Agent.exe" -PathType Leaf))
		{
			if (!$NoX86exeCheck)
			{
				Write-Verbose "Resilio-Connect-Agent.exe file is missing"
				$filecheckfailure = $true
				$errcode = 2
			}
			else
			{
				Write-Verbose "Bypassing x86 binary check as requested"
			}
		}
		
		if (!(Test-Path "Resilio-Connect-Agent_x64.exe" -PathType Leaf))
		{
			Write-Verbose "Resilio-Connect-Agent_x64.exe file is missing"
			$filecheckfailure = $true
			$errcode = 3
		}

		$filelockingenabled = Get-FileLockingFeatureEnabled -AgentFileNameX64 "Resilio-Connect-Agent_x64.exe"
		if ($filelockingenabled)
		{
			if (!(Test-Path "Resilio-File-Locking-Driver_x64.msi" -PathType Leaf))
			{
				if (!$NoFLDriverUpgrade)
				{
					Write-Verbose "Resilio-File-Locking-Driver_x64.msie file is missing"
					$filecheckfailure = $true
					$errcode = 20
				}
				else
				{
					Write-Verbose "Bypassing file locking driver check as requested"
				}
			}
		}
		else
		{
			if (Test-Path "Resilio-File-Locking-Driver_x64.msi" -PathType Leaf)
			{
				Write-Verbose "Skipping driver upgrade as agent doesn't support file locking"
			}
		}

		if ($filecheckfailure)
		{
			throw "Some files are missing or paths are invalid, upgrade impossile"
		}
		Write-Verbose "[OK]"
		
		######### Check 1 - elevated privileges
		Write-Verbose "Checking for elevated privileges..."
		$currentPrincipal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
		if (!$currentPrincipal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator))
		{
			$errcode = 12
			throw "Script is not running with elevated privileges, upgrade impossible"
		}
		Write-Verbose "[OK]"
		
		######### Check 2 - if running on battery
		Write-Verbose "Checking computer is AC powered..."
		
		if ([System.Windows.Forms.SystemInformation]::PowerStatus.PowerLineStatus -ne 'Online')
		{
			$errcode = 13
			throw "Computer runs on battery power, upgrade is too risky now"
		}
		Write-Verbose "[OK]"
		
		######### Check 3 - if task scheduler works
		Write-Verbose "Checking Task Scheduler service is running..."
		if ((Get-Service -Name "schedule").Status -ne 'Running')
		{
			$errcode = 14
			throw "Task Scheduler service is not running, upgrade is not possible"
		}
		Write-Verbose "[OK]"
		
		######### Check 4 - checking the agent version
		Write-Verbose "Checking Agent versions..."
		$processname = "Resilio Connect Agent.exe"
		$agentupgradeablex86 = "Resilio-Connect-Agent.exe"
		$agentupgradeablex64 = "Resilio-Connect-Agent_x64.exe"
		if ([IntPtr]::size -eq 8) { $agentupgradeble = $agentupgradeablex64 }
		else { $agentupgradeble = $agentupgradeablex86 }
		$tmp = Get-ItemProperty -path 'HKLM:\SOFTWARE\Resilio, Inc.\Resilio Connect Agent\' -ErrorAction SilentlyContinue
		if (!$tmp)
		{
			$errcode = 16
			throw "Agent installation not found on target system"
		}
		$processpath = $tmp.InstallDir
		$fullexepath = Join-Path -Path $processpath -ChildPath $processname
		$fullupgradeablepath = Join-Path -Path $ownscriptpath -ChildPath $agentupgradeble
		[System.Version]$oldversion = [System.Diagnostics.FileVersionInfo]::GetVersionInfo("$fullexepath").FileVersion
		[System.Version]$newversion = [System.Diagnostics.FileVersionInfo]::GetVersionInfo("$fullupgradeablepath").FileVersion

		if ($oldversion.Build -lt 10000)
		{
			if ($oldversion -gt $newversion)
			{
				$errcode = 15
				throw "Attempted to downgrade from $oldversion to $newversion, downgrade is not supported"
			}
		}
		else { Write-Verbose "Skipping downgrade verification check since exising Agent installation is a custom build" }
		
		[System.Version]$newfldriverversion = $Null
		[System.Version]$oldfldriverversion = $Null

		if (!$NoFLDriverUpgrade)
		{
			$msifldriverfullpath = Join-Path -Path $ownscriptpath -ChildPath "Resilio-File-Locking-Driver_x64.msi"
			$msifldriverversion = Get-MsiProductVersion -MsiPath $msifldriverfullpath
			if ($msifldriverversion -ne $newversion)
			{
				$errcode = 21
				throw "File locking driver msi must have the same version as the agent, found $msifldriverversion, expected $newversion"
			}

			$oldfldriverpath = Join-Path -Path $processpath -ChildPath 'rsldrv\rsldrv.sys'
			if ([System.IO.File]::Exists("$oldfldriverpath"))
			{
				$oldfldriverversion = [System.Diagnostics.FileVersionInfo]::GetVersionInfo("$oldfldriverpath").FileVersion
			}

			$newfldriverversion = Get-MsiFileVersion -MsiPath $msifldriverfullpath -FileName "rsldrv.sys"
		}

		if (!$filelockingenabled)
		{
			if ($oldversion -eq $newversion)
			{
				Write-Verbose "Same version detected, no point in launching upgrade"
				$errcode = 1
			}
			else { Write-Verbose "[OK]" }
		}
		else
		{
			if ($NoFLDriverUpgrade -and $oldversion -eq $newversion)
			{
				Write-Verbose "Same version detected, no point in launching upgrade"
				$errcode = 1
			}
			elseif (!$NoFLDriverUpgrade -and ($oldversion -eq $newversion) -and ($newfldriverversion -eq $oldfldriverversion))
			{
				Write-Verbose "Same versions detected, no point in launching upgrade"
				$errcode = 1
			}
			else { Write-Verbose "[OK]" }
		}

		######### Check 5 - Calculate if disk space is enough for the upgrade
		if (!$NoDiskSpaceCheck)
		{
			Write-Verbose "Checking total size of estimated backup..."
			$storagepath = "$env:ProgramData\Resilio\Connect Agent\"
			[int64]$totalsize = 0
			$totalsize += (Get-Item "$storagepath\settings.dat" -ErrorAction SilentlyContinue).Length
			$totalsize += (Get-Item "$storagepath\sync.dat" -ErrorAction SilentlyContinue).Length
			$databases = Get-ChildItem -Path $storagepath -Filter *.db*
			foreach ($databasefile in $databases)
			{
				$totalsize += $databasefile.Length
			}
			$storagedrive = (Get-Item $storagepath).PSDrive.Name
			$driveproperties = (Get-CimInstance -Class CIM_LogicalDisk | Select-Object * | Where-Object { $_.DriveType -eq '3' -or $_.DriveType -eq '4' } | Where-Object {$_.Name -eq "$($storagedrive):"}) 4>$null
			if ($driveproperties.FreeSpace -lt $totalsize)
			{
				$errcode = 19
				throw "Insufficiend disk space. Drive $($storagedrive): has $($driveproperties.FreeSpace) free space, $totalsize required for backup"
			}
			Write-Verbose "[OK]"
		}
		else { Write-Verbose "Bypassing disk space check as requested" }

		# If no errors found, we can report that the upgrade will happen
		if ($errcode -eq 0)
		{
			if ($oldversion -ne $newversion)
			{
				Write-Verbose "Agent can be upgraded from $oldversion to $newversion"
			}
			else
			{
				Write-Verbose "Only file locking driver can be upgraded"
			}
		}
	}
	catch
	{
		Write-Verbose "ERROR: $_"
	}
	return $errcode
}
# --------------------------------------------------------------------------------------------------------------------------------

function Get-BinaryArchitecture
{
	param
	(
		[parameter(Mandatory = $true)]
		[string]$Path
	)
	$PEHeader = [System.IO.File]::ReadAllBytes($path)
	
	$PE_POINTER = [System.BitConverter]::ToUInt32($PEHeader, 60)
	if ($PE_POINTER -gt 4096 -or $PE_POINTER -eq 0) { throw "Cannot determine binary architecture" }
	$Arch = [System.BitConverter]::ToUInt16($PEHeader, $PE_POINTER + 4)
	
	if ($Arch -eq 0x014c) { return "x86" }
	if ($Arch -eq 0x0200 -or $Arch -eq 0x8664) { return "x64" }
}

# --------------------------------------------------------------------------------------------------------------------------------

function Get-MsiProductVersion
{
	param
	(
		[parameter(Mandatory = $true)]
		[string]$MsiPath
	)

	[int]$msiOpenDatabaseMode = 0
	$inst = new-object -comobject WindowsInstaller.Installer
	$db = $inst.GetType().InvokeMember("OpenDatabase", "InvokeMethod", $Null, $inst, @($MsiPath, $msiOpenDatabaseMode))
	$view = $db.GetType().InvokeMember("OpenView", "InvokeMethod", $Null, $db, @("SELECT `Value` FROM `Property` WHERE `Property` = 'ProductVersion'"))
	$view.GetType().InvokeMember("Execute", "InvokeMethod", $Null, $view, $Null) | Out-Null
	$record = $view.GetType().InvokeMember("Fetch", "InvokeMethod", $Null, $view, $Null)
	$ver = $record.GetType().InvokeMember("StringData", "GetProperty", $null, $record, 1)
	$view.GetType().InvokeMember("Close", "InvokeMethod", $null, $view, $null) | Out-Null
	return [System.Version]$ver
}

# --------------------------------------------------------------------------------------------------------------------------------

function Get-MsiFileVersion
{
	param
	(
		[parameter(Mandatory = $true)]
		[string]$MsiPath,
		[parameter(Mandatory = $true)]
		[string]$FileName
	)

	[int]$msiOpenDatabaseMode = 0
	$inst = new-object -comobject WindowsInstaller.Installer
	$db = $inst.GetType().InvokeMember("OpenDatabase", "InvokeMethod", $Null, $inst, @($MsiPath, $msiOpenDatabaseMode))
	$view = $db.GetType().InvokeMember("OpenView", "InvokeMethod", $Null, $db, @("SELECT `Version` FROM `File` WHERE `FileName` = '$FileName'"))
	$view.GetType().InvokeMember("Execute", "InvokeMethod", $Null, $view, $Null) | Out-Null
	$record = $view.GetType().InvokeMember("Fetch", "InvokeMethod", $Null, $view, $Null)
	$ver = $record.GetType().InvokeMember("StringData", "GetProperty", $null, $record, 1)
	$view.GetType().InvokeMember("Close", "InvokeMethod", $null, $view, $null) | Out-Null
	return [System.Version]$ver
}

# --------------------------------------------------------------------------------------------------------------------------------

function Get-FileLockingFeatureEnabled
{
	param
	(
		[parameter(Mandatory = $false)]
		[string]$AgentFileNameX64,
		[parameter(Mandatory = $false)]
		[System.Version]$NewAgentVersion
	)

	if ($AgentFileNameX64)
	{
		if ([System.IO.File]::Exists("$AgentFileNameX64"))
		{
			$NewAgentVersion = [System.Diagnostics.FileVersionInfo]::GetVersionInfo("$AgentFileNameX64").FileVersion
		}
	}
	$featureEnabled = $NewAgentVersion -gt [System.Version]"4.1.0.0"
	return $featureEnabled
}

# --------------------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------------------
# Script starts here
$ownscriptpathname = $MyInvocation.MyCommand.Definition
$ownscriptpath = Split-Path -Path $ownscriptpathname
$ownscriptname = Split-Path $ownscriptpathname -Leaf

# Here we need to verify if the installation can be done
if ($Verify)
{
	$result = Verify-UpgradePossible
	exit $result
}

# Just register self as a task scheduler
if ($CreateUpgradeTask)
{
	if ($NoExtensionUpgrade) { $AgentUpgradeXML = "$upgradexmlpart1`"$ownscriptpathname`" -NoExtensionUpgrade -Verbose$upgradexmlpart2" }
	else { $AgentUpgradeXML = "$upgradexmlpart1`"$ownscriptpathname`" -Verbose$upgradexmlpart2"}
	Set-Content -Path "ResilioUpgrade.xml" -Value $AgentUpgradeXML
	Start-Process -FilePath "schtasks" -ArgumentList "/create /TN ResilioUpgrade /XML ResilioUpgrade.xml /F"
	exit 0
}

# Start previously registered task from task scheduler
if ($RunUpgradeTask)
{
	Start-Process -FilePath "schtasks" -ArgumentList "/run /tn ResilioUpgrade"
	exit 0	
}

# Start logging
Start-Transcript -Path "$ownscriptpath\upgrade.log" -Append

Write-Verbose "Upgrade script started"
try
{
	# Define common names and paths used below
	if ([System.IO.File]::Exists("$ownscriptpath\oldstorage.path")) { $oldstoragepath = Get-Content "$ownscriptpath\oldstorage.path" }
	else { $oldstoragepath = "$env:SystemRoot\System32\config\systemprofile\AppData\Roaming\Resilio Connect Agent Service\" }
	
	$servicename = "connectsvc"
	$processname = "Resilio Connect Agent.exe"
	$agentupgradeablex86 = "Resilio-Connect-Agent.exe"
	$agentupgradeablex64 = "Resilio-Connect-Agent_x64.exe"
	$extensionx86 = "SyncShellContextMenu_x86.dll"
	$extensionx64 = "SyncShellContextMenu_x64.dll"
	$fldrivermsix64 = "Resilio-File-Locking-Driver_x64.msi"
	$newstoragepath = "$env:ProgramData\Resilio\Connect Agent\"
	$processpath = (Get-ItemProperty -path 'HKLM:\SOFTWARE\Resilio, Inc.\Resilio Connect Agent\').InstallDir
	Write-Verbose "Found Agent installed to: $processpath"
	$fullexepath = Join-Path -Path $processpath -ChildPath $processname
	$ExeArchitecture = Get-BinaryArchitecture -Path $fullexepath
	if ($ExeArchitecture)
	{
		if ($ExeArchitecture -eq 'x86')
		{
			$agentupgradeble = $agentupgradeablex86
			Write-Verbose "OS identified as x86 bit version of Windows"
		}
		else
		{
			$agentupgradeble = $agentupgradeablex64
			Write-Verbose "OS identified as x64 bit version of Windows"
		}
	}
	else
	{
		$agentupgradeble = $agentupgradeablex64
		Write-Verbose "Failed to determine executable architecture, using x64 by default"
	}
	$fullupgradeablepath = Join-Path -Path $ownscriptpath -ChildPath $agentupgradeble
	[System.Version]$oldversion = [System.Diagnostics.FileVersionInfo]::GetVersionInfo("$fullexepath").FileVersion
	[System.Version]$newversion = [System.Diagnostics.FileVersionInfo]::GetVersionInfo("$fullupgradeablepath").FileVersion
	$fullextx86path = Join-Path -Path $processpath -ChildPath $extensionx86
	$fullextx64path = Join-Path -Path $processpath -ChildPath $extensionx64
	$filelockingenabled = Get-FileLockingFeatureEnabled -NewAgentVersion $newversion
	
	Write-Verbose "Currently installed verison of agent is: $oldversion"
	Write-Verbose "Going to upgrade to agent verison: $newversion"
	
	# Stop Agent service
	Write-Verbose "Stopping agent service..."
	Stop-Service -Name $servicename
	
	# Give it some minutes to end in case it got really large DB
	$timer = [system.diagnostics.stopwatch]::StartNew()
	while ($timer.Elapsed.Minutes -lt 10)
	{
		$srv = Get-Service -Name $servicename
		if ($srv.Status -eq 'Stopped')
		{
			Write-Verbose "Agent service stopped gracefully"
			break
		}
		Start-Sleep 1
	}
	$timer.Stop
	
	# If it hangs, kill the process forcefully
	if ($timer.Elapsed.Minutes -ge 10)
	{
		Stop-Process -Name $processname -Force
		Write-Verbose "Agent service failed to stop, killing process"
	}
	
	# Now kill all the rest of agents which are actually UI processes
	Get-Process -Name "Resilio Connect Agent" | Stop-Process -Force
	
	# Rename old executable
	Move-Item -Path "$fullexepath" -Destination "$fullexepath.old" -Force
	Write-Verbose "Old binary got renamed"
	
	# Put new executable in place of old one
	Copy-Item -Path $fullupgradeablepath -Destination $fullexepath -Force
	Write-Verbose "New binary is in place"
	
	# Migrate storage from old position to a new one if necessary
	if ($oldversion -lt [System.Version]"2.5.0.0" -and $newversion -ge [System.Version]"2.5.0.0")
	{
		Write-Verbose "Migrating old storage to a new position in ProgramData"
		# Ensure folder exists
		if (!(Test-Path $newstoragepath)) { New-Item -Path "$newstoragepath" -ItemType Directory -Force | Out-Null }
		
		# Set full perms to local system user
		# Disable ACLs temporary
		#$perms = Get-Acl "$newstoragepath"
		#$newperms = New-Object System.Security.AccessControl.FileSystemAccessRule("LOCAL SYSTEM", "FullControl", "ContainerInherit,ObjectInherit", "None", "Allow")
		#$perms.SetAccessRule($newperms)
		#Set-Acl "$newstoragepath" $perms
		
		Copy-Item -path "$oldstoragepath\*" -Destination "$newstoragepath" -Recurse -Force
	}
	
	# Upgrade Explorer extension DLLs if they are installed
	if ([System.IO.File]::Exists("$fullextx86path") -and !$NoExtensionUpgrade)
	{
		# Unregister old extensions
		Write-Verbose "Unregistering extension"
		Start-Process -FilePath "regsvr32" -ArgumentList "/u /s `"$fullextx86path`""
		Start-Process -FilePath "regsvr32" -ArgumentList "/u /s `"$fullextx64path`""
		# Rename old extension DLLs
		Remove-Item -Path "$fullextx86path.old" -Force -ErrorAction SilentlyContinue
		Remove-Item -Path "$fullextx64path.old" -Force -ErrorAction SilentlyContinue
		Move-Item -Path "$fullextx86path" -Destination "$fullextx86path.old" -Force -ErrorAction SilentlyContinue
		Move-Item -Path "$fullextx64path" -Destination "$fullextx64path.old" -Force -ErrorAction SilentlyContinue
		Write-Verbose "Old extension DLLs were renamed"
		
		# Extract new extension DLLs in place of old ones
		Add-Type -TypeDefinition $extractor		
		$objextrator = New-Object ExtractData
		$dllx86 = $objextrator.ExtractDLLFromEXE($fullexepath, 418)
		$dllx64 = $objextrator.ExtractDLLFromEXE($fullexepath, 419)
		Set-Content -Path $fullextx86path -Value $dllx86 -Encoding Byte
		Set-Content -Path $fullextx64path -Value $dllx64 -Encoding Byte
		Write-Verbose "New extension DLLs are in place"
		# Register new extensions
		Write-Verbose "Registering updated extensions"
		Start-Process -FilePath "regsvr32" -ArgumentList "/i /s `"$fullextx86path`""
		Start-Process -FilePath "regsvr32" -ArgumentList "/i /s `"$fullextx64path`""
		
		# Restart explorer so new DLLs take effect
		$currentloggeduser = (Get-Process -Name explorer -IncludeUserName -ErrorAction SilentlyContinue).UserName
		if ($currentloggeduser)
		{
			Write-Verbose "Found user `"$currentloggeduser`" logged in, attempting to restart explorer.exe"
			Stop-Process -Name explorer -Force
			Start-Sleep 5
			
			if (!(Get-Process -Name explorer))
			{
				Write-Verbose "Explorer did not restart automatically, restarting via Task Scheduler"
				$ExplorerXML = "$explorerxmlpart1$currentloggeduser$explorerxmlpart2"
				Set-Content -Path "StartExplorer.xml" -Value $ExplorerXML
				Start-Process -FilePath "schtasks" -ArgumentList "/create /TN StartExplorer /XML StartExplorer.xml /F"
				Start-Sleep -Seconds 3
				Start-Process -FilePath "schtasks" -ArgumentList "/run /tn StartExplorer"
			}
			else
			{
				Write-Verbose "Explorer restarted automatically by OS"
			}
		}
		else
		{
			Write-Verbose "No user logged in, explorer restart is not required"
		}
	}

	# Upgrade file locking driver
	if ($filelockingenabled)
	{
		if ($ExeArchitecture -eq 'x64')
		{
			$fullfldrivermsix64 = Join-Path -Path $ownscriptpath -ChildPath $fldrivermsix64
			if ([System.IO.File]::Exists("$fullfldrivermsix64"))
			{
				Write-Verbose "Installing file locking driver"
				$msiresult = (Start-Process -FilePath "msiexec" -ArgumentList "/quiet /norestart /i `"$fullfldrivermsix64`"" -wait -passthru).ExitCode
				Write-Verbose "Driver installation finished with result $msiresult"
			}
		}
	}
}
catch
{
	Write-Output "Unexpected error occuerd: $_"
}
finally
{
	# Start Agent service
	Start-Service -Name $servicename
	Write-Verbose "Agent service started"
	
	# Stop logging
	Write-Verbose "Finishing logging"
	Stop-Transcript
}
