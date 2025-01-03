#
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify,
# merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
# PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#

<#
    .SYNOPSIS
        CreateRdsbackupMonitoring.ps1

    .DESCRIPTION
        This script configures RDS instances to export their Error Logs into CloudWatch, then configures a Metric and Alarm to report SQL Server backup Errors
        You can run for all RDS instances in all regions or filter for a subset of regions and RDS instances
    .EXAMPLE
        To run for All regions and All RDS instances:
        .\CreateRdsbackupMonitoring.ps1 -SnsTopic 'arn:aws:sns:xxxxx:0000000000:MySNSTopic' 

        To filter for a subset of regions:
        .\CreateRdsbackupMonitoring.ps1 -regions 'us-east-1,us-east-2' -SnsTopic 'arn:aws:sns:xxxxx:0000000000:MySNSTopic' 

        To run for a subset of RDS Instances
        .\CreateRdsbackupMonitoring.ps1 -regions 'us-east-1' -SnsTopic 'arn:aws:sns:xxxxx:0000000000:MySNSTopic' -RdsInstances 'sql-1,sql-2' 

        To run for a single RDS Instances
        .\CreateRdsbackupMonitoring.ps1 -regions 'us-east-1' -SnsTopic 'arn:aws:sns:xxxxx:0000000000:MySNSTopic' -RdsInstances 'sql-1' 

    .NOTES
        Date: 01/02/2025

    .AUTHOR
        Phil Ekins - ekins@amazon.com
#>

Param (
    [Parameter(Mandatory=$true)]  [string] $SnsTopicARN,
    [Parameter(Mandatory=$false)] [String] $Regions,
    [Parameter(Mandatory=$false)] [string] $RdsInstances
)

#==================================================
# Variables
#==================================================

$Namespace        = "AWS-RDS-Backup-Errors"
$AlarmDescription = "Alarm when RDS Backup Error Detected"
$LogFilterName = "AWS-RDS-Backup-Filter"
$LogFilterPattern = "Errors Detected in RDS Backup Queue Status"

#==================================================
# Functions
#==================================================

Function IsvalidRegion {
    [CmdletBinding()]
    param (
        [String]$Region
    )

    Try {
        IF (([string]::IsNullOrWhiteSpace($Region))) { 
            $IsvalidRegion = $false
        } Else {
            IF ((Get-AWSRegion -SystemName $Region -IncludeChina -IncludeGovCloud).Name -ne 'Unknown') {
                $AccountNumber = $Null
                $AccountNumber = Get-STSCallerIdentity -Region $Region -ErrorAction Stop | Select-Object -ExpandProperty 'Account'
                $IsvalidRegion = $true
            } Else {
                $IsvalidRegion = $false
            }
        }
    } Catch {
        $IsvalidRegion = $false
    }
    Return $IsvalidRegion
}

#==================================================
# Main
#==================================================

IF (!([string]::IsNullOrWhiteSpace($SnsTopicARN))) { 
    Try {
        $SnsTopic = Get-SNSTopicAttribute -TopicArn $SnsTopicARN -ErrorAction Stop
    } Catch {
        Write-Host "Invalid SNS Topic."
        Exit
    }
} Else {
    Write-Host "Invalid SNS Topic."
    Exit
}

Try {
    IF ([string]::IsNullOrWhiteSpace($Regions)) { 
        [string[]]$regions = Get-AWSRegion -IncludeChina -IncludeGovCloud -ErrorAction Stop | Where-Object { $_.Region -notlike '*-iso*' } | Select-Object -ExpandProperty 'Region'
        $RegionsManuallyEntered = $false
    } Else {       
        [string[]]$regions = $Regions.split(',')
        $RegionsManuallyEntered = $true
    }
} Catch [System.Exception] {
    Write-Host "Failed to get Regions."
    Exit
}

ForEach ($region in $regions) {
    IF (IsValidRegion $region) {
        write-host "Processing: $region"
        Try {
            IF (!([string]::IsNullOrWhiteSpace($RdsInstances))) { 
                [string[]]$RdsInstances = $RdsInstances.split(',')
            } Else {
                [string[]]$rdsInstances = Get-RDSDBInstance -Region $region -Select DBInstances.DBInstanceIdentifier -ErrorAction Stop
            }

            ForEach ($rds in $rdsInstances) {

                Write-host "Processing RDS Instance:"$rds

                $MetricName = "AWS-RDS-Backup-Metric-$rds"
                $AlarmName = "AWS-RDS-Backup-Alarm-$rds"
                $LogName = "/aws/rds/instance/$rds/error"

                Try {
                    $RdsInstanceInfo = Get-RDSDBInstance -DBInstanceIdentifier $rds -region $region -ErrorAction Stop

                    IF ($RdsInstanceInfo.DBInstanceStatus -eq 'available') { 
                        Try {
                            $output = Edit-RDSDBInstance -DBInstanceIdentifier $rds -CloudwatchLogsExportConfiguration_EnableLogType 'error' -ApplyImmediately $true -Region $region -ErrorAction Stop
                            Write-Host " - Log Export (errorlog) to CloudWatch Enabled on $rds"
                        } Catch {
                            Write-Host "Errors Occured Setting Error Log Export on $rds."
                            Exit
                        }
                
                        Try {
                            IF ((Get-CWLLogGroup -LogGroupNamePattern $LogName -Region $region -ErrorAction Stop) -eq $null) {
                                $counter = 0
                                While ( ((Get-CWLLogGroup -LogGroupNamePattern $LogName -Region $region -ErrorAction Stop) -eq $null) -AND ($counter -ge 1) ) {
                                    Write-Host "Pausing 60 seconds for log group creation : $rds"
                                    Start-sleep 60
                                    $counter ++
                                }
                            }
                        } Catch {
                            Write-Host "Errors Occured Checking Error Log Export State on $rds."
                            Exit
                        }
                
                        IF ((Get-CWLLogGroup -LogGroupNamePattern $LogName -Region $region -ErrorAction Stop) -eq $null) {
                            Write-Host "Skipping $rds, Error Log group has not been published to CloudWatch yet, re-run for $rds after error log group is created."
                        } Else {
                            Try {
                                $metricTransformation = @{ 
                                    MetricName = $MetricName
                                    MetricNamespace = $Namespace
                                    MetricValue = "1"
                                }
                                Write-CWLMetricFilter -LogGroupName $LogName -FilterName $LogFilterName -FilterPattern $LogFilterPattern -MetricTransformation $metricTransformation -region $region -ErrorAction Stop
                                Write-Host " - Metric Filter $LogFilterName Created on $rds"
                            } Catch {
                                Write-Host "Errors Occured Creating Metric Filter on $rds."
                                Exit
                            }
                    
                            Try {
                                Write-CWMetricAlarm -AlarmName $AlarmName -AlarmDescription $AlarmDescription -MetricName $MetricName -Namespace $Namespace -Statistic Minimum -Period 60 -Threshold 0 -ComparisonOperator GreaterThanThreshold -EvaluationPeriod 1 -AlarmAction $SnsTopicARN -DatapointsToAlarm 1 -TreatMissingData "notBreaching" -region $region -ErrorAction Stop
                                Write-Host " - Metric Alarm $AlarmName Created on $rds"
                            } Catch {
                                Write-Host "Errors Occured Creating Metric Filter Alarm on $rds."
                                Exit
                            }
                        }
                    } Else {
                        Write-Host " - RDS instance $rds is not available for modifications."
                    }
                } Catch {
                    Write-Host "RDS instance $rds not found in region $region."
                }
            } 
        } Catch [System.Exception] {
            Write-host "Skipping: $region"
            $error.Clear()
        }
        $RdsInstances = $null
    } Else {
        IF ($RegionsManuallyEntered) { 
            Write-Host "Disabled or Invalid Region Specified: $region - Skipping"
        } Else {
            Write-Host "$Region not Enabled - Skipping"
        }
    } 
} 