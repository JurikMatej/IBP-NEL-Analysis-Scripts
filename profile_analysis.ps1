<#
    Profile the analyze.py script

    Use to gain insight on how long which metrics take to compute during the analysis run.
#>
param (
    [Parameter(Position=0,mandatory=$true)]
    [string]$script,

    [string]$out = 'profiling_result.prof',

    [bool]$show = $true
)

python -m cProfile -o $out $script

if ($show)
{
    snakeviz $out
}
