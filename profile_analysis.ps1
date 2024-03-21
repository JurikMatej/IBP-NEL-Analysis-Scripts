<#
    Profile the analyze.py script

    Use to gain insight on how long which metrics take to compute during the analysis run.
#>
param (
    [string]$out = 'analyze.prof',

    [bool]$show = $true
)

python -m cProfile -o $out .\analyze.py

if ($show)
{
    snakeviz $out
}
