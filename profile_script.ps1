<#
    Profile the provided script
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
