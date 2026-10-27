Add-Type -AssemblyName System.Drawing

$logPath = "C:\Users\prati\OneDrive\Desktop\BDA\Big-Data-Analysis\output\exp9_terminal_run.txt"
$outPath = "C:\Users\prati\OneDrive\Desktop\BDA\Big-Data-Analysis\output\exp9_terminal_screenshot.png"

if (-not (Test-Path $logPath)) {
    throw "Missing log file: $logPath"
}

$text = Get-Content -Path $logPath -Raw
$lines = $text -split "`r?`n"
$font = New-Object System.Drawing.Font("Consolas", 12, [System.Drawing.FontStyle]::Regular)
$imgWidth = 1800
$imgHeight = [Math]::Max(900, (($lines.Count + 5) * 18) + 30)
$bitmap = New-Object System.Drawing.Bitmap($imgWidth, $imgHeight)
$graphics = [System.Drawing.Graphics]::FromImage($bitmap)
$graphics.Clear([System.Drawing.Color]::Black)
$brush = [System.Drawing.Brushes]::White
$y = 20
foreach ($line in $lines) {
    if ($line.Length -gt 0) {
        $graphics.DrawString($line, $font, $brush, 20, $y)
    }
    $y += 18
}
$bitmap.Save($outPath, [System.Drawing.Imaging.ImageFormat]::Png)
$graphics.Dispose()
$bitmap.Dispose()
Write-Host "Saved PNG: $outPath"
