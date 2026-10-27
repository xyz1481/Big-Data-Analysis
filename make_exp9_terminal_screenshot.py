from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

log_path = Path(r"C:\Users\prati\OneDrive\Desktop\BDA\Big-Data-Analysis\output\exp9_terminal_run.txt")
out_path = Path(r"C:\Users\prati\OneDrive\Desktop\BDA\Big-Data-Analysis\output\exp9_terminal_screenshot.png")

if not log_path.exists():
    raise FileNotFoundError(f"Missing log file: {log_path}")

text = log_path.read_text(encoding="utf-8", errors="replace")
lines = text.splitlines()

font_path = None
for candidate in [
    r"C:\Windows\Fonts\consola.ttf",
    r"C:\Windows\Fonts\lucon.ttf",
    r"C:\Windows\Fonts\cour.ttf",
    r"C:\Windows\Fonts\courbd.ttf",
]:
    if Path(candidate).exists():
        font_path = candidate
        break

font = ImageFont.truetype(font_path or "DejaVuSansMono.ttf", 15) if font_path else ImageFont.load_default()
max_len = max((len(line) for line in lines), default=80)
width = max(1200, min(2200, max_len * 9 + 60))
height = max(700, len(lines) * 18 + 60)
img = Image.new("RGB", (width, height), "black")
draw = ImageDraw.Draw(img)
for i, line in enumerate(lines[:200]):
    draw.text((20, 20 + i * 18), line, fill="white", font=font)
img.save(out_path)
print(f"Saved PNG: {out_path}")
