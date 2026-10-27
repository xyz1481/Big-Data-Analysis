"""
Render the saved Experiment 7 terminal output into a PNG screenshot.
"""

import os
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


BASE_DIR = Path(__file__).resolve().parent
TEXT_FILE = BASE_DIR / "output" / "exp7_demo_output.txt"
OUTPUT_FILE = BASE_DIR / "screenshots" / "exp7_01_streaming_demo_terminal.png"
FONT_PATH = os.path.join(os.environ.get("WINDIR", "C:\\Windows"), "Fonts", "consola.ttf")


def get_font(size=15):
    try:
        return ImageFont.truetype(FONT_PATH, size)
    except Exception:
        return ImageFont.load_default()


def render_terminal_screenshot(title, text_lines, output_file, width=1100):
    font = get_font(14)
    title_font = get_font(13)

    clean_lines = []
    for line in text_lines:
        line_clean = line.rstrip()
        if not line_clean:
            clean_lines.append("")
            continue
        clean_lines.append(line_clean)

    max_chars_per_line = 120
    wrapped_lines = []
    for line in clean_lines:
        if len(line) > max_chars_per_line:
            wrapped_lines.append(line[:max_chars_per_line] + "...")
        else:
            wrapped_lines.append(line)

    line_height = 20
    header_height = 42
    padding = 24
    height = header_height + (len(wrapped_lines) * line_height) + (padding * 2)

    img = Image.new("RGB", (width, height), color="#1E1E2E")
    draw = ImageDraw.Draw(img)

    draw.rectangle([(0, 0), (width, header_height)], fill="#252538")
    dot_radius = 6
    dot_y = header_height // 2
    draw.ellipse([(18 - dot_radius, dot_y - dot_radius), (18 + dot_radius, dot_y + dot_radius)], fill="#FF5F56")
    draw.ellipse([(38 - dot_radius, dot_y - dot_radius), (38 + dot_radius, dot_y + dot_radius)], fill="#FFBD2E")
    draw.ellipse([(58 - dot_radius, dot_y - dot_radius), (58 + dot_radius, dot_y + dot_radius)], fill="#27C93F")
    draw.text((85, 12), f"Terminal - {title}", fill="#A6ADC8", font=title_font)

    y_cursor = header_height + padding
    for line in wrapped_lines:
        text_color = "#CDD6F4"
        if line.startswith("===") or line.startswith(">>>") or line.startswith("---"):
            text_color = "#89B4FA"
        elif "[+]" in line or "[RESULT]" in line:
            text_color = "#A6E3A1"
        elif "[DEMO]" in line:
            text_color = "#94E2D5"
        draw.text((padding, y_cursor), line, fill=text_color, font=font)
        y_cursor += line_height

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    img.save(output_file, "PNG", optimize=True)
    print(f"Saved screenshot: {output_file}")


def main():
    if not TEXT_FILE.exists():
        raise FileNotFoundError(f"Missing terminal text file: {TEXT_FILE}")

    try:
        with open(TEXT_FILE, "r", encoding="utf-8") as handle:
            text_lines = handle.read().splitlines()
    except UnicodeDecodeError:
        with open(TEXT_FILE, "r", encoding="utf-16") as handle:
            text_lines = handle.read().splitlines()

    render_terminal_screenshot("Experiment 7 - Streaming Demo Terminal", text_lines, OUTPUT_FILE)


if __name__ == "__main__":
    main()