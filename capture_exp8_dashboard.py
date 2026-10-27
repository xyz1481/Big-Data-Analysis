"""
Capture the Experiment 8 Streamlit dashboard as a PNG screenshot.
"""

import asyncio
from pathlib import Path

from playwright.async_api import async_playwright


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_FILE = BASE_DIR / "screenshots" / "exp8_01_dashboard_streamlit.png"
TARGET_URL = "http://127.0.0.1:8501"


async def main():
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        page = await browser.new_page(viewport={"width": 1600, "height": 4200}, device_scale_factor=1)
        await page.goto(TARGET_URL, wait_until="domcontentloaded")
        await page.wait_for_timeout(6000)
        await page.screenshot(path=str(OUTPUT_FILE), full_page=True)
        await browser.close()
    print(f"Saved screenshot: {OUTPUT_FILE}")


if __name__ == "__main__":
    asyncio.run(main())