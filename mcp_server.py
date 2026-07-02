import os
import httpx
import json
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP
from typing import Optional, Dict, Any

# Load environment variables
load_dotenv()

# Initialize FastMCP server
mcp = FastMCP("imcity_trading_bot_server")

# CMI credentials
CMI_URL = os.getenv("CMI_URL")
CMI_USERNAME = os.getenv("CMI_USERNAME_CHALLENGE") or os.getenv("CMI_USERNAME_TEST")
CMI_PASSWORD = os.getenv("CMI_PASSWORD_CHALLENGE") or os.getenv("CMI_PASSWORD_TEST")
AERODATABOX_KEY = os.getenv("AERODATABOX_KEY")

def get_auth_token() -> str:
    """Helper function to authenticate and get token from CMI"""
    if not CMI_URL or not CMI_USERNAME or not CMI_PASSWORD:
         raise ValueError("CMI credentials missing in .env")
         
    url = f"{CMI_URL.rstrip('/')}/api/user/authenticate"
    response = httpx.post(
        url,
        json={"username": CMI_USERNAME, "password": CMI_PASSWORD},
        headers={"Content-Type": "application/json; charset=utf-8"},
        timeout=10.0
    )
    response.raise_for_status()
    return response.headers["Authorization"]

@mcp.tool()
async def get_orderbook(product: str) -> Dict[str, Any]:
    """
    Fetch the live orderbook for a specific product.
    
    Args:
        product: Symbol of the product (e.g., 'TIDE_SPOT', 'WX_SPOT', 'LHR_COUNT', 'LON_ETF')
    """
    token = get_auth_token()
    url = f"{CMI_URL.rstrip('/')}/api/product/{product}/order-book/current-user"
    
    async with httpx.AsyncClient() as client:
        response = await client.get(
            url,
            headers={"Authorization": token, "Content-Type": "application/json; charset=utf-8"}
        )
        response.raise_for_status()
        return response.json()

@mcp.tool()
async def get_weather(past_steps: int = 96, forecast_steps: int = 96) -> Dict[str, Any]:
    """
    Fetch 15-min weather for London from Open-Meteo. 
    96 steps = 24 hours.
    
    Args:
        past_steps: Number of past 15-minute intervals to fetch
        forecast_steps: Number of future 15-minute intervals to fetch
    """
    LONDON_LAT, LONDON_LON = 51.5074, -0.1278
    variables = "temperature_2m,apparent_temperature,relative_humidity_2m,precipitation,wind_speed_10m,cloud_cover,visibility"
    
    async with httpx.AsyncClient() as client:
        response = await client.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": LONDON_LAT, 
                "longitude": LONDON_LON,
                "minutely_15": variables,
                "past_minutely_15": past_steps,
                "forecast_minutely_15": forecast_steps,
                "timezone": "Europe/London",
            }
        )
        response.raise_for_status()
        return response.json()

@mcp.tool()
async def get_thames_tides(limit: int = 200) -> Dict[str, Any]:
    """
    Fetch recent Thames tidal readings at Westminster from EA Flood Monitoring API.
    Levels are in metres Above Ordnance Datum (mAOD). 15 min intervals.
    
    Args:
        limit: Number of recent readings to fetch (use 400 for ~4 days history)
    """
    THAMES_MEASURE = "0006-level-tidal_level-i-15_min-mAOD"
    url = f"https://environment.data.gov.uk/flood-monitoring/id/measures/{THAMES_MEASURE}/readings"
    
    async with httpx.AsyncClient() as client:
        response = await client.get(
            url,
            params={"_sorted": "", "_limit": limit}
        )
        response.raise_for_status()
        return response.json()

@mcp.tool()
async def get_flights(
    offset_minutes: int = -360, 
    duration_minutes: int = 720,
    filters: Optional[Dict[str, bool]] = None
) -> Dict[str, Any]:
    """
    Fetch flights by relative time window for London Heathrow (LHR) via AeroDataBox.
    
    Args:
        offset_minutes: Start of window relative to now (negative = past)
        duration_minutes: Window length in minutes (max 720)
        filters: Optional dict of boolean query parameters
    """
    if not AERODATABOX_KEY:
        raise ValueError("AERODATABOX_KEY missing in .env")
        
    airport = "LHR"
    AERODATABOX_HOST = "aerodatabox.p.rapidapi.com"
    
    # Construct query params
    query = f"?offsetMinutes={offset_minutes}&durationMinutes={duration_minutes}&direction=Both"
    if filters:
        for k, v in filters.items():
            query += f"&{k}={'true' if v else 'false'}"
            
    url = f"https://{AERODATABOX_HOST}/flights/airports/iata/{airport}{query}"
    
    async with httpx.AsyncClient() as client:
        response = await client.get(
            url,
            headers={
                "x-rapidapi-host": AERODATABOX_HOST, 
                "x-rapidapi-key": AERODATABOX_KEY
            }
        )
        response.raise_for_status()
        return response.json()

if __name__ == "__main__":
    mcp.run()
