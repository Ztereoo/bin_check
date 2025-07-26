import aiohttp
import asyncio
import redis.asyncio as redis

API_NINJAS_KEY = "IW5xn5sImzBjUxP2OPf1TUwuGRlSJMsEHFJv38En"
REDIS_URL = "redis://localhost:6379"

EU_COUNTRIES_ALPHA2 = {
    "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "DE", "GR",
    "HU", "IE", "IT", "LV", "LT", "LU", "MT", "NL", "PL", "PT", "RO", "SK",
    "SI", "ES", "SE", "IS", "LI", "NO", "CH",
}

SERBIA_ALPHA2 = "RS"

async def get_country_with_bin(number: str, redis_client) -> str | None:
    """
    Retrieves the ISO alpha-2 country code for a given BIN.
    First checks Redis cache; if not found, fetches data from the API Ninjas BIN API.

    Args:
        number (str): The credit card number or BIN. Only the first 6 digits are used.
        redis_client: An instance of an asynchronous Redis client.

    Returns:
        str | None: The country alpha-2 code, "RATE_LIMIT", "HTTP_<status>", or error message string.
    """
    bin_number = number[:6]
    cache_key = f"bin_country:{bin_number}"
    cached = await redis_client.get(cache_key)
    if cached:
        return cached.decode()

    url = f"https://api.api-ninjas.com/v1/bin?bin={bin_number}"
    headers = {"X-Api-Key": API_NINJAS_KEY}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=5) as response:
                if response.status == 200:
                    data = await response.json()
                    if isinstance(data, list) and len(data) > 0:
                        country_code = data[0].get("iso_code2")
                        if country_code:
                            await redis_client.set(cache_key, country_code, ex=30 * 24 * 60 * 60)
                            return country_code
                elif response.status == 429:
                    return f"RATE_LIMIT"
                else:
                    return f"HTTP_{response.status}"
    except aiohttp.ClientError as e:
        return f"CLIENT_ERROR: {str(e)}"
    except asyncio.TimeoutError:
        return "TIMEOUT"

    return None

def classify_country_by_alpha2(country_alpha2: str | None) -> str:
    """
    Classifies the country into one of three categories: "EU", "RS" (Serbia), or "OTHER".
    Returns "UNKNOWN" if input is None or invalid.

    Args:
        country_alpha2 (str | None): ISO alpha-2 country code.

    Returns:
        str: One of the classification strings: "EU", "RS", "OTHER", or "UNKNOWN".
    """
    if not country_alpha2:
        return "UNKNOWN"
    if country_alpha2 == SERBIA_ALPHA2:
        return "RS"
    elif country_alpha2 in EU_COUNTRIES_ALPHA2:
        return "EU"
    else:
        return "OTHER"

async def main(bin_number: str):
    """
    Main function to get country code and classification for a BIN.

    Args:
        bin_number (str): BIN (first 6 digits of the card number).

    Returns:
        dict[str, str | None]: Dictionary containing:
            - "bin": original BIN string,
            - "country_alpha2": ISO alpha-2 country code or error message,
            - "classification": one of "EU", "RS", "OTHER", or "UNKNOWN".
    """
    redis_client = redis.from_url(REDIS_URL)
    country_alpha2 = await get_country_with_bin(bin_number, redis_client)
    classification = classify_country_by_alpha2(country_alpha2)
    await redis_client.aclose()
    return {
        "bin": bin_number,
        "country_alpha2": country_alpha2,
        "classification": classification,
    }
