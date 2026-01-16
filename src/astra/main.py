"""Main FastAPI application for Astra observatory automation system.

This module provides a web-based interface for controlling and monitoring multiple
astronomical observatories. It handles real-time device status updates, schedule
management, image display, logging, and WebSocket communications for the observatory
control system.

Key features:
    - Multi-observatory support with device monitoring
    - Real-time WebSocket updates for device status
    - Schedule upload and editing capabilities
    - Image conversion and display (FITS to JPEG)
    - Database logging and telemetry storage
    - Safety monitoring and robotic operation control
"""

import asyncio
import datetime
import json
import logging
import sqlite3
import time
from contextlib import asynccontextmanager
from datetime import UTC
from io import BytesIO
from pathlib import Path

import httpx
import numpy as np
import pandas as pd
import uvicorn
from astropy.coordinates import AltAz, EarthLocation, get_body, get_sun
from astropy.io import fits
from astropy.time import Time
from astropy.visualization import ZScaleInterval
from fastapi import Body, FastAPI, File, Request, UploadFile, WebSocket
from fastapi.responses import (
    FileResponse,
    HTMLResponse,
    RedirectResponse,
    StreamingResponse,
)
from fastapi.templating import Jinja2Templates
from PIL import Image

from astra import Config, __version__
from astra.frontend.file_explorer.file_explorer import include_file_explorer
from astra.image_handler import HeaderManager
from astra.logger import ConsoleStreamHandler, FileHandler
from astra.observatory import Observatory
from astra.observatory_loader import ObservatoryLoader
from astra.paired_devices import PairedDevices

pd.set_option("future.no_silent_downcasting", True)

# Configure our module + noisy third-party loggers with the project's
# `ConsoleStreamHandler` and `CustomFormatter` to keep formatting consistent
# for pre-uvicorn startup messages without modifying the root logger.
# global variables
logger = logging.getLogger(__name__)
ConsoleStreamHandler.attach(logger)
ConsoleStreamHandler.attach(logging.getLogger("httpx"), level=logging.WARNING)
ConsoleStreamHandler.attach(logging.getLogger("astropy"), remove_other_handlers=True)

FRONTEND_PATH = Path(__file__).parent / "frontend"
OBSERVATORY: Observatory = None  # type: ignore
WEBCAMFEED = {}
FWS = {}
DEBUG = False
FRONTEND = Jinja2Templates(directory=FRONTEND_PATH)
LAST_IMAGE = None
LAST_IMAGE_PREVIEW = None  # Stores (jpeg_bytes, headers) tuple
LAST_IMAGE_TIME = None
TRUNCATE_FACTOR = None
CUSTOM_OBSERVATORY = None

# Twilight calculation cache: stores (timestamp, start_time, end_time, periods)
TWILIGHT_CACHE = None
TWILIGHT_CACHE_TIME = None

# Celestial data cache: stores celestial body positions for sky projection
CELESTIAL_CACHE = None
CELESTIAL_CACHE_TIME = None

# Polling data cache: stores { (device_type, day): (timestamp, result) }
POLLING_CACHE = {}


def observatory_db() -> sqlite3.Connection:
    """Get database connection for observatory logging.

    Args:
        name (str): Observatory name for database file.

    Returns:
        sqlite3.Connection: Database connection object.
    """
    db = sqlite3.connect(Config().paths.logs / f"{Config().observatory_name}.db")
    return db


def load_observatories() -> None:
    """Load and initialize all observatory configurations.

    Discovers observatory config files, creates Observatory instances,
    establishes device connections, and sets up filter wheel mappings.
    Updates global OBSERVATORY, WEBCAMFEED, and FWS dictionaries.
    """
    global OBSERVATORY  # not sure if this is necessary
    global WEBCAMFEED
    global FWS

    config_file = (
        Config().paths.observatory_config / f"{Config().observatory_name}_config.yml"
    )
    if CUSTOM_OBSERVATORY:
        observatory_class = ObservatoryLoader(
            observatory_name=CUSTOM_OBSERVATORY
        ).load()
        logger.info(f"Selected custom observatory class: {observatory_class.__name__}")
    else:
        observatory_class = Observatory

    obs = observatory_class(
        config_file,
        TRUNCATE_FACTOR,
        logging_level=logging.DEBUG if DEBUG else logging.INFO,
    )
    OBSERVATORY = obs

    if "Misc" in obs.config:
        if "Webcam" in obs.config["Misc"]:
            WEBCAMFEED = obs.config["Misc"]["Webcam"]

    obs.connect_all_devices()

    if "FilterWheel" in obs.devices:
        FWS = {}
        for fw_name in obs.devices["FilterWheel"].keys():
            filter_names = obs.devices["FilterWheel"][fw_name].get("Names")
            obs.logger.info(f"FilterWheel {fw_name} has filters: {filter_names}")
            FWS[fw_name] = obs.devices["FilterWheel"][fw_name].get("Names")


def clean_up() -> None:
    """Clean up and stop all observatory devices before shutdown.

    Iterates through all observatories and device types to safely
    stop all connected devices. Handles exceptions during shutdown.
    """
    obs = OBSERVATORY
    # Get all the devices
    for device_type in obs.devices:
        for device_name in obs.devices[device_type]:
            # Get the device
            device = obs.devices[device_type][device_name]
            # Stop the device
            try:
                # logging.info(f"Stopping device {device_name}")
                device.stop()
            except Exception as e:
                logger.error(f"Error stopping device {device_name}: {e}", exc_info=True)

    logger.info("Exiting clean_up")


def format_time(ftime: datetime.datetime) -> str | None:
    """Format datetime object to HH:MM:SS string.

    Args:
        ftime (datetime.datetime): Datetime object to format.

    Returns:
        str | None: Formatted time string or None if formatting fails.
    """
    # if ftime is not NaTType:
    try:
        return ftime.strftime("%H:%M:%S")
    except Exception:
        return None


def convert_fits_to_preview(fits_file: str) -> tuple[bytes, dict]:
    """Convert FITS astronomical image to JPEG bytes for web display.

    Opens FITS file, extracts image data and headers, applies Z-scale
    normalization, and returns JPEG as bytes (no disk I/O).

    Args:
        fits_file (str): Path to FITS file to convert.

    Returns:
        tuple[bytes, dict]: JPEG image bytes and extracted FITS headers.
    """
    headers = {}
    with fits.open(fits_file) as hdulist:
        image_data = hdulist[0].data  # type: ignore

        # Bin image data if larger than limit
        h, w = image_data.shape
        limit = 1024
        if h > limit or w > limit:
            bin_factor = max(int(np.ceil(h / limit)), int(np.ceil(w / limit)))
            new_h = (h // bin_factor) * bin_factor
            new_w = (w // bin_factor) * bin_factor
            data_trimmed = image_data[:new_h, :new_w]
            image_data = data_trimmed.reshape(
                new_h // bin_factor, bin_factor, new_w // bin_factor, bin_factor
            ).mean(axis=(1, 3))

        for key in ["EXPTIME", "DATE-OBS", "FILTER", "IMAGETYP"]:
            headers[key] = hdulist[0].header[key]  # type: ignore
        if headers["IMAGETYP"] == "Light":
            headers["OBJECT"] = hdulist[0].header["OBJECT"]  # type: ignore

    # Apply Z-scale normalization and convert to 8-bit
    interval = ZScaleInterval(contrast=0.005)
    vmin, vmax = interval.get_limits(image_data)
    image_data = np.clip((image_data - vmin) / (vmax - vmin) * 255, 0, 255).astype(
        np.uint8
    )

    # Convert to JPEG bytes using Pillow (in-memory, no disk I/O)
    img = Image.fromarray(image_data, mode="L")
    buffer = BytesIO()
    img.save(buffer, format="JPEG", quality=85)
    buffer.seek(0)

    return buffer.getvalue(), headers


@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan context manager for startup and shutdown.

    Handles application startup (loading observatories) and shutdown
    (cleaning up device connections) lifecycle events.

    Args:
        app (FastAPI): FastAPI application instance.

    Yields:
        None: Application runs between yield statements.
    """
    # Load observatories
    load_observatories()
    yield
    # Clean up
    clean_up()


app = FastAPI(lifespan=lifespan)

try:
    include_file_explorer(
        app,
        fits_dir=Config().paths.images,
        prefix="/fits_explorer",
        static_url="/fits_explorer/static",
        fits_url="/fits",
    )
    logger.debug("Registered file explorer at /fits_explorer")
except Exception as e:
    logger.error(f"Failed to register file explorer: {e}", exc_info=True)


@app.get("/video/{filename:path}", include_in_schema=False)
async def get_video(request: Request, filename: str):
    """Proxy video streams from observatory webcams.

    Forwards HTTP requests to webcam feeds, handling both MP4 video
    streams and HTML content with appropriate media types.

    Args:
        request (Request): FastAPI request object with headers.
        filename (str): Video filename or path to stream.

    Returns:
        StreamingResponse: Proxied video content with appropriate headers.
    """
    headers = request.headers
    base_url = WEBCAMFEED
    target_url = f"{base_url}/{filename}"

    async with httpx.AsyncClient() as client:
        response = await client.get(target_url, headers=headers)
        content = response.content
        status_code = response.status_code
        headers = response.headers

    if filename.endswith(".mp4"):
        return StreamingResponse(
            BytesIO(content),
            status_code=status_code,
            headers=headers,
            media_type="video/mp4",
        )
    else:
        return HTMLResponse(content, status_code=status_code, headers=headers)


@app.get("/api/heartbeat")
async def heartbeat():
    """Get observatory heartbeat status for health monitoring.

    Returns:
        dict: JSON response with heartbeat status data.
    """
    obs = OBSERVATORY

    return {"status": "success", "data": obs.heartbeat, "message": ""}


@app.get("/api/latest_image_preview")
async def latest_image_preview():
    """Serve the latest FITS image as a JPEG preview.

    Returns the most recent observatory image converted to JPEG format,
    generated in-memory without disk I/O.

    Returns:
        StreamingResponse: JPEG image data with appropriate headers.
    """
    if LAST_IMAGE_PREVIEW is None:
        return HTMLResponse(status_code=404, content="No image available")

    jpeg_bytes, headers = LAST_IMAGE_PREVIEW
    return StreamingResponse(
        BytesIO(jpeg_bytes),
        media_type="image/jpeg",
        headers={
            "Cache-Control": "no-cache",
            "X-Image-Timestamp": LAST_IMAGE_TIME.isoformat() if LAST_IMAGE_TIME else "",
        },
    )


@app.post("/api/close")
def close_observatory():
    """Close observatory and stop all operations safely.

    Stops running schedule if active and closes the observatory.
    Logs all actions for audit trail.

    Returns:
        dict: JSON response with operation status.
    """
    obs = OBSERVATORY

    obs.logger.info("User initiated closing of observatory from web interface")

    if obs.schedule_manager.running:
        obs.logger.info("Stopping schedule for safety.")
        obs.schedule_manager.stop_schedule(obs.thread_manager)

    val = obs.close_observatory()

    if val:
        obs.logger.info("Observatory closed.")

    return {"status": "success", "data": "null", "message": ""}


@app.post("/api/cool_camera/{device_name}")
def cool_camera(device_name: str):
    """Initiate camera cooling to configured target temperature.

    Gets camera configuration and starts cooling process to the
    specified set temperature with defined tolerance.

    Args:
        device_name (str): Camera device name to cool.

    Returns:
        dict: JSON response with operation status and cooling details.
    """
    obs = OBSERVATORY

    paired_devices = PairedDevices.from_observatory(
        observatory=obs,
        camera_name=device_name,
    )
    camera_config = paired_devices.get_device_config("Camera")

    set_temperature = camera_config["temperature"]
    temperature_tolerance = camera_config["temperature_tolerance"]
    cooling_timeout = camera_config.get("cooling_timeout", 30)

    obs.logger.info(f"User initiated cooling of {device_name} from web interface")

    camera = obs.devices["Camera"][device_name]

    poll_data = camera.poll_latest()
    if poll_data is not None and "CCDTemperature" in poll_data:
        current_temperature = poll_data["CCDTemperature"]["value"]
        obs.logger.info(
            f"Current camera temperature: {current_temperature}C, Set temperature: {set_temperature}C"
        )
    else:
        obs.logger.info(
            f"Camera temperature unavailable, cooling to set temperature: {set_temperature}C"
        )

    obs.cool_camera(
        device_name=device_name,
        set_temperature=set_temperature,
        temperature_tolerance=temperature_tolerance,
        cooling_timeout=cooling_timeout,
    )

    return {"status": "success", "data": "null", "message": ""}


@app.post("/api/complete_headers")
def complete_headers():
    """Complete FITS header processing for observatory images.

    Returns:
        dict: JSON response with operation status.
    """
    obs = OBSERVATORY

    obs.logger.info("User initiated completion of headers from web interface")

    HeaderManager.final_headers(
        obs.database_manager,
        obs.logger,
        obs.config,
        obs.devices,
        obs.fits_config,
    )

    return {"status": "success", "data": "null", "message": ""}


@app.post("/api/startwatchdog")
async def start_watchdog():
    """Start observatory watchdog monitoring system.

    Resets error states and starts the watchdog process for
    continuous observatory health monitoring.

    Returns:
        dict: JSON response with operation status.
    """
    obs = OBSERVATORY

    obs.logger.info("User initiated starting of watchdog from web interface")

    obs.logger.error_free = True
    obs.logger.error_source = []
    obs.start_watchdog()

    return {"status": "success", "data": "null", "message": ""}


@app.post("/api/stopwatchdog")
async def stop_watchdog():
    """Stop observatory watchdog monitoring system.

    Returns:
        dict: JSON response with operation status.
    """
    obs = OBSERVATORY

    obs.logger.info("User initiated stopping of watchdog from web interface")

    obs.watchdog_running = False

    return {"status": "success", "data": "null", "message": ""}


@app.post("/api/roboticswitch")
async def roboticswitch():
    """Toggle observatory robotic operation mode.

    Returns:
        dict: JSON response with current robotic switch state.
    """
    obs = OBSERVATORY

    obs.logger.info("User initiated robotic switch from web interface")

    obs.toggle_robotic_switch()

    return {"status": "success", "data": obs.robotic_switch, "message": ""}


@app.post("/api/startschedule")
async def start_schedule():
    """Start executing the observatory's observation schedule.

    Returns:
        dict: JSON response with operation status.
    """
    obs = OBSERVATORY

    obs.logger.info("User initiated starting of schedule from web interface")

    obs.start_schedule()

    return {"status": "success", "data": "null", "message": ""}


@app.post("/api/stopschedule")
async def stop_schedule():
    """Stop executing the observatory's observation schedule.

    Returns:
        dict: JSON response with operation status.
    """
    obs = OBSERVATORY

    obs.logger.info("User initiated stopping of schedule from web interface")

    obs.schedule_manager.stop_schedule(obs.thread_manager)

    return {"status": "success", "data": "null", "message": ""}


@app.get("/api/schedule")
async def schedule():
    """Get current observatory schedule with formatted times.

    Returns:
        list: Schedule items with start/end times formatted as HH:MM:SS,
              or empty list if no schedule exists.
    """
    obs = OBSERVATORY
    if (
        obs is None
        or not hasattr(obs, "schedule_manager")
        or obs.schedule_manager is None
    ):
        logger.warning(
            "Schedule request but OBSERVATORY not initialized or has no schedule_manager"
        )
        return []

    if getattr(obs.schedule_manager, "schedule_mtime", 0) == 0:
        return []

    try:
        schedule_obj = obs.schedule_manager.get_schedule()
        schedule = schedule_obj.to_dataframe()

        # Add formatted time columns
        schedule["start_HHMMSS"] = pd.to_datetime(
            schedule["start_time"], errors="coerce"
        ).apply(lambda x: x.strftime("%H:%M:%S") if pd.notna(x) else "")
        schedule["end_HHMMSS"] = pd.to_datetime(
            schedule["end_time"], errors="coerce"
        ).apply(lambda x: x.strftime("%H:%M:%S") if pd.notna(x) else "")
        obs.logger.debug("Schedule read for frontend")
        result = schedule.to_dict(orient="records")

        return result

    except Exception as e:
        obs.logger.warning(f"Error reading schedule for frontend: {e}", exc_info=True)
        return []


@app.post("/api/editschedule")
async def edit_schedule(schedule_data: str = Body(..., media_type="text/plain")):
    """Update observatory schedule from web editor.

    Parses JSONL schedule data and saves to observatory schedule file.

    Args:
        schedule_data (str): JSONL formatted schedule data.

    Returns:
        dict: Status response with success/error information.
    """
    obs = OBSERVATORY

    schedule_path = obs.schedule_manager.schedule_path

    try:
        # Parse the JSONL data
        lines = schedule_data.strip().split("\n")
        schedule_items = []
        for line in lines:
            if line.strip():
                schedule_items.append(json.loads(line.strip()))

        # Convert to DataFrame and save as JSONL
        df = pd.DataFrame(schedule_items)
        df.to_json(schedule_path, orient="records", lines=True)

        obs.logger.info(
            f"Schedule updated with {len(schedule_items)} items from editor"
        )

        return {
            "status": "success",
            "data": None,
            "message": f"Schedule updated with {len(schedule_items)} items",
        }

    except Exception as e:
        obs.logger.error(f"Error updating schedule: {e}", exc_info=True)
        return {
            "status": "error",
            "data": None,
            "message": f"Error updating schedule: {str(e)}",
        }


@app.post("/api/uploadschedule")
async def upload_schedule(file: UploadFile = File(...)):
    """Upload schedule file to replace current observatory schedule.

    Args:
        file (UploadFile): Uploaded schedule file in JSONL format.

    Returns:
        dict: Upload status response with success/error information.
    """
    obs = OBSERVATORY

    try:
        # Save the uploaded file
        file_path = obs.schedule_manager.schedule_path
        with open(file_path, "wb") as f:
            f.write(await file.read())

        obs.logger.info("Schedule uploaded from web interface")

        return {
            "status": "success",
            "data": None,
            "message": "Schedule uploaded successfully",
        }
    except Exception as e:
        obs.logger.warning(f"Error uploading schedule: {e}")
        return {
            "status": "error",
            "data": None,
            "message": f"Error uploading schedule: {str(e)}",
        }


def calculate_twilight_periods(
    start_time: datetime.datetime,
    end_time: datetime.datetime,
    obs_location: EarthLocation,
) -> list[dict]:
    """Calculate twilight periods for the given time range.

    Args:
        start_time: Start of time range (UTC)
        end_time: End of time range (UTC)
        obs_location: Observatory location

    Returns:
        List of period dictionaries with start, end, and phase
    """
    global TWILIGHT_CACHE, TWILIGHT_CACHE_TIME

    # Check cache (valid for 1 minute)
    if TWILIGHT_CACHE is not None and TWILIGHT_CACHE_TIME is not None:
        cache_age = (datetime.datetime.now(UTC) - TWILIGHT_CACHE_TIME).total_seconds()
        if cache_age < 60:
            return TWILIGHT_CACHE

    # Calculate sun altitudes at regular intervals
    time_points = pd.date_range(start=start_time, end=end_time, freq="5min")
    times = Time(time_points)

    sun = get_sun(times)
    altaz_frame = AltAz(obstime=times, location=obs_location)
    sun_altaz = sun.transform_to(altaz_frame)
    altitudes = sun_altaz.alt.degree  # type: ignore

    periods = []
    current_phase = None
    period_start = None

    for i, (time_point, altitude) in enumerate(zip(time_points, altitudes)):  # type: ignore
        # Determine phase based on sun altitude
        if altitude >= 0:
            phase = "day"
        elif altitude >= -6:
            phase = "civil"
        elif altitude >= -12:
            phase = "nautical"
        elif altitude >= -18:
            phase = "astronomical"
        else:
            phase = "night"

        # Detect phase changes
        if phase != current_phase:
            if current_phase is not None and period_start is not None:
                # Save previous period
                periods.append(
                    {
                        "start": period_start.isoformat(),
                        "end": time_point.isoformat(),
                        "phase": current_phase,
                    }
                )
            period_start = time_point
            current_phase = phase

    # Add final period
    if current_phase is not None and period_start is not None:
        periods.append(
            {
                "start": period_start.isoformat(),
                "end": time_points[-1].isoformat(),
                "phase": current_phase,
            }
        )

    # Cache result with current timestamp
    TWILIGHT_CACHE = periods
    TWILIGHT_CACHE_TIME = datetime.datetime.now(UTC)

    return periods


def calculate_celestial_data(obs_location: EarthLocation) -> dict:
    """Calculate positions of celestial objects for all-sky projection.

    Args:
        obs_location: Observatory location as EarthLocation

    Returns:
        Dictionary with observatory info, UTC time, and celestial body data
    """
    global CELESTIAL_CACHE, CELESTIAL_CACHE_TIME

    # Check cache (valid for 1 minute)
    if CELESTIAL_CACHE is not None and CELESTIAL_CACHE_TIME is not None:
        cache_age = (datetime.datetime.now(UTC) - CELESTIAL_CACHE_TIME).total_seconds()
        if cache_age < 60:
            return CELESTIAL_CACHE

    current_time = Time.now()
    altaz_frame = AltAz(obstime=current_time, location=obs_location)

    celestial_bodies = []

    # Sun
    try:
        sun = get_sun(current_time)
        sun_altaz = sun.transform_to(altaz_frame)
        celestial_bodies.append(
            {
                "name": "Sun",
                "alt": float(sun_altaz.alt.degree),  # type: ignore
                "az": float(sun_altaz.az.degree),  # type: ignore
                "type": "sun",
                "magnitude": -26.74,
            }
        )
    except Exception as e:
        logger.warning(f"Error calculating sun position: {e}")

    # Moon
    try:
        moon = get_body("moon", current_time)
        moon_altaz = moon.transform_to(altaz_frame)

        # Calculate moon phase (illumination fraction)
        sun = get_sun(current_time)
        elongation = sun.separation(moon).degree
        phase = (1 - np.cos(np.radians(elongation))) / 2  # 0=new, 1=full

        celestial_bodies.append(
            {
                "name": "Moon",
                "alt": float(moon_altaz.alt.degree),  # type: ignore
                "az": float(moon_altaz.az.degree),  # type: ignore
                "type": "moon",
                "magnitude": -12.0,  # Approximate full moon magnitude
                "phase": float(phase),  # Illumination fraction 0-1
            }
        )
    except Exception as e:
        logger.warning(f"Error calculating moon position: {e}")

    # Planets
    planets = {
        "mercury": ("Mercury", -1.9),
        "venus": ("Venus", -4.4),
        "mars": ("Mars", -2.9),
        "jupiter": ("Jupiter", -2.9),
        "saturn": ("Saturn", 0.0),
    }

    for planet_key, (planet_name, magnitude) in planets.items():
        try:
            planet = get_body(planet_key, current_time)
            planet_altaz = planet.transform_to(altaz_frame)
            celestial_bodies.append(
                {
                    "name": planet_name,
                    "alt": float(planet_altaz.alt.degree),  # type: ignore
                    "az": float(planet_altaz.az.degree),  # type: ignore
                    "type": "planet",
                    "magnitude": magnitude,
                }
            )
        except Exception as e:
            logger.warning(f"Error calculating {planet_name} position: {e}")

    result = {
        "observatory": {
            "lat": float(obs_location.lat.degree),
            "lon": float(obs_location.lon.degree),
            "elevation": float(obs_location.height.value),
        },
        "utc_time": current_time.iso,
        "celestial_bodies": celestial_bodies,
    }

    # Cache result
    CELESTIAL_CACHE = result
    CELESTIAL_CACHE_TIME = datetime.datetime.now(UTC)

    return result


@app.get("/api/sky_data")
async def sky_data():
    """Get celestial body positions for all-sky projection.

    Returns:
        dict: JSON response with observatory location, time, and celestial body positions
    """
    obs = OBSERVATORY

    try:
        obs_location = obs.get_observatory_location()
        data = calculate_celestial_data(obs_location)  # type: ignore
        return {"status": "success", "data": data, "message": ""}
    except Exception as e:
        logger.warning(f"Error calculating sky data: {e}", exc_info=True)
        return {
            "status": "error",
            "data": None,
            "message": f"Error calculating sky data: {str(e)}",
        }


@app.get("/api/db/polling/{device_type}")
async def polling(device_type: str, day: float = 1, since: str | None = None):
    """Get device polling data from observatory database.

    Retrieves and processes telemetry data for specific device types,
    including pivot formatting, safety limits, and statistical grouping.

    Args:
        device_type (str): Type of device (e.g., 'ObservingConditions').
        day (float): Number of days back to retrieve data. Defaults to 1.
        since (str): Optional timestamp to get only newer records.

    Returns:
        dict: Processed polling data with safety limits and latest values.
    """
    # Check cache for full queries (since is None)
    if since is None:
        cache_key = (device_type, day)
        if cache_key in POLLING_CACHE:
            cache_time, cache_result = POLLING_CACHE[cache_key]
            if (datetime.datetime.now(UTC) - cache_time).total_seconds() < 60:
                return cache_result

    db = observatory_db()
    obs = OBSERVATORY

    if since is not None:
        # Only fetch new records since the given timestamp
        q = f"""SELECT * FROM polling WHERE device_type = '{device_type}' AND datetime > '{since}'"""
    else:
        q = f"""SELECT * FROM polling WHERE device_type = '{device_type}' AND datetime > datetime('now', '-{day} day')"""

    df = pd.read_sql_query(q, db)

    if device_type == "ObservingConditions" and "SafetyMonitor" in obs.config:
        # Also get SafetyMonitor data
        if since is not None:
            q_isSafe = f"""SELECT * FROM polling WHERE device_type = 'SafetyMonitor' AND datetime > '{since}'"""
        else:
            q_isSafe = f"""SELECT * FROM polling WHERE device_type = 'SafetyMonitor' AND datetime > datetime('now', '-{day} day')"""

        df_isSafe = pd.read_sql_query(q_isSafe, db)

        # Append isSafe to df
        if not df_isSafe.empty:
            df = pd.concat([df, df_isSafe], ignore_index=True)

    db.close()

    # Pivot: datetime as index, device_command as columns
    df = df.pivot(index="datetime", columns="device_command", values="device_value")

    # Ensure datetime index and numeric values
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()
    # Convert boolean strings/values to 1/0 before numeric conversion
    df = df.replace({"True": 1, "False": 0, True: 1, False: 0})
    df = df.apply(pd.to_numeric, errors="coerce")

    # Latest values
    latest = {}
    for col in df.columns:
        latest[col] = df[col].dropna().iloc[-1] if not df[col].dropna().empty else None

    if "SkyTemperature" in latest and "Temperature" in latest:
        latest["RelativeSkyTemp"] = latest["SkyTemperature"] - latest["Temperature"]

    # Group by 60s
    df_groupby = df.groupby(pd.Grouper(freq="60s")).mean()
    df_groupby = df_groupby.dropna()

    # Add RelativeSkyTemp = SkyTemperature - Temperature
    if "SkyTemperature" in df_groupby.columns and "Temperature" in df_groupby.columns:
        df_groupby["RelativeSkyTemp"] = (
            df_groupby["SkyTemperature"] - df_groupby["Temperature"]
        )

    if device_type == "ObservingConditions" and "ObservingConditions" in obs.config:
        # Safety limits
        closing_limits = obs.config["ObservingConditions"][0]["closing_limits"]
        safety_limits = {}

        for key in closing_limits:
            upper_val = float("inf")
            lower_val = float("-inf")
            for item in closing_limits[key]:
                if item.get("upper", float("inf")) < upper_val:
                    upper_val = item["upper"]
                if item.get("lower", float("-inf")) > lower_val:
                    lower_val = item["lower"]

            safety_limits[key] = {
                "upper": upper_val if upper_val != float("inf") else None,
                "lower": lower_val if lower_val != float("-inf") else None,
            }

        # Calculate twilight periods if we have telescope location
        twilight_periods = []
        if "Telescope" in obs.devices:
            try:
                obs_location = obs.get_observatory_location()

                # Always calculate twilight for 3 days, regardless of data range
                end_time = datetime.datetime.now(UTC)
                start_time = end_time - datetime.timedelta(days=3)

                twilight_periods = calculate_twilight_periods(
                    start_time,
                    end_time,
                    obs_location,  # type: ignore
                )
            except Exception as e:
                logger.warning(f"Error calculating twilight periods: {e}")

        result = {
            "data": df_groupby.reset_index().to_dict(orient="records"),
            "safety_limits": safety_limits,
            "latest": latest,
            "twilight_periods": twilight_periods,
        }
    else:
        result = {
            "data": df_groupby.reset_index().to_dict(orient="records"),
            "latest": latest,
        }

    if since is None:
        POLLING_CACHE[(device_type, day)] = (datetime.datetime.now(UTC), result)

    return result


@app.get("/api/db/guiding")
async def guiding_data(
    day: float = 1, since: str | None = None, telescope: str | None = None
):
    """Get autoguider log data for plotting guiding performance.

    Retrieves guiding corrections (post_pid_x, post_pid_y) from the
    autoguider_log table for visualization.

    Args:
        day (float): Number of days back to retrieve data. Defaults to 1.
        since (str): Optional timestamp to get only newer records.
        telescope (str): Optional telescope name to filter data.

    Returns:
        dict: JSON response with guiding data including datetime,
              telescope_name, post_pid_x, and post_pid_y values.
    """
    db = observatory_db()
    telescope_filter = f"AND telescope_name = '{telescope}'" if telescope else ""

    if since is not None:
        q = f"""SELECT datetime, telescope_name, post_pid_x, post_pid_y FROM autoguider_log 
                WHERE datetime > '{since}' {telescope_filter} ORDER BY datetime ASC"""
    else:
        q = f"""SELECT datetime, telescope_name, post_pid_x, post_pid_y FROM autoguider_log 
                WHERE datetime > datetime('now', '-{day} day') {telescope_filter} ORDER BY datetime ASC"""

    df = pd.read_sql_query(q, db)
    db.close()

    if df.empty:
        return {"status": "success", "data": [], "message": "No guiding data available"}

    # Convert datetime to proper format
    df["datetime"] = pd.to_datetime(df["datetime"])

    return {
        "status": "success",
        "data": df.to_dict(orient="records"),
        "message": "",
    }


@app.get("/api/log")
async def log(datetime: str, limit: int = 100):
    """Get observatory log entries before specified datetime.

    Args:
        datetime (str): Upper limit datetime for log entries.
        limit (int): Maximum number of log entries to return. Defaults to 100.

    Returns:
        list: Log entries as dictionary records.
    """
    db = observatory_db()
    q = f"""SELECT * FROM (SELECT * FROM log WHERE datetime < '{datetime}' ORDER BY datetime DESC LIMIT {limit}) a ORDER BY datetime ASC"""

    df = pd.read_sql_query(q, db)

    db.close()

    return df.to_dict(orient="records")


@app.websocket("/ws/log")
async def websocket_log(websocket: WebSocket):
    """WebSocket endpoint for real-time log streaming.

    Provides initial log history and streams new log entries as they
    are added to the database. Also includes schedule modification time.

    Args:
        websocket (WebSocket): WebSocket connection object.
    """
    await websocket.accept()
    obs = OBSERVATORY

    db = observatory_db()
    q = """SELECT * FROM (SELECT * FROM log ORDER BY datetime DESC LIMIT 100) a ORDER BY datetime ASC"""
    initial_df = pd.read_sql_query(q, db)

    last_time = initial_df.datetime.iloc[-1]

    initial_log = initial_df.to_dict(orient="records")

    data_dict = {}
    data_dict["log"] = initial_log
    data_dict["schedule_mtime"] = obs.schedule_manager.schedule_mtime

    socket = True

    try:
        await websocket.send_json(data_dict)
        await asyncio.sleep(1)
    except Exception:
        socket = False

    while socket:
        if len(initial_log) > 0:
            q = f"""SELECT * FROM log WHERE datetime > '{last_time}'"""

        df = pd.read_sql_query(q, db)
        data = df.to_dict(orient="records")

        data_dict = {}
        data_dict["log"] = data
        data_dict["schedule_mtime"] = obs.schedule_manager.schedule_mtime

        try:
            if len(data) > 0:
                last_time = df.datetime.iloc[-1]
            await websocket.send_json(data_dict)
            await asyncio.sleep(1)
        except Exception:
            db.close()
            logging.info("log socket closed")
            socket = False


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """Main WebSocket endpoint for real-time observatory status updates.

    Streams comprehensive observatory status including device polling data,
    system health, operational status, and latest images. Handles FITS to
    JPEG conversion for image display.

    Args:
        websocket (WebSocket): WebSocket connection object.
    """
    global LAST_IMAGE, LAST_IMAGE_PREVIEW, LAST_IMAGE_TIME

    await websocket.accept()

    obs = OBSERVATORY

    socket = True
    while socket:
        dt_now = datetime.datetime.now(UTC)
        polled_list = {}

        for device_type in obs.devices:
            polled_list[device_type] = {}

            for device_name in obs.devices[device_type]:
                polled_list[device_type][device_name] = {}

                polled = obs.devices[device_type][device_name].poll_latest()

                if polled is not None:  # not sure if correct to put this here, or later
                    polled_keys = polled.keys()
                    for k in polled_keys:
                        polled_list[device_type][device_name][k] = {}
                        polled_list[device_type][device_name][k]["value"] = polled[k][
                            "value"
                        ]
                        polled_list[device_type][device_name][k]["datetime"] = polled[
                            k
                        ]["datetime"]

        thread_summaries = obs.thread_manager.get_thread_summary()
        table0 = []
        table1 = [
            {"item": "error free", "value": obs.logger.error_free},
            {
                "item": "utc time",
                "value": datetime.datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S"),
            },
            {
                "item": "watchdog",
                "value": "running" if obs.watchdog_running else "stopped",
            },
            {
                "item": "schedule",
                "value": "running" if obs.schedule_manager.running else "stopped",
            },
            {
                "item": "robotic switch",
                "value": "on" if obs.robotic_switch else "off",
            },
            {"item": "weather safe", "value": "safe" if obs.weather_safe else "unsafe"},
            {
                "item": "error source",
                "value": "none"
                if len(obs.logger.error_source) == 0
                else "hover to see",
                "error_source": obs.logger.error_source,
            },
            {
                "item": "threads",
                "value": len(thread_summaries),
                "threads": thread_summaries,
            },
            {"item": "time to safe", "value": f"{obs.time_to_safe:.2f} mins"},
        ]

        try:
            if "Telescope" in obs.devices:
                # we want to know if slewing or tracking
                device_type = "Telescope"
                for device_name in polled_list[device_type].keys():
                    polled = polled_list[device_type][device_name]

                    tracking = polled["Tracking"]["value"]
                    dt_tracking = polled["Tracking"]["datetime"]
                    slewing = polled["Slewing"]["value"]
                    dt_slewing = polled["Slewing"]["datetime"]

                    status = (
                        "slewing" if slewing else "tracking" if tracking else "stopped"
                    )
                    dt = (
                        dt_tracking
                        if tracking
                        else dt_slewing
                        if slewing
                        else dt_tracking
                    )

                    try:
                        polled["RightAscension"]["value"] = polled["RightAscension"][
                            "value"
                        ] * (360 / 24)  # convert to degrees
                    except Exception:
                        pass

                    last_update = (dt_now - dt).total_seconds()
                    last_update = last_update if last_update > 0 else 0

                    valid = None
                    # convert datetime to string and check if polled values are valid
                    for key in polled:
                        polled[key]["datetime"] = polled[key]["datetime"].strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )
                        if polled[key]["value"] != "null" and valid is not False:
                            valid = True
                        else:
                            valid = False

                    table0.append(
                        {
                            "item": device_type,
                            "name": device_name,
                            "status": status,
                            "valid": valid,
                            "last_update": f"{last_update:.0f} s ago",
                            "polled": polled,
                        }
                    )

                    table0.append(
                        {
                            "item": "guider",
                            "name": f"{device_name}'s guider",
                            "status": obs.guider_manager.guider[device_name].running,
                            "valid": valid,
                            "last_update": "0 s ago",
                        }
                    )

            if "Dome" in obs.devices:
                # we want to know if dome open or closed
                device_type = "Dome"
                for device_name in polled_list[device_type].keys():
                    polled = polled_list[device_type][device_name]

                    shutter_status = polled["ShutterStatus"]["value"]

                    if shutter_status == 0:
                        status = "open"
                    elif shutter_status == 1:
                        status = "closed"
                    elif shutter_status == 2:
                        status = "opening"
                    elif shutter_status == 3:
                        status = "closing"
                    elif shutter_status == 4:
                        status = "error"
                    else:
                        status = "unknown"

                    dt = polled["ShutterStatus"]["datetime"]

                    last_update = (dt_now - dt).total_seconds()
                    last_update = last_update if last_update > 0 else 0

                    valid = None
                    # convert datetime to string and check if polled values are valid
                    for key in polled:
                        polled[key]["datetime"] = polled[key]["datetime"].strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )
                        if polled[key]["value"] != "null" and valid is not False:
                            valid = True
                        else:
                            valid = False

                    table0.append(
                        {
                            "item": device_type,
                            "name": device_name,
                            "status": status,
                            "valid": valid,
                            "last_update": f"{last_update:.0f} s ago",
                            "polled": polled,
                        }
                    )

            if "FilterWheel" in obs.devices:
                # we want to know name of filter
                device_type = "FilterWheel"
                for device_name in polled_list[device_type].keys():
                    polled = polled_list[device_type][device_name]

                    pos = polled["Position"]["value"]

                    if pos == -1:
                        status = "moving"
                    else:
                        try:
                            status = FWS[device_name][pos]
                        except KeyError:
                            logger.error(
                                f"FilterWheel {device_name} position {pos} not found in fws dict",
                                FWS,
                            )
                            status = "unknown"

                    dt = polled["Position"]["datetime"]

                    last_update = (dt_now - dt).total_seconds()
                    last_update = last_update if last_update > 0 else 0

                    valid = None
                    # convert datetime to string and check if polled values are valid
                    for key in polled:
                        polled[key]["datetime"] = polled[key]["datetime"].strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )
                        if polled[key]["value"] != "null" and valid is not False:
                            valid = True
                        else:
                            valid = False

                    table0.append(
                        {
                            "item": device_type,
                            "name": device_name,
                            "status": status,
                            "valid": valid,
                            "last_update": f"{last_update:.0f} s ago",
                            "polled": polled,
                            "filter_names": FWS[device_name],
                        }
                    )

            if "Camera" in obs.devices:
                device_type = "Camera"
                for device_name in polled_list[device_type].keys():
                    polled = polled_list[device_type][device_name]

                    camera_status = polled["CameraState"]["value"]

                    if camera_status == 0:
                        status = "idle"
                    elif camera_status == 1:
                        status = "waiting"
                    elif camera_status == 2:
                        status = "exposing"
                    elif camera_status == 3:
                        status = "reading"
                    elif camera_status == 4:
                        status = "download"
                    elif camera_status == 5:
                        status = "error"
                    else:
                        status = "unknown"

                    status += f" ({polled['CCDTemperature']['value']:.2f} C)"

                    dt = polled["CameraState"]["datetime"]

                    last_update = (dt_now - dt).total_seconds()
                    last_update = last_update if last_update > 0 else 0

                    valid = None
                    # convert datetime to string and check if polled values are valid
                    for key in polled:
                        polled[key]["datetime"] = polled[key]["datetime"].strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )
                        if polled[key]["value"] != "null" and valid is not False:
                            valid = True
                        else:
                            valid = False

                    table0.append(
                        {
                            "item": device_type,
                            "name": device_name,
                            "status": status,
                            "valid": valid,
                            "last_update": f"{last_update:.0f} s ago",
                            "polled": polled,
                        }
                    )

            if "Focuser" in obs.devices:
                device_type = "Focuser"
                for device_name in polled_list[device_type].keys():
                    polled = polled_list[device_type][device_name]

                    status = polled["Position"]["value"]

                    dt = polled["Position"]["datetime"]

                    last_update = (dt_now - dt).total_seconds()
                    last_update = last_update if last_update > 0 else 0

                    valid = None
                    # convert datetime to string and check if polled values are valid
                    for key in polled:
                        polled[key]["datetime"] = polled[key]["datetime"].strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )
                        if polled[key]["value"] != "null" and valid is not False:
                            valid = True
                        else:
                            valid = False

                    table0.append(
                        {
                            "item": device_type,
                            "name": device_name,
                            "status": status,
                            "valid": valid,
                            "last_update": f"{last_update:.0f} s ago",
                            "polled": polled,
                        }
                    )

            if "ObservingConditions" in obs.devices:
                device_type = "ObservingConditions"
                for device_name in polled_list[device_type].keys():
                    polled = polled_list[device_type][device_name]

                    dt = polled["Temperature"]["datetime"]

                    last_update = (dt_now - dt).total_seconds()
                    last_update = last_update if last_update > 0 else 0

                    valid = None
                    status = None
                    # convert datetime to string and check if polled values are valid
                    for key in polled:
                        polled[key]["datetime"] = polled[key]["datetime"].strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )
                        if polled[key]["value"] != "null" and valid is not False:
                            valid = True
                            status = "valid"
                        else:
                            valid = False
                            status = "invalid"

                    table0.append(
                        {
                            "item": device_type,
                            "name": device_name,
                            "status": status,
                            "valid": valid,
                            "last_update": f"{last_update:.0f} s ago",
                            "polled": polled,
                        }
                    )

            if "SafetyMonitor" in obs.devices:
                device_type = "SafetyMonitor"
                for device_name in polled_list[device_type].keys():
                    polled = polled_list[device_type][device_name]

                    safe = polled["IsSafe"]["value"]

                    valid = None
                    if safe is True:
                        status = "safe"
                        valid = True
                    else:
                        status = "unsafe"
                        valid = False

                    dt = polled["IsSafe"]["datetime"]

                    last_update = (dt_now - dt).total_seconds()
                    last_update = last_update if last_update > 0 else 0

                    # convert datetime to string and check if polled values are valid
                    for key in polled:
                        polled[key]["datetime"] = polled[key]["datetime"].strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )
                        if polled[key]["value"] != "null" and valid is not False:
                            valid = True
                        else:
                            valid = False

                    table0.append(
                        {
                            "item": device_type,
                            "name": device_name,
                            "status": status,
                            "valid": valid,
                            "last_update": f"{last_update:.0f} s ago",
                            "polled": polled,
                        }
                    )

        except Exception as e:
            logger.error(f"Error in websocket_endpoint: {e}", exc_info=True)

        # Check all image handlers for the most recent image
        image_info = None
        if obs._image_handlers:
            most_recent_path = None
            most_recent_time = None

            for camera_name, handler in obs._image_handlers.items():
                if handler.last_image_path is not None:
                    if most_recent_time is None or (
                        handler.last_image_timestamp is not None
                        and handler.last_image_timestamp > most_recent_time
                    ):
                        most_recent_path = handler.last_image_path
                        most_recent_time = handler.last_image_timestamp

            # Update preview if we have a new image
            if most_recent_path is not None and LAST_IMAGE != most_recent_path:
                LAST_IMAGE = most_recent_path
                LAST_IMAGE_TIME = most_recent_time
                try:
                    jpeg_bytes, headers = convert_fits_to_preview(str(LAST_IMAGE))
                    LAST_IMAGE_PREVIEW = (jpeg_bytes, headers)
                except Exception as e:
                    logger.error(f"Error converting FITS to preview: {e}")

            if LAST_IMAGE_PREVIEW is not None:
                image_info = {
                    "available": True,
                    "timestamp": LAST_IMAGE_TIME.isoformat()
                    if LAST_IMAGE_TIME
                    else None,
                    "headers": LAST_IMAGE_PREVIEW[1],
                }

        data = {
            "table0": table0,
            "table1": table1,
            "last_image": image_info,
        }

        # make temp image, say how many images have been made?
        try:
            await websocket.send_json(data)
            await asyncio.sleep(1)
        except Exception:
            socket = False


@app.get("/autofocus", include_in_schema=False)
async def autofocus(request: Request):
    """Autofocus web interface endpoint (TODO: Implement).

    Placeholder for autofocus functionality that will process CSV
    files with FITS image references for focus analysis.

    Args:
        request (Request): FastAPI request object.

    Returns:
        TemplateResponse: HTML template for autofocus interface.
    """
    return FRONTEND.TemplateResponse(
        request=request,
        name="autofocus.html.j2",
        context={
            "request": request,
            # "observatories": list(OBSERVATORY.keys()),
            # "webcamfeeds": WEBCAMFEED,
            # "configs": {obs.name: obs.config for obs in OBSERVATORY.values()},
        },
    )


@app.get("/schedule")
async def get_schedule(request: Request):
    """Serve schedule editor page with current schedule data.

    Loads raw JSONL schedule file preserving original datetime
    format for the web-based schedule editor interface.

    Args:
        request (Request): FastAPI request object.

    Returns:
        TemplateResponse: HTML template with schedule editor and data.
    """
    obs = OBSERVATORY

    # Read the raw JSONL file to preserve original datetime string format
    schedule_path = obs.schedule_manager.schedule_path
    try:
        with open(schedule_path, "r") as f:
            schedule_jsonl = f.read().strip()
    except (FileNotFoundError, IOError):
        schedule_jsonl = ""

    return FRONTEND.TemplateResponse(
        request=request,
        name="schedule.html.j2",
        context={
            "request": request,
            "observatory": OBSERVATORY.name,
            "schedule": schedule_jsonl,
        },
    )


@app.get("/fits_explorer", include_in_schema=False)
async def fits_explorer_root_redirect():
    """Redirect /fits_explorer -> /fits_explorer/ so the mounted sub-app handles it."""
    return RedirectResponse(url="/fits_explorer/", status_code=307)


@app.get("/{path:path}", include_in_schema=False)
async def serve_files(request: Request, path: str = ""):
    """Serve static files and main application interface.

    Handles routing for the main dashboard, favicon, JavaScript files,
    and frontend assets. Returns 404 for unknown paths.

    Args:
        request (Request): FastAPI request object.
        path (str): Requested file path. Defaults to empty string for root.

    Returns:
        Union[TemplateResponse, FileResponse, HTMLResponse]: Appropriate response
            based on requested path.
    """
    if path == "":
        return FRONTEND.TemplateResponse(
            request=request,
            name="index.html.j2",
            context={
                "request": request,
                "observatory": OBSERVATORY.name,
                "webcamfeeds": WEBCAMFEED,
                "config": OBSERVATORY.config,
            },
        )
    elif path == "favicon.svg":
        return FileResponse(str(FRONTEND_PATH / "favicon.svg"))
    elif path == "favicon.ico":
        favicon = FRONTEND_PATH / "favicon.svg"
        if favicon.exists():
            return FileResponse(str(favicon), media_type="image/svg+xml")
        return HTMLResponse(status_code=404, content="Not Found")
    elif path.startswith("js/"):
        return FileResponse(str(FRONTEND_PATH / path))
    elif path.startswith("frontend/"):
        return FileResponse(str(FRONTEND_PATH / path[len("frontend/") :]))
    else:
        return HTMLResponse(status_code=404, content="Not Found")


def main():
    """Main entry point for Astra observatory automation system.

    Parses command line arguments, configures logging, handles configuration
    reset, and starts the FastAPI server with specified options.
    """
    print(r"""
            *        *                 *                 *        *             
                                                                                 
        ███            *                  ██              *                     
       █████                              ██          *                         
   *   ██ ██                             ███                   *                
      ██   ██         ██████████    ███████████████    ███████ ██████    █████████   *
     ███   ███       ██        █         ███               ████          █       ██     
     ██     ██       ██                  ███           *   ███                   ███   
    ███ ███ ███       ████████           ███               ███            ██████████   *
    ██ █████ ██              ████        ███       *       ███          ███      ███    
 * ███       ███               ██        ███               ███          ██       ███    
   ██         ███   ████      ███         ██           *   ███          ███     ████    
  ██           ██      ████████            ████████    ████████████       ██████  ████  
                                                                                 
            *             *                       *              *             
    """)
    from sys import platform

    if platform == "linux":
        # on linux, switch process launching model from fork to spawn to avoid system lockup
        # using fork clones all variables in the same state, whereas spawsn instantiates a new interpreter and reloads all
        # modules.
        # Looks like the spawn cloning makes multiple process wait on the same object. From previous debugging,
        # urllib3 clones all connection information and then processes lock each other
        # by having multiple instances all expecting an answer on the same cloned connection
        import multiprocessing

        multiprocessing.set_start_method("spawn")

    import argparse

    global DEBUG, TRUNCATE_FACTOR, CUSTOM_OBSERVATORY

    print(f"Astra config path: {Config.CONFIG_PATH}")

    parser = argparse.ArgumentParser(description="Run Astra")
    parser.add_argument(
        "--debug", action="store_true", help="run in debug mode (default: false)"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="port to run the server on (default: 8000)",
    )
    parser.add_argument(
        "--truncate",
        type=float,
        help="truncate schedule by factor and reset time start time to now (default: None)",
    )
    parser.add_argument(
        "--observatory",
        type=str,
        help="specify observatory name (default: None)",
    )
    parser.add_argument(
        "--reset", action="store_true", help="reset the Astra's base config"
    )
    args = parser.parse_args()

    if args.reset:
        prompt = (
            input("Are you sure you want to reset Astra's base config? [y/n]: ")
            .strip()
            .lower()
        )
        if prompt == "y":
            Config(reset=True)

    Config().paths.archive_log_file()
    logging.basicConfig(
        format=FileHandler.FORMAT,
        datefmt=FileHandler.DATEFMT,
        filename=Config().paths.log_file,
        level=logging.DEBUG,
    )
    logging.Formatter.converter = time.gmtime

    if args.debug:
        DEBUG = True
        logging.getLogger().setLevel(logging.DEBUG)

    logger.info(f"Astra version: {__version__}")

    TRUNCATE_FACTOR = args.truncate

    if args.observatory:
        CUSTOM_OBSERVATORY = args.observatory

    # start the server
    log_level = "info" if not DEBUG else "debug"
    if log_level == "info":
        logging.getLogger().setLevel(logging.INFO)

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=args.port,
        log_level=log_level,
        timeout_graceful_shutdown=None,
        log_config={
            "version": 1,
            "disable_existing_loggers": False,
            "handlers": {
                "astra": {
                    "formatter": "astra",
                    "class": "logging.StreamHandler",
                    "stream": "ext://sys.stdout",
                }
            },
            "formatters": {
                "astra": {
                    "()": "astra.logger.CustomFormatter",
                    "fmt": "%(levelname)-8s :: %(asctime)s :: %(message)s",
                    "datefmt": "%Y-%m-%d %H:%M:%S",
                },
            },
            "loggers": {
                "uvicorn": {"handlers": ["astra"], "level": "INFO", "propagate": False},
                "uvicorn.error": {
                    "handlers": ["astra"],
                    "level": "INFO",
                    "propagate": False,
                },
                "uvicorn.access": {
                    "handlers": ["astra"],
                    "level": "INFO",
                    "propagate": False,
                },
            },
        },
    )


if __name__ == "__main__":
    main()
