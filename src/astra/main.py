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
import os
import sqlite3
import time
from contextlib import asynccontextmanager
from datetime import UTC
from glob import glob
from io import BytesIO
from pathlib import Path

import httpx
import matplotlib.pyplot as plt
import pandas as pd
import uvicorn
from astropy.io import fits
from astropy.visualization import ZScaleInterval
from fastapi import Body, FastAPI, File, Request, UploadFile, WebSocket
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from astra import ASTRA_VER, Config
from astra.image_handler import HeaderManager
from astra.logger import ConsoleStreamHandler, CustomFormatter, FileHandler
from astra.observatory import Observatory
from astra.paired_devices import PairedDevices

logger = logging.getLogger(__name__)
logger.addHandler(ConsoleStreamHandler())
logger.setLevel(logging.INFO)

# silence httpx logging
logging.getLogger("httpx").setLevel(logging.WARNING)

# global variables
FRONTEND_PATH = Path(__file__).parent / "frontend"
OBSERVATORIES: dict[str, Observatory] = {}
WEBCAMFEEDS = {}
FWS = {}
DEBUG = False
FRONTEND = Jinja2Templates(directory=FRONTEND_PATH)
LAST_IMAGE = None
LAST_IMAGE_JPG = None
USEFUL_HEADERS = None
TRUNCATE_FACTOR = None
CUSTOM_OBSERVATORY = None


def observatory_db(name: str) -> sqlite3.Connection:
    """Get database connection for observatory logging.

    Args:
        name (str): Observatory name for database file.

    Returns:
        sqlite3.Connection: Database connection object.
    """
    db = sqlite3.connect(Config().paths.logs / f"{name}.db")
    return db


def load_observatories() -> None:
    """Load and initialize all observatory configurations.

    Discovers observatory config files, creates Observatory instances,
    establishes device connections, and sets up filter wheel mappings.
    Updates global OBSERVATORIES, WEBCAMFEEDS, and FWS dictionaries.
    """
    global OBSERVATORIES  # not sure if this is necessary
    global WEBCAMFEEDS
    global FWS

    config_files = glob(
        str(Config().paths.observatory_config / "*_config.yml")
    )  # should we use Config().config['observatory_name'] here instead?

    for config_filename in config_files:
        obs = Observatory(
            config_filename,
            TRUNCATE_FACTOR,
            custom_observatory=CUSTOM_OBSERVATORY,
            logging_level=logging.DEBUG if DEBUG else logging.INFO,
        )
        OBSERVATORIES[obs.name] = obs

        if "Misc" in obs.config:
            if "Webcam" in obs.config["Misc"]:
                WEBCAMFEEDS[obs.name] = obs.config["Misc"]["Webcam"]

        obs.connect_all_devices()

        if "FilterWheel" in obs.devices:
            FWS[obs.name] = {}
            for fw_name in obs.devices["FilterWheel"].keys():
                filter_names = obs.devices["FilterWheel"][fw_name].get("Names")
                obs.logger.info(f"FilterWheel {fw_name} has filters: {filter_names}")
                FWS[obs.name][fw_name] = obs.devices["FilterWheel"][fw_name].get(
                    "Names"
                )


def clean_up() -> None:
    """Clean up and stop all observatory devices before shutdown.

    Iterates through all observatories and device types to safely
    stop all connected devices. Handles exceptions during shutdown.
    """
    for obs in OBSERVATORIES.values():
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
                    logger.error(f"Error stopping device {device_name}: {e}")

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


def convert_fits_to_jpg(fits_file: str, observatory: str) -> tuple[str, dict]:
    """Convert FITS astronomical image to JPEG for web display.

    Opens FITS file, extracts image data and headers, applies Z-scale
    normalization, and saves as JPEG. Removes old JPEG files for the
    observatory before creating new one.

    Args:
        fits_file (str): Path to FITS file to convert.
        observatory (str): Observatory name for file management.

    Returns:
        tuple[str, dict]: Filepath (relative path to JPEG) and headers
            (extracted FITS header information).
    """
    # Open the FITS file
    headers = {}
    with fits.open(fits_file) as hdulist:
        # Get the image data from the primary HDU
        image_data = hdulist[0].data
        headers["EXPTIME"] = hdulist[0].header["EXPTIME"]
        headers["DATE-OBS"] = hdulist[0].header["DATE-OBS"]
        headers["FILTER"] = hdulist[0].header["FILTER"]
        headers["IMAGETYP"] = hdulist[0].header["IMAGETYP"]
        if headers["IMAGETYP"] == "Light":
            headers["OBJECT"] = hdulist[0].header["OBJECT"]

    # Normalize the image data to the 8-bit range (0-255)
    interval = ZScaleInterval(contrast=0.005)
    vmin, vmax = interval.get_limits(image_data)

    # delete previous jpgs
    old_img_path = str(FRONTEND_PATH / f"*{observatory}*.jpg")
    for file in glob(old_img_path):
        os.remove(file)

    # Save the jpg image
    filename = os.path.splitext(os.path.basename(fits_file))[0] + ".jpg"
    filepath = str(FRONTEND_PATH / filename)
    plt.imsave(filepath, image_data, format="jpg", cmap="gray", vmin=vmin, vmax=vmax)

    # TODO: don't like this trick, but it works for now
    return str(Path("frontend") / filename), headers


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


@app.get("/video/{observatory}/{filename:path}", include_in_schema=False)
async def get_video(request: Request, observatory: str, filename: str = None):
    """Proxy video streams from observatory webcams.

    Forwards HTTP requests to webcam feeds, handling both MP4 video
    streams and HTML content with appropriate media types.

    Args:
        request (Request): FastAPI request object with headers.
        observatory (str): Observatory name for webcam lookup.
        filename (str): Video filename or path to stream.

    Returns:
        StreamingResponse: Proxied video content with appropriate headers.
    """
    headers = request.headers
    base_url = WEBCAMFEEDS[observatory]
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


@app.get("/api/heartbeat/{observatory}")
async def heartbeat(observatory: str):
    """Get observatory heartbeat status for health monitoring.

    Args:
        observatory (str): Observatory name to check.

    Returns:
        dict: JSON response with heartbeat status data.
    """
    obs = OBSERVATORIES[observatory]

    return {"status": "success", "data": obs.heartbeat, "message": ""}


# @app.get("/api/open/{observatory}")
# def open_observatory(observatory: str):
#     obs = OBSERVATORIES[observatory]

#     obs.logger.info(f"User initiated opening of observatory from web interface")

#     obs.open_observatory()

#     return {"status": "success", "data": "null", "message": ""}


@app.get("/api/close/{observatory}")
def close_observatory(observatory: str):
    """Close observatory and stop all operations safely.

    Stops running schedule if active and closes the observatory.
    Logs all actions for audit trail.

    Args:
        observatory (str): Observatory name to close.

    Returns:
        dict: JSON response with operation status.
    """
    obs = OBSERVATORIES[observatory]

    obs.logger.info("User initiated closing of observatory from web interface")

    if obs.schedule_manager.running:
        obs.logger.info("Stopping schedule for safety.")
        obs.schedule_manager.stop_schedule(obs.thread_manager)

    val = obs.close_observatory()

    if val:
        obs.logger.info("Observatory closed.")

    return {"status": "success", "data": "null", "message": ""}


@app.get("/api/cool_camera/{observatory}/{device_name}")
def cool_camera(observatory: str, device_name: str):
    """Initiate camera cooling to configured target temperature.

    Gets camera configuration and starts cooling process to the
    specified set temperature with defined tolerance.

    Args:
        observatory (str): Observatory name containing the camera.
        device_name (str): Camera device name to cool.

    Returns:
        dict: JSON response with operation status and cooling details.
    """
    obs = OBSERVATORIES[observatory]

    row = {"device_name": device_name}

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

    current_temperature = camera.poll_latest()["CCDTemperature"]["value"]

    obs.logger.info(
        f"Current camera temperature: {current_temperature}C, Set temperature: {set_temperature}C"
    )

    obs.cool_camera(
        row,
        set_temperature=set_temperature,
        temperature_tolerance=temperature_tolerance,
        cooling_timeout=cooling_timeout,
    )

    return {"status": "success", "data": "null", "message": ""}


@app.get("/api/complete_headers/{observatory}")
def complete_headers(observatory: str):
    """Complete FITS header processing for observatory images.

    Args:
        observatory (str): Observatory name to process headers for.

    Returns:
        dict: JSON response with operation status.
    """
    obs = OBSERVATORIES[observatory]

    obs.logger.info("User initiated completion of headers from web interface")

    HeaderManager.final_headers(
        obs.database_manager,
        obs.logger,
        obs.config,
        obs.devices,
        obs.fits_config,
    )

    return {"status": "success", "data": "null", "message": ""}


@app.get("/api/startwatchdog/{observatory}")
async def start_watchdog(observatory: str):
    """Start observatory watchdog monitoring system.

    Resets error states and starts the watchdog process for
    continuous observatory health monitoring.

    Args:
        observatory (str): Observatory name to start watchdog for.

    Returns:
        dict: JSON response with operation status.
    """
    obs = OBSERVATORIES[observatory]

    obs.logger.info("User initiated starting of watchdog from web interface")

    obs.logger.error_free = True
    obs.logger.error_source = []
    obs.start_watchdog()

    return {"status": "success", "data": "null", "message": ""}


@app.get("/api/stopwatchdog/{observatory}")
async def stop_watchdog(observatory: str):
    """Stop observatory watchdog monitoring system.

    Args:
        observatory (str): Observatory name to stop watchdog for.

    Returns:
        dict: JSON response with operation status.
    """
    obs = OBSERVATORIES[observatory]

    obs.logger.info("User initiated stopping of watchdog from web interface")

    obs.watchdog_running = False

    return {"status": "success", "data": "null", "message": ""}


@app.get("/api/roboticswitch/{observatory}")
async def roboticswitch(observatory: str):
    """Toggle observatory robotic operation mode.

    Args:
        observatory (str): Observatory name to toggle robotic mode for.

    Returns:
        dict: JSON response with current robotic switch state.
    """
    obs = OBSERVATORIES[observatory]

    obs.logger.info("User initiated robotic switch from web interface")

    obs.toggle_robotic_switch()

    return {"status": "success", "data": obs.robotic_switch, "message": ""}


@app.get("/api/startschedule/{observatory}")
async def start_schedule(observatory: str):
    """Start executing the observatory's observation schedule.

    Args:
        observatory (str): Observatory name to start schedule for.

    Returns:
        dict: JSON response with operation status.
    """
    obs = OBSERVATORIES[observatory]

    obs.logger.info("User initiated starting of schedule from web interface")

    obs.schedule_manager.start_schedule(obs)

    return {"status": "success", "data": "null", "message": ""}


@app.get("/api/stopschedule/{observatory}")
async def stop_schedule(observatory: str):
    """Stop executing the observatory's observation schedule.

    Args:
        observatory (str): Observatory name to stop schedule for.

    Returns:
        dict: JSON response with operation status.
    """
    obs = OBSERVATORIES[observatory]

    obs.logger.info("User initiated stopping of schedule from web interface")

    obs.schedule_manager.stop_schedule(obs.thread_manager)

    return {"status": "success", "data": "null", "message": ""}


@app.get("/api/schedule/{observatory}")
async def schedule(observatory: str):
    """Get current observatory schedule with formatted times.

    Args:
        observatory (str): Observatory name to get schedule for.

    Returns:
        list: Schedule items with start/end times formatted as HH:MM:SS,
              or empty list if no schedule exists.
    """
    obs = OBSERVATORIES[observatory]
    if obs.schedule_manager.schedule_mtime != 0:
        try:
            schedule = obs.schedule_manager.get_schedule().to_dataframe()
        except Exception as e:
            obs.logger.warning(f"Error reading schedule for frontend: {e}")
            return []

        schedule["start_HHMMSS"] = schedule["start_time"].apply(format_time)
        schedule["end_HHMMSS"] = schedule["end_time"].apply(format_time)

        # replace NaN with None
        schedule = schedule.where(pd.notnull(schedule), None)

        return schedule.to_dict(orient="records")
    else:
        return []


@app.post("/api/editschedule/{observatory}")
async def edit_schedule(
    observatory: str, schedule_data: str = Body(..., media_type="text/plain")
):
    """Update observatory schedule from web editor.

    Parses JSONL schedule data and saves to observatory schedule file.

    Args:
        observatory (str): Observatory name to update schedule for.
        schedule_data (str): JSONL formatted schedule data.

    Returns:
        dict: Status response with success/error information.
    """
    obs = OBSERVATORIES[observatory]

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
        obs.logger.error(f"Error updating schedule: {e}")
        return {
            "status": "error",
            "data": None,
            "message": f"Error updating schedule: {str(e)}",
        }


@app.post("/api/uploadschedule/{observatory}")
async def upload_schedule(observatory: str, file: UploadFile = File(...)):
    """Upload schedule file to replace current observatory schedule.

    Args:
        observatory (str): Observatory name to upload schedule for.
        file (UploadFile): Uploaded schedule file in JSONL format.

    Returns:
        dict: Upload status response with success/error information.
    """
    obs = OBSERVATORIES[observatory]

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


@app.get("/api/db/polling/{observatory}/{device_type}")
async def polling(
    observatory: str, device_type: str, day: float = 1, since: str = None
):
    """Get device polling data from observatory database.

    Retrieves and processes telemetry data for specific device types,
    including pivot formatting, safety limits, and statistical grouping.

    Args:
        observatory (str): Observatory name to query data for.
        device_type (str): Type of device (e.g., 'ObservingConditions').
        day (float): Number of days back to retrieve data. Defaults to 1.
        since (str): Optional timestamp to get only newer records.

    Returns:
        dict: Processed polling data with safety limits and latest values.
    """
    db = observatory_db(observatory)
    if since:
        # Only fetch new records since the given timestamp
        q = f"""SELECT * FROM polling WHERE device_type = '{device_type}' AND datetime > '{since}'"""
    else:
        q = f"""SELECT * FROM polling WHERE device_type = '{device_type}' AND datetime > datetime('now', '-{day} day')"""

    df = pd.read_sql_query(q, db)
    db.close()

    # Pivot: datetime as index, device_command as columns
    df = df.pivot(index="datetime", columns="device_command", values="device_value")

    # Ensure datetime index and numeric values
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()
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

    obs = OBSERVATORIES[observatory]
    if "ObservingConditions" in obs.config:
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

        return {
            "data": df_groupby.reset_index().to_dict(orient="records"),
            "safety_limits": safety_limits,
            "latest": latest,
        }
    else:
        return {
            "data": df_groupby.reset_index().to_dict(orient="records"),
            "latest": latest,
        }


@app.get("/api/log/{observatory}")
async def log(observatory: str, datetime: str, limit: int = 100):
    """Get observatory log entries before specified datetime.

    Args:
        observatory (str): Observatory name to query logs for.
        datetime (str): Upper limit datetime for log entries.
        limit (int): Maximum number of log entries to return. Defaults to 100.

    Returns:
        list: Log entries as dictionary records.
    """
    db = observatory_db(observatory)
    q = f"""SELECT * FROM (SELECT * FROM log WHERE datetime < '{datetime}' ORDER BY datetime DESC LIMIT {limit}) a ORDER BY datetime ASC"""

    df = pd.read_sql_query(q, db)

    db.close()

    return df.to_dict(orient="records")


@app.websocket("/ws/log/{observatory}")
async def websocket_log(websocket: WebSocket, observatory: str):
    """WebSocket endpoint for real-time log streaming.

    Provides initial log history and streams new log entries as they
    are added to the database. Also includes schedule modification time.

    Args:
        websocket (WebSocket): WebSocket connection object.
        observatory (str): Observatory name for log streaming.
    """
    await websocket.accept()
    obs = OBSERVATORIES[observatory]

    db = observatory_db(observatory)
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
        logger.error("log socket closed")
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


@app.websocket("/ws/{observatory}")
async def websocket_endpoint(websocket: WebSocket, observatory: str):
    """Main WebSocket endpoint for real-time observatory status updates.

    Streams comprehensive observatory status including device polling data,
    system health, operational status, and latest images. Handles FITS to
    JPEG conversion for image display.

    Args:
        websocket (WebSocket): WebSocket connection object.
        observatory (str): Observatory name for status monitoring.
    """
    global LAST_IMAGE, LAST_IMAGE_JPG, USEFUL_HEADERS

    await websocket.accept()

    obs = OBSERVATORIES[observatory]

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
                            status = FWS[observatory][device_name][pos]
                        except Exception:
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
                            "filter_names": FWS[observatory][device_name],
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
            logger.error(f"Error in websocket_endpoint: {e}")

        # if last_image_jpg is None:
        #     # use placeholder image
        #     last_image_jpg = "https://upload.wikimedia.org/wikipedia/commons/thumb/a/ac/No_image_available.svg/600px-No_image_available.svg.png"

        # TODO: need to make it less CPU intensive if multiple clients
        # Check all image handlers for the most recent image
        if obs._image_handlers:
            # Find the most recent image across all cameras
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

            # Convert to JPEG if we have a new image
            if most_recent_path is not None and LAST_IMAGE != most_recent_path:
                LAST_IMAGE = most_recent_path
                LAST_IMAGE_JPG, USEFUL_HEADERS = convert_fits_to_jpg(
                    str(LAST_IMAGE), observatory
                )

        data = {
            "table0": table0,
            "table1": table1,
            "last_image": {"url": LAST_IMAGE_JPG, "useful_headers": USEFUL_HEADERS},
        }

        # make temp image, say how many images have been made?
        try:
            await websocket.send_json(data)
            await asyncio.sleep(1)
        except Exception:
            logger.error("main socket closed")
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
        "autofocus.html.j2",
        {
            "request": request,
            # "observatories": list(OBSERVATORIES.keys()),
            # "webcamfeeds": WEBCAMFEEDS,
            # "configs": {obs.name: obs.config for obs in OBSERVATORIES.values()},
        },
        request=request,
    )


@app.get("/schedule/{observatory}")
async def get_schedule(request: Request, observatory: str):
    """Serve schedule editor page with current schedule data.

    Loads raw JSONL schedule file preserving original datetime
    format for the web-based schedule editor interface.

    Args:
        request (Request): FastAPI request object.
        observatory (str): Observatory name to load schedule for.

    Returns:
        TemplateResponse: HTML template with schedule editor and data.
    """
    obs = OBSERVATORIES[observatory]

    # Read the raw JSONL file to preserve original datetime string format
    schedule_path = obs.schedule_manager.schedule_path
    try:
        with open(schedule_path, "r") as f:
            schedule_jsonl = f.read().strip()
    except (FileNotFoundError, IOError):
        schedule_jsonl = ""

    return FRONTEND.TemplateResponse(
        "schedule.html.j2",
        {
            "request": request,
            "observatory": observatory,
            "schedule": schedule_jsonl,
        },
        request=request,
    )


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
            "index.html.j2",
            {
                "request": request,
                "observatories": list(OBSERVATORIES.keys()),
                "webcamfeeds": WEBCAMFEEDS,
                "configs": {obs.name: obs.config for obs in OBSERVATORIES.values()},
            },
            request=request,
        )
    elif path == "favicon.svg":
        return FileResponse(str(FRONTEND_PATH / "favicon.svg"))
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

    Config().paths.archive_log_file()
    logging.basicConfig(
        format=FileHandler.FORMAT,
        datefmt=FileHandler.DATEFMT,
        filename=Config().paths.log_file,
        level=logging.DEBUG,
    )
    logging.Formatter.converter = time.gmtime

    global DEBUG, TRUNCATE_FACTOR, CUSTOM_OBSERVATORY

    logger.info(f"Astra version: {ASTRA_VER}")

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
        choices=["speculoos"],
        help="specify observatory name (default: None)",
    )
    parser.add_argument(
        "--reset", action="store_true", help="reset the Astra's base config"
    )
    args = parser.parse_args()

    if args.debug:
        DEBUG = True
        logging.getLogger().setLevel(logging.DEBUG)

    if args.reset:
        prompt = (
            input(
                "Are you sure you want to reset Astra's base config"
                f" located at {Config().CONFIG_PATH}? [y/n]: "
            )
            .strip()
            .lower()
        )
        if prompt == "y":
            Config().reset()

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
            "formatters": {
                "custom": {
                    "()": CustomFormatter,
                    "fmt": "%(levelname)-8s :: %(asctime)s :: %(message)s",
                    "datefmt": "%Y-%m-%d %H:%M:%S",
                },
            },
        },
    )


if __name__ == "__main__":
    main()
