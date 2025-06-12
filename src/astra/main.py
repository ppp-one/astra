import asyncio
import datetime
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
from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from astra import ASTRA_VER, Config
from astra.observatory import Observatory

# silence httpx logging
logging.getLogger("httpx").setLevel(logging.WARNING)

# global variables
CONFIG = Config()
FRONTEND_PATH = Path(__file__).parent / "frontend"
OBSERVATORIES: dict[str, Observatory] = {}
WEBCAMFEEDS = {}
FWS = {}
DEBUG = False
FRONTEND = Jinja2Templates(directory=FRONTEND_PATH)
LAST_IMAGE = None
LAST_IMAGE_JPG = None
USEFUL_HEADERS = None
TRUNCATE_SCHEDULE = False
SPECULOOS = False


def load_observatories():
    global OBSERVATORIES  # not sure if this is necessary
    global WEBCAMFEEDS
    global FWS

    config_files = glob(
        str(CONFIG.paths.observatory_config / "*_config.yml")
    )  # should we use CONFIG.config['observatory_name'] here instead?

    for config_filename in config_files:
        obs = Observatory(config_filename, TRUNCATE_SCHEDULE, speculoos=SPECULOOS)
        OBSERVATORIES[obs.name] = obs

        if "Misc" in obs.config:
            if "Webcam" in obs.config["Misc"]:
                WEBCAMFEEDS[obs.name] = obs.config["Misc"]["Webcam"]

        obs.connect_all()

        if "FilterWheel" in obs.devices:
            FWS[obs.name] = {}
            for fw_name in obs.devices["FilterWheel"].keys():
                filter_names = obs.devices["FilterWheel"][fw_name].get("Names")
                obs.logger.info(f"FilterWheel {fw_name} has filters: {filter_names}")
                FWS[obs.name][fw_name] = obs.devices["FilterWheel"][fw_name].get(
                    "Names"
                )


def observatory_db(name):
    db = sqlite3.connect(CONFIG.paths.logs / f"{name}.db")
    return db


def clean_up():
    for obs in OBSERVATORIES.values():
        # Get all the devices
        for device_type in obs.devices:
            for device_name in obs.devices[device_type]:
                # Get the device
                device = obs.devices[device_type][device_name]
                # Stop the device
                try:
                    # print(f"Stopping device {device_name}")
                    device.stop()
                except Exception as e:
                    print(f"Error stopping device {device_name}: {e}")

    print("Exiting clean_up")


def format_time(ftime: datetime.datetime):
    # if ftime is not NaTType:
    try:
        return ftime.strftime("%H:%M:%S")
    except:
        return None


def convert_fits_to_jpg(fits_file, observatory):
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
    # Load observatories
    load_observatories()
    yield
    # Clean up
    clean_up()


app = FastAPI(lifespan=lifespan)


@app.get("/video/{observatory}/{filename:path}", include_in_schema=False)
async def get_video(request: Request, observatory, filename: str = None):
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
    obs = OBSERVATORIES[observatory]

    obs.logger.info(f"User initiated closing of observatory from web interface")

    if obs.schedule_running:
        obs.logger.info(f"Stopping schedule for safety.")
        obs.stop_schedule()

    val = obs.close_observatory()

    if val:
        obs.logger.info(f"Observatory closed.")

    return {"status": "success", "data": "null", "message": ""}


@app.get("/api/cool_camera/{observatory}/{device_name}")
def cool_camera(observatory: str, device_name: str):
    obs = OBSERVATORIES[observatory]

    row = {"device_name": device_name}

    cam_index = obs.get_cam_index(row["device_name"])

    set_temperature = obs.config["Camera"][cam_index]["temperature"]
    temperature_tolerance = obs.config["Camera"][cam_index]["temperature_tolerance"]

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
    )

    return {"status": "success", "data": "null", "message": ""}


@app.get("/api/complete_headers/{observatory}")
def cool_camera(observatory: str):
    obs = OBSERVATORIES[observatory]

    obs.logger.info(f"User initiated completion of headers from web interface")

    obs.final_headers()

    return {"status": "success", "data": "null", "message": ""}


@app.get("/api/startwatchdog/{observatory}")
async def start_watchdog(observatory: str):
    obs = OBSERVATORIES[observatory]

    obs.logger.info(f"User initiated starting of watchdog from web interface")

    obs.error_free = True
    obs.error_source = []
    obs.start_watchdog()

    return {"status": "success", "data": "null", "message": ""}


@app.get("/api/stopwatchdog/{observatory}")
async def stop_watchdog(observatory: str):
    obs = OBSERVATORIES[observatory]

    obs.logger.info(f"User initiated stopping of watchdog from web interface")

    obs.watchdog_running = False

    return {"status": "success", "data": "null", "message": ""}


@app.get("/api/roboticswitch/{observatory}")
async def roboticswitch(observatory: str):
    obs = OBSERVATORIES[observatory]

    obs.logger.info(f"User initiated robotic switch from web interface")

    obs.toggle_robotic_switch()

    return {"status": "success", "data": obs.robotic_switch, "message": ""}


@app.get("/api/startschedule/{observatory}")
async def start_schedule(observatory: str):
    obs = OBSERVATORIES[observatory]

    obs.logger.info(f"User initiated starting of schedule from web interface")

    obs.start_schedule()

    return {"status": "success", "data": "null", "message": ""}


@app.get("/api/stopschedule/{observatory}")
async def stop_schedule(observatory: str):
    obs = OBSERVATORIES[observatory]

    obs.logger.info(f"User initiated stopping of schedule from web interface")

    obs.stop_schedule()

    return {"status": "success", "data": "null", "message": ""}


@app.get("/api/schedule/{observatory}")
async def schedule(observatory: str):
    obs = OBSERVATORIES[observatory]
    if obs.schedule_mtime != 0:
        schedule = obs.schedule

        schedule["start_HHMMSS"] = schedule["start_time"].apply(format_time)
        schedule["end_HHMMSS"] = schedule["end_time"].apply(format_time)

        # replace NaN with None
        schedule = schedule.where(pd.notnull(schedule), None)

        return schedule.to_dict(orient="records")
    else:
        return []


@app.get("/api/db/polling/{observatory}/{device_type}")
async def polling(observatory: str, device_type: str, day: float = 1):
    db = observatory_db(observatory)
    q = f"""SELECT * FROM polling WHERE device_type = '{device_type}' AND datetime > datetime('now', '-{day} day')"""

    df = pd.read_sql_query(q, db)

    db.close()

    # make new dataframe with f as columns and device_value as their values and datetime as index
    df = df.pivot(index="datetime", columns="device_command", values="device_value")

    # make sure your index is a datetime index
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()
    df = df.apply(pd.to_numeric, errors="coerce")

    latest = {}
    for col in df.columns:
        latest[col] = df[col].dropna().iloc[-1]

    # group by 60 seconds
    df_groupby = df.groupby(pd.Grouper(freq="60s")).mean()
    df_groupby = df_groupby.dropna()

    # safety limits, TODO: make this nicer
    obs = OBSERVATORIES[observatory]
    closing_limits = obs.config["ObservingConditions"][0]["closing_limits"]
    safety_limits = {}

    for key in closing_limits:
        safety_limits[key] = closing_limits[key]

        # find smallest value upper and largest value lower for each key
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


@app.get("/api/log/{observatory}")
async def log(observatory: str, datetime: str):
    db = observatory_db(observatory)
    q = f"""SELECT * FROM (SELECT * FROM log WHERE datetime < '{datetime}' ORDER BY datetime DESC LIMIT 100) a ORDER BY datetime ASC"""

    df = pd.read_sql_query(q, db)

    db.close()

    return df.to_dict(orient="records")


@app.websocket("/ws/log/{observatory}")
async def websocket_log(websocket: WebSocket, observatory: str):
    await websocket.accept()
    obs = OBSERVATORIES[observatory]

    db = observatory_db(observatory)
    q = """SELECT * FROM (SELECT * FROM log ORDER BY datetime DESC LIMIT 100) a ORDER BY datetime ASC"""
    initial_df = pd.read_sql_query(q, db)

    last_time = initial_df.datetime.iloc[-1]

    initial_log = initial_df.to_dict(orient="records")

    data_dict = {}
    data_dict["log"] = initial_log
    data_dict["schedule_mtime"] = obs.schedule_mtime

    socket = True

    try:
        await websocket.send_json(data_dict)
        await asyncio.sleep(1)
    except:
        print("log socket closed")
        socket = False

    while socket:
        if len(initial_log) > 0:
            q = f"""SELECT * FROM log WHERE datetime > '{last_time}'"""

        df = pd.read_sql_query(q, db)
        data = df.to_dict(orient="records")

        data_dict = {}
        data_dict["log"] = data
        data_dict["schedule_mtime"] = obs.schedule_mtime

        try:
            if len(data) > 0:
                last_time = df.datetime.iloc[-1]
            await websocket.send_json(data_dict)
            await asyncio.sleep(1)
        except:
            db.close()
            print("log socket closed")
            socket = False


@app.websocket("/ws/{observatory}")
async def websocket_endpoint(websocket: WebSocket, observatory: str):
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

        threads = [
            {"type": i["type"], "device_name": i["device_name"], "id": i["id"]}
            for i in obs.threads
        ]
        table0 = []
        table1 = [
            {"item": "error free", "value": obs.error_free},
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
                "value": "running" if obs.schedule_running else "stopped",
            },
            {
                "item": "robotic switch",
                "value": "on" if obs.robotic_switch else "off",
            },
            {"item": "weather safe", "value": "safe" if obs.weather_safe else "unsafe"},
            {
                "item": "error source",
                "value": "none" if len(obs.error_source) == 0 else "hover to see",
                "error_source": obs.error_source,
            },
            {"item": "threads", "value": len(threads), "threads": threads},
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
                    except:
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
                            "status": obs.guider[device_name].running,
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
                        except:
                            print(
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
            print(f"Error in websocket_endpoint: {e}")

        # if last_image_jpg is None:
        #     # use placeholder image
        #     last_image_jpg = "https://upload.wikimedia.org/wikipedia/commons/thumb/a/ac/No_image_available.svg/600px-No_image_available.svg.png"

        # TODO: need to make it less CPU intensive if multiple clients
        if LAST_IMAGE is not obs.last_image:
            LAST_IMAGE = obs.last_image
            LAST_IMAGE_JPG, USEFUL_HEADERS = convert_fits_to_jpg(
                LAST_IMAGE, observatory
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
        except:
            print("main socket closed")
            socket = False


@app.get("/autofocus", include_in_schema=False)
async def autofocus(request: Request):
    """TODO: Implement
    Pass the csv file and the fits file_names
    Call fits files in the csv file
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


@app.get("/{path:path}", include_in_schema=False)
async def serve_files(request: Request, path: str = ""):
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
    import argparse

    global DEBUG, TRUNCATE_SCHEDULE, SPECULOOS

    print(f"Astra version: {ASTRA_VER}")

    # TODO: add observatory tag
    parser = argparse.ArgumentParser(description="Run Astra")
    parser.add_argument("--debug", action="store_true", help="run in debug mode")
    parser.add_argument(
        "--truncate", action="store_true", help="run in truncate_schedule mode"
    )
    parser.add_argument(
        "--speculoos", action="store_true", help="run in speculoos mode"
    )
    args = parser.parse_args()

    logging.basicConfig(
        format="%(levelname)s,%(asctime)s.%(msecs)03d,%(process)d,%(name)s,(%(filename)s:%(lineno)d),%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        filename=CONFIG.paths.log_file,
        level=logging.DEBUG,
    )
    logging.Formatter.converter = time.gmtime

    if args.debug:
        DEBUG = True
        logging.getLogger().setLevel(logging.DEBUG)

    if args.truncate:
        TRUNCATE_SCHEDULE = True

    if args.speculoos:
        SPECULOOS = True

    # start the server
    log_level = "info" if not DEBUG else "debug"
    if log_level == "info":
        logging.getLogger().setLevel(logging.INFO)
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level=log_level,
        timeout_graceful_shutdown=None,
    )


if __name__ == "__main__":
    main()
