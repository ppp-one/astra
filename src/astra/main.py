import asyncio
import datetime
import logging
import os
import sqlite3
import time
from contextlib import asynccontextmanager
from glob import glob
from io import BytesIO

import httpx
import matplotlib.pyplot as plt
import pandas as pd
import uvicorn
from astropy.io import fits
from astropy.visualization import ZScaleInterval
from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from astra.astra_object import Astra

# change base directory to code/src
os.chdir(os.path.dirname(os.path.realpath(__file__)))

logging.basicConfig(
    format="%(levelname)s,%(asctime)s.%(msecs)03d,%(process)d,%(name)s,(%(filename)s:%(lineno)d),%(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    filename="../log/astra.log",
    level=logging.INFO,
)
logging.Formatter.converter = time.gmtime


frontend = Jinja2Templates(directory="frontend")
observatories = {}
webcamfeeds = {}
fws = {}
last_image = None
last_image_jpg = None
useful_headers = None
debug = False
truncate_schedule = False


def load_observatories():
    global observatories  # not sure if this is necessary
    global webcamfeeds
    global fws
    global debug

    config_dir = os.path.join("..", "config")
    config_files = glob(os.path.join(config_dir, "*.yml"))

    for config_filename in config_files:
        obs = Astra(config_filename, debug, truncate_schedule, speculoos=True)
        observatories[obs.observatory_name] = obs

        if "Misc" in obs.observatory:
            if "Webcam" in obs.observatory["Misc"]:
                webcamfeeds[obs.observatory_name] = obs.observatory["Misc"]["Webcam"]

        obs.connect_all()

        if "FilterWheel" in obs.devices:
            fws[obs.observatory_name] = {}
            for fw_name in obs.devices["FilterWheel"].keys():
                fws[obs.observatory_name][fw_name] = obs.devices["FilterWheel"][
                    fw_name
                ].get("Names")


def clean_up():
    pass


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
    old_img_path = os.path.join("frontend", f"*{observatory}*.jpg")
    for file in glob(old_img_path):
        os.remove(file)

    # Save the jpg image
    filename = os.path.splitext(os.path.basename(fits_file))[0] + ".jpg"
    filepath = os.path.join("frontend", filename)
    plt.imsave(filepath, image_data, format="jpg", cmap="gray", vmin=vmin, vmax=vmax)

    return filepath, headers


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load observatories
    load_observatories()
    yield
    # Clean up
    clean_up()


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def root(request: Request):
    return frontend.TemplateResponse(
        "index.html.j2",
        {
            "request": request,
            "observatories": observatories.keys(),
            "webcamfeeds": webcamfeeds,
        },
    )


@app.get("/favicon.svg", include_in_schema=False)
async def favicon():
    return FileResponse(os.path.join("frontend", "favicon.svg"))


@app.get("/js/{file}", include_in_schema=False)
async def js(file: str):
    return FileResponse(os.path.join("frontend", "js", file))


@app.get("/frontend/{image}", include_in_schema=False)
async def lastest_image(image: str):
    return FileResponse(os.path.join("frontend", image))


@app.get("/video/{observatory}/{filename:path}", include_in_schema=False)
async def get_video(request: Request, observatory, filename: str = None):
    headers = request.headers
    base_url = webcamfeeds[observatory]
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
    obs = observatories[observatory]

    return {"status": "success", "data": obs.heartbeat, "message": ""}


@app.get("/api/pausepolls/{observatory}")
def pause_polls(observatory: str):
    obs = observatories[observatory]

    obs.pause_polls()

    return {"status": "success", "data": "null", "message": ""}


@app.get("/api/resumepolls/{observatory}")
def resume_polls(observatory: str):
    obs = observatories[observatory]

    obs.resume_polls()

    return {"status": "success", "data": "null", "message": ""}


@app.get("/api/open/{observatory}")
def open_observatory(observatory: str):
    obs = observatories[observatory]

    obs.open_observatory()

    return {"status": "success", "data": "null", "message": ""}


@app.get("/api/close/{observatory}")
def close_observatory(observatory: str):
    obs = observatories[observatory]

    obs.close_observatory()

    return {"status": "success", "data": "null", "message": ""}


@app.get("/api/startwatchdog/{observatory}")
async def start_watchdog(observatory: str):
    obs = observatories[observatory]
    obs.error_free = True
    obs.error_source = []
    obs.start_watchdog()

    return {"status": "success", "data": "null", "message": ""}


@app.get("/api/stopwatchdog/{observatory}")
async def stop_watchdog(observatory: str):
    obs = observatories[observatory]
    obs.watchdog_running = False

    return {"status": "success", "data": "null", "message": ""}


@app.get("/api/startschedule/{observatory}")
async def start_schedule(observatory: str):
    obs = observatories[observatory]
    obs.start_schedule()

    return {"status": "success", "data": "null", "message": ""}


@app.get("/api/stopschedule/{observatory}")
async def stop_schedule(observatory: str):
    obs = observatories[observatory]
    obs.schedule_running = False

    return {"status": "success", "data": "null", "message": ""}


@app.get("/api/interrupt/{observatory}")
async def interrupt(observatory: str):
    obs = observatories[observatory]

    obs.start_toggle_interrupt()

    return {"status": "success", "data": "null", "message": ""}


@app.get("/api/ackastelos/{observatory}")
async def ackastelos(observatory: str):
    obs = observatories[observatory]

    obs.astelos_check_and_ack_error()

    return {"status": "success", "data": "null", "message": ""}


@app.get("/api/connect/{observatory}")
async def connect(observatory: str):
    obs = observatories[observatory]
    obs.connect_all()

    return {"status": "success", "data": "null", "message": ""}


@app.get("/api/schedule/{observatory}")
async def schedule(observatory: str):
    obs = observatories[observatory]
    schedule = obs.schedule

    schedule["start_HHMMSS"] = schedule["start_time"].apply(format_time)
    schedule["end_HHMMSS"] = schedule["end_time"].apply(format_time)

    # replace NaN with None
    schedule = schedule.where(pd.notnull(schedule), None)

    return schedule.to_dict(orient="records")


@app.get("/api/db/polling/{observatory}/{device_type}")
async def polling(observatory: str, device_type: str):
    db = sqlite3.connect(os.path.join("..", "log", observatory + ".db"))

    q = f"""SELECT * FROM polling WHERE device_type = '{device_type}' AND datetime > datetime('now', '-1 day')"""

    df = pd.read_sql_query(q, db)

    db.close()

    # make new dataframe with f as columns and device_value as their values and datetime as index
    df = df.pivot(index="datetime", columns="device_command", values="device_value")

    # make sure your index is a datetime index
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()
    df = df.apply(pd.to_numeric, errors="coerce")

    # group by 60 seconds
    df = df.groupby(pd.Grouper(freq="60s")).mean()
    df = df.dropna()

    return df.to_dict(orient="series")


@app.websocket("/ws/log/{observatory}")
async def websocket_log(websocket: WebSocket, observatory: str):
    await websocket.accept()
    obs = observatories[observatory]

    db = sqlite3.connect(os.path.join("..", "log", observatory + ".db"))

    q = """SELECT * FROM (SELECT * FROM log ORDER BY datetime DESC LIMIT 1000) a ORDER BY datetime ASC"""
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


@app.websocket("/ws/weather/{observatory}")
async def websocket_weather(websocket: WebSocket, observatory: str):
    # this + frontend need work...
    await websocket.accept()
    db = sqlite3.connect(os.path.join("..", "log", observatory + ".db"))

    # TODO: change to limit instead of datetime
    q = """SELECT * FROM polling WHERE device_type = 'ObservingConditions' AND datetime > datetime('now', '-1 day')"""

    initial_df = pd.read_sql_query(q, db)

    # make new dataframe with f as columns and device_value as their values and datetime as index
    initial_df = initial_df.pivot(
        index="datetime", columns="device_command", values="device_value"
    )

    # make sure your index is a datetime index
    initial_df.index = pd.to_datetime(initial_df.index)
    initial_df = initial_df.sort_index()
    initial_df = initial_df.apply(pd.to_numeric, errors="coerce")

    # group by 60 seconds
    initial_df = initial_df.groupby(pd.Grouper(freq="60s")).mean()
    initial_df = initial_df.dropna()

    last_time = initial_df.index[-1]

    # reset index
    initial_df = initial_df.reset_index()

    # convert datetime to string
    initial_df["datetime"] = initial_df["datetime"].astype(str)

    initial_data = initial_df.to_dict(orient="records")

    socket = True

    try:
        print(initial_data)
        await websocket.send_json(initial_data)
        await asyncio.sleep(1)
    except:
        db.close()
        print("weather socket closed")
        socket = False

    while socket:
        if len(initial_data) > 0:
            q = f"""SELECT * FROM polling WHERE device_type = 'ObservingConditions' AND datetime > '{last_time}'"""

        df = pd.read_sql_query(q, db)

        # make new dataframe with f as columns and device_value as their values and datetime as index
        df = df.pivot(index="datetime", columns="device_command", values="device_value")

        # make sure your index is a datetime index
        df.index = pd.to_datetime(df.index)
        df = df.sort_index()
        df = df.apply(pd.to_numeric, errors="coerce")

        # group by 60 seconds
        df = df.groupby(pd.Grouper(freq="60s")).mean()
        df = df.dropna()

        # reset index
        df = df.reset_index()

        # convert datetime to string
        df["datetime"] = df["datetime"].astype(str)

        data = df.to_dict(orient="records")

        try:
            if len(data) > 0:
                last_time = df.index[-1]
                print(data)
                await websocket.send_json(data)
            else:
                await websocket.send_json({})
            await asyncio.sleep(60)
        except:
            db.close()
            print("weather socket closed")
            socket = False


@app.websocket("/ws/{observatory}")
async def websocket_endpoint(websocket: WebSocket, observatory: str):
    global last_image, last_image_jpg, useful_headers

    await websocket.accept()

    obs = observatories[observatory]

    socket = True
    while socket:
        dt_now = datetime.datetime.utcnow()
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
                "value": datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            },
            {
                "item": "watchdog",
                "value": "running" if obs.watchdog_running else "stopped",
            },
            {
                "item": "schedule",
                "value": "running" if obs.schedule_running else "stopped",
            },
            {"item": "weather safe", "value": "safe" if obs.weather_safe else "unsafe"},
            {"item": "interrupt", "value": "on" if obs.interrupt else "off"},
            {
                "item": "error source",
                "value": "none" if len(obs.error_source) == 0 else "hover to see",
                "error_source": obs.error_source,
            },
            {"item": "threads", "value": len(threads), "threads": threads},
            {"item": "time to safe", "value": f"{obs.time_to_safe:.2f} mins"},
        ]

        if "Telescope" in obs.devices:
            # we want to know if slewing or tracking
            device_type = "Telescope"
            for device_name in polled_list[device_type].keys():
                polled = polled_list[device_type][device_name]

                tracking = polled["Tracking"]["value"]
                dt_tracking = polled["Tracking"]["datetime"]
                slewing = polled["Slewing"]["value"]
                dt_slewing = polled["Slewing"]["datetime"]

                status = "slewing" if slewing else "tracking" if tracking else "stopped"
                dt = dt_tracking if tracking else dt_slewing if slewing else dt_tracking

                try:
                    polled["RightAscension"]["value"] = polled["RightAscension"][
                        "value"
                    ] * (
                        360 / 24
                    )  # convert to degrees
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

                match shutter_status:
                    case 0:
                        status = "open"
                    case 1:
                        status = "closed"
                    case 2:
                        status = "opening"
                    case 3:
                        status = "closing"
                    case 4:
                        status = "error"
                    case _:
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
                        status = fws[observatory][device_name][pos]
                    except:
                        print(
                            f"FilterWheel {device_name} position {pos} not found in fws dict",
                            fws,
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

                match camera_status:
                    case 0:
                        status = "idle"
                    case 1:
                        status = "waiting"
                    case 2:
                        status = "exposing"
                    case 3:
                        status = "reading"
                    case 4:
                        status = "download"
                    case 5:
                        status = "error"
                    case _:
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

        # if last_image_jpg is None:
        #     # use placeholder image
        #     last_image_jpg = "https://upload.wikimedia.org/wikipedia/commons/thumb/a/ac/No_image_available.svg/600px-No_image_available.svg.png"

        # TODO: need to make it less CPU intensive if multiple clients
        if last_image is not obs.last_image:
            last_image = obs.last_image
            last_image_jpg, useful_headers = convert_fits_to_jpg(
                last_image, observatory
            )

        data = {
            "table0": table0,
            "table1": table1,
            "last_image": {"url": last_image_jpg, "useful_headers": useful_headers},
        }

        # make temp image, say how many images have been made?
        try:
            await websocket.send_json(data)
            await asyncio.sleep(1)
        except:
            print("main socket closed")
            socket = False


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run Astra")
    parser.add_argument("--debug", action="store_true", help="run in debug mode")
    parser.add_argument(
        "--truncate", action="store_true", help="run in truncate_schedule mode"
    )
    args = parser.parse_args()

    if args.debug:
        debug = True
        logging.getLogger().setLevel(logging.DEBUG)

    if args.truncate:
        truncate_schedule = True

    # start the server
    log_level = "info" if not debug else "debug"
    uvicorn.run(
        app, host="0.0.0.0", port=8000, log_level=log_level, timeout_graceful_shutdown=0
    )
