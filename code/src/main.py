import asyncio
import datetime
import sqlite3
from contextlib import asynccontextmanager
from glob import glob
from astropy.io import fits
import os
from PIL import Image

import pandas as pd
from astra import Astra

import uvicorn
from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import FileResponse
from fastapi.templating import Jinja2Templates



frontend = Jinja2Templates(directory="frontend")
observatories = {}
fws = {}
last_image = None
last_image_jpg = None
debug = False

def load_observatories():
    global observatories # not sure if this is necessary
    global fws

    kill_observatories()

    for config_filename in glob('../config/*.yml'):
        obs = Astra(config_filename, debug)
        observatories[obs.observatory_name] = obs
        obs.connect_all()

        if 'FilterWheel' in obs.devices:
            fws[obs.observatory_name] = {}
            for fw_name in obs.devices['FilterWheel'].keys():
                fws[obs.observatory_name][fw_name] = obs.devices['FilterWheel'][fw_name].get('Names')['data']

def kill_observatories():
    global observatories

    # TODO: kill processes (when it is a process)
    if len(observatories) > 0:
        print('Killing observatories')
        for obs in observatories.values():
            obs.disconnect_all()

        observatories = {}

def format_time(time : datetime.datetime):
    # if time is not NaTType:
    try:
        return time.strftime("%H:%M:%S")
    except:
        return None

def convert_fits_to_jpg(fits_file, observatory):
    # Open the FITS file
    with fits.open(fits_file) as hdulist:
        # Get the image data from the primary HDU
        image_data = hdulist[0].data

    # Normalize the image data to the 8-bit range (0-255)
    normalized_data = (image_data - image_data.min()) * (255.0 / (image_data.max() - image_data.min()))

    # Create an image from the normalized data
    image = Image.fromarray(normalized_data.astype('uint8'))

    # delete previous jpgs
    for file in glob(f'./frontend/*{observatory}*.jpg'):
        os.remove(file)
        
    # Save the jpg image
    filename = './frontend/' + fits_file.split('/')[-1].split('.')[0] + '.jpg'
    image.save(filename)

    return filename


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load observatories
    load_observatories()
    yield
    # Clean up
    kill_observatories()


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def root(request: Request):
    return frontend.TemplateResponse("index.html.j2", {"request": request, "observatories" : observatories.keys()})

@app.get('/favicon.svg', include_in_schema=False)
async def favicon():
    return FileResponse('./frontend/favicon.svg')

@app.get('/frontend/{image}', include_in_schema=False)
async def lastest_image(image: str):
    return FileResponse(f'./frontend/{image}')

@app.get("/api/start/{observatory}")
async def start(observatory: str):
    obs = observatories[observatory]
    obs.start_watchdog()

    return {"status": "success", "data": "null", "message": ""}

@app.get("/api/stop/{observatory}")
async def stop(observatory: str):
    obs = observatories[observatory]
    obs.schedule_running = False
    obs.watchdog_running = False

    obs.error_free = True

    return {"status": "success", "data": "null", "message": ""}

@app.get("/api/interrupt/{observatory}")
async def interrupt(observatory: str):
    obs = observatories[observatory]

    obs.toggle_interrupt()

    return {"status": "success", "data": "null", "message": ""}

@app.get("/api/connect/{observatory}")
async def connect(observatory: str):
    obs = observatories[observatory]
    obs.connect_all()

    return {"status": "success", "data": "null", "message": ""}

@app.get("/api/disconnect/{observatory}")
async def disconnect(observatory: str):
    obs = observatories[observatory]
    obs.disconnect_all()

    return {"status": "success", "data": "null", "message": ""}

@app.get("/api/schedule/{observatory}")
async def schedule(observatory: str):
    obs = observatories[observatory]
    schedule = obs.schedule
    
    schedule['start_HHMMSS'] = schedule['start_time'].apply(format_time)
    schedule['end_HHMMSS'] = schedule['end_time'].apply(format_time)

    # replace NaN with None
    schedule = schedule.where(pd.notnull(schedule), None)

    return schedule.to_dict(orient='records')

@app.get("/api/read_schedule/{observatory}")
async def read_schedule(observatory: str):
    obs = observatories[observatory]
    obs.schedule = obs.read_schedule()

    return {"status": "success", "data": "null", "message": ""}

@app.get("/api/db/polling/{observatory}/{device_type}")
async def polling(observatory: str, device_type: str):
    
    db = sqlite3.connect('../log/' + observatory + '.db')

    q = f"""SELECT * FROM polling WHERE device_type = '{device_type}' AND datetime > datetime('now', '-1 day')"""

    df = pd.read_sql_query(q, db)

    db.close()

    # make new dataframe with f as columns and device_value as their values and datetime as index
    df = df.pivot(index='datetime', columns='device_command', values='device_value')
    
    # make sure your index is a datetime index
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()
    df = df.apply(pd.to_numeric, errors='coerce')

    # group by 60 seconds
    df = df.groupby(pd.Grouper(freq='60s')).mean()
    df = df.dropna()


    return df.to_dict(orient='series')

@app.websocket("/ws/log/{observatory}")
async def websocket_log(websocket: WebSocket, observatory: str):
    await websocket.accept()
    db = sqlite3.connect('../log/' + observatory + '.db')
    
    q = """SELECT * FROM log WHERE datetime > datetime('now', '-1 day')"""
    initial_df = pd.read_sql_query(q, db)

    last_time = initial_df.datetime.iloc[-1]

    initial_data = initial_df.to_dict(orient='records')
    
    socket = True

    try:
        await websocket.send_json(initial_data)
        await asyncio.sleep(1)
    except:
        print("log socket closed")
        socket = False

    while socket:

        if len(initial_data) > 0:
            q = f"""SELECT * FROM log WHERE datetime > '{last_time}'"""
        
        df = pd.read_sql_query(q, db)
        data = df.to_dict(orient='records')

        try:
            if len(data) > 0:
                last_time = df.datetime.iloc[-1]
                await websocket.send_json(data)
            else:
                await websocket.send_json({})
            await asyncio.sleep(1)
        except:
            db.close()
            print("log socket closed")
            socket = False

@app.websocket("/ws/weather/{observatory}")
async def websocket_weather(websocket: WebSocket, observatory: str):
    await websocket.accept()
    db = sqlite3.connect('../log/' + observatory + '.db')
    
    q = """SELECT * FROM polling WHERE device_type = 'ObservingConditions' AND datetime > datetime('now', '-1 day')"""

    initial_df = pd.read_sql_query(q, db)

    # make new dataframe with f as columns and device_value as their values and datetime as index
    initial_df = initial_df.pivot(index='datetime', columns='device_command', values='device_value')
    
    # make sure your index is a datetime index
    initial_df.index = pd.to_datetime(initial_df.index)
    initial_df = initial_df.sort_index()
    initial_df = initial_df.apply(pd.to_numeric, errors='coerce')

    # group by 60 seconds
    initial_df = initial_df.groupby(pd.Grouper(freq='60s')).mean()
    initial_df = initial_df.dropna()

    last_time = initial_df.datetime.iloc[-1]

    initial_data = initial_df.to_dict(orient='series')
    
    socket = True

    try:
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
        df = df.pivot(index='datetime', columns='device_command', values='device_value')
        
        # make sure your index is a datetime index
        df.index = pd.to_datetime(df.index)
        df = df.sort_index()
        df = df.apply(pd.to_numeric, errors='coerce')

        # group by 60 seconds
        df = df.groupby(pd.Grouper(freq='60s')).mean()
        df = df.dropna()

        data = df.to_dict(orient='series')

        try:
            if len(data) > 0:
                last_time = df.datetime.iloc[-1]
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
    global last_image, last_image_jpg

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

                if polled['status'] == 'success':

                    if polled['data'] is not None: # not sure if correct to put this here, or later

                        polled_keys = polled['data'].keys()
                        for k in polled_keys:

                            polled_list[device_type][device_name][k] = {}
                            polled_list[device_type][device_name][k]['value'] = polled['data'][k]['value']
                            polled_list[device_type][device_name][k]['datetime'] = polled['data'][k]['datetime']

        # TODO: need to make it less CPU intensive if multiple clients
        if last_image is not obs.last_image:
            last_image = obs.last_image
            last_image_jpg = convert_fits_to_jpg(last_image, observatory)


        table0 = []
        table1 = [{"item": "error free" , "value" : obs.error_free},
                  {"item": "watchdog" , "value" : "running" if obs.watchdog_running else "stopped"},
                  {"item": "schedule" , "value" : "running" if obs.schedule_running else "stopped"},
                  {"item": "utc time" , "value" : datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")},
                  {"item": "local time" , "value" : datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}]

        if 'Telescope' in obs.devices:
            # we want to know if slewing or tracking
            device_type = 'Telescope'
            for device_name in polled_list[device_type].keys():
                polled = polled_list[device_type][device_name]

                tracking = polled['Tracking']['value']
                dt_tracking = polled['Tracking']['datetime']
                slewing = polled['Slewing']['value']
                dt_slewing = polled['Slewing']['datetime']

                status = 'slewing' if slewing else 'tracking' if tracking else 'stopped'
                dt = dt_tracking if tracking else dt_slewing if slewing else dt_tracking
                
                last_update = (dt_now - dt).total_seconds()

                valid = None
                # convert datetime to string and check if polled values are valid
                for key in polled:
                    polled[key]['datetime'] = polled[key]['datetime'].strftime("%Y-%m-%d %H:%M:%S")
                    if polled[key]['value'] != 'null' and valid is not False:
                        valid = True
                    else:
                        valid = False

                table0.append({"item": device_type, "name": device_name, "status": status, "valid": valid, "last_update": f'{last_update:.0f} s ago', "polled": polled})

        if 'Dome' in obs.devices:
            # we want to know if dome open or closed
            device_type = 'Dome'
            for device_name in polled_list[device_type].keys():
                polled = polled_list[device_type][device_name]

                shutter_status = polled['ShutterStatus']['value']
                
                match shutter_status:
                    case 0:
                        status = 'open'
                    case 1:
                        status = 'closed'
                    case 2:
                        status = 'opening'
                    case 3:
                        status = 'closing'
                    case 4:
                        status = 'error'
                    case _:
                        status = 'unknown'
                        
                dt = polled['ShutterStatus']['datetime']

                last_update = (dt_now - dt).total_seconds()

                valid = None
                # convert datetime to string and check if polled values are valid
                for key in polled:
                    polled[key]['datetime'] = polled[key]['datetime'].strftime("%Y-%m-%d %H:%M:%S")
                    if polled[key]['value'] != 'null' and valid is not False:
                        valid = True
                    else:
                        valid = False

                table0.append({"item": device_type, "name": device_name, "status": status, "valid": valid, "last_update": f'{last_update:.0f} s ago', "polled": polled})

        if 'FilterWheel' in obs.devices:
            # we want to know name of filter
            device_type = 'FilterWheel'
            for device_name in polled_list[device_type].keys():
                polled = polled_list[device_type][device_name]
                
                pos = polled['Position']['value']
                
                if pos == -1:
                    status = 'moving'
                else:
                    try:
                        status = fws[observatory][device_name][pos]
                    except:
                        print(f'FilterWheel {device_name} position {pos} not found in fws dict', fws)
                        status = 'unknown'

                dt = polled['Position']['datetime']

                last_update = (dt_now - dt).total_seconds()

                valid = None
                # convert datetime to string and check if polled values are valid
                for key in polled:
                    polled[key]['datetime'] = polled[key]['datetime'].strftime("%Y-%m-%d %H:%M:%S")
                    if polled[key]['value'] != 'null' and valid is not False:
                        valid = True
                    else:
                        valid = False

                table0.append({"item": device_type, "name": device_name, "status": status, "valid": valid, "last_update": f'{last_update:.0f} s ago', "polled": polled})

        if 'Camera' in obs.devices:

            device_type = 'Camera'
            for device_name in polled_list[device_type].keys():
                polled = polled_list[device_type][device_name]

                camera_status = polled['CameraState']['value']

                match camera_status:
                    case 0:
                        status = 'idle'
                    case 1:
                        status = 'waiting'
                    case 2:
                        status = 'exposing'
                    case 3:
                        status = 'reading'
                    case 4:
                        status = 'download'
                    case 5:
                        status = 'error'
                    case _:
                        status = 'unknown'

                dt = polled['CameraState']['datetime']

                last_update = (dt_now - dt).total_seconds()

                valid = None
                # convert datetime to string and check if polled values are valid
                for key in polled:
                    polled[key]['datetime'] = polled[key]['datetime'].strftime("%Y-%m-%d %H:%M:%S")
                    if polled[key]['value'] != 'null' and valid is not False:
                        valid = True
                    else:
                        valid = False

                table0.append({"item": device_type, "name": device_name, "status": status, "valid": valid, "last_update": f'{last_update:.0f} s ago', "polled": polled})

        if 'Focuser' in obs.devices:
                
            device_type = 'Focuser'
            for device_name in polled_list[device_type].keys():
                polled = polled_list[device_type][device_name]

                status = polled['Position']['value']

                dt = polled['Position']['datetime']

                last_update = (dt_now - dt).total_seconds()

                valid = None
                # convert datetime to string and check if polled values are valid
                for key in polled:
                    polled[key]['datetime'] = polled[key]['datetime'].strftime("%Y-%m-%d %H:%M:%S")
                    if polled[key]['value'] != 'null' and valid is not False:
                        valid = True
                    else:
                        valid = False

                table0.append({"item": device_type, "name": device_name, "status": status, "valid": valid, "last_update": f'{last_update:.0f} s ago', "polled": polled})

        if 'ObservingConditions' in obs.devices:

            device_type = 'ObservingConditions'
            for device_name in polled_list[device_type].keys():
                polled = polled_list[device_type][device_name]

                dt = polled['Temperature']['datetime']

                last_update = (dt_now - dt).total_seconds()

                valid = None
                status = None
                # convert datetime to string and check if polled values are valid
                for key in polled:
                    polled[key]['datetime'] = polled[key]['datetime'].strftime("%Y-%m-%d %H:%M:%S")
                    if polled[key]['value'] != 'null' and valid is not False:
                        valid = True
                        status = "valid"
                    else:
                        valid = False
                        status = "invalid"

                table0.append({"item": device_type, "name": device_name, "status": status, "valid": valid, "last_update": f'{last_update:.0f} s ago', "polled": polled})
                
        if 'SafetyMonitor' in obs.devices:

            device_type = 'SafetyMonitor'
            for device_name in polled_list[device_type].keys():
                polled = polled_list[device_type][device_name]

                safe = polled['IsSafe']['value']

                valid = None
                if safe is True:
                    status = 'safe'
                    valid = True
                else:
                    status = 'unsafe'
                    valid = False

                dt = polled['IsSafe']['datetime']

                last_update = (dt_now - dt).total_seconds()

                # convert datetime to string and check if polled values are valid
                for key in polled:
                    polled[key]['datetime'] = polled[key]['datetime'].strftime("%Y-%m-%d %H:%M:%S")
                    if polled[key]['value'] != 'null' and valid is not False:
                        valid = True
                    else:
                        valid = False

                table0.append({"item": device_type, "name": device_name, "status": status, "valid": valid, "last_update": f'{last_update:.0f} s ago', "polled": polled})


        data = {"table0" : table0,
                "table1" : table1,
                "last_image": {"url": last_image_jpg, "datetime": datetime.datetime.utcnow().isoformat()}
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
    parser = argparse.ArgumentParser(description='Run Astra')
    parser.add_argument('--debug', action='store_true', help='run in debug mode')
    args = parser.parse_args()

    if args.debug:
        debug = True
    else:
        debug = False

    # start the server
    uvicorn.run(app, host="0.0.0.0", port=8000)