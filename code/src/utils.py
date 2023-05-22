import math
import os
from datetime import datetime, timedelta

import astropy.units as u
import numpy as np
import pandas as pd
from astropy.coordinates import AltAz, get_sun
from astropy.time import Time


def __to_format(jd: float, fmt: str) -> float:
    """
    Converts a Julian Day object into a specific format.  For
    example, Modified Julian Day.
    Parameters
    ----------
    jd: float
    fmt: str

    Returns
    -------
    jd: float
    """
    if fmt.lower() == 'jd':
        return jd
    elif fmt.lower() == 'mjd':
        return jd - 2400000.5
    elif fmt.lower() == 'rjd':
        return jd - 2400000
    else:
        raise ValueError('Invalid Format')

def to_jd(dt: datetime, fmt: str = 'jd') -> float:
    """
    Converts a given datetime object to Julian date.
    Algorithm is copied from https://en.wikipedia.org/wiki/Julian_day
    All variable names are consistent with the notation on the wiki page.

    Parameters
    ----------
    fmt
    dt: datetime
        Datetime object to convert to MJD

    Returns
    -------
    jd: float
    """
    a = math.floor((14-dt.month)/12)
    y = dt.year + 4800 - a
    m = dt.month + 12*a - 3

    jdn = dt.day + math.floor((153*m + 2)/5) + 365*y + math.floor(y/4) - math.floor(y/100) + math.floor(y/400) - 32045

    jd = jdn + (dt.hour - 12) / 24 + dt.minute / 1440 + dt.second / 86400 + dt.microsecond / 86400000000

    return __to_format(jd, fmt)

def getLightTravelTimes(target, time_to_correct):
    """
    Get the light travel times to the helio- and
    barycentres
    Parameters
    ----------
    ra : str
        The Right Ascension of the target in degrees
    dec : str
        The Declination of the target in degrees
    time_to_correct : astropy.Time object
        The time of observation to correct. The astropy.Time
        object must have been initialised with an EarthLocation
    Returns
    -------
    ltt_bary : float
        The light travel time to the barycentre
    ltt_helio : float
        The light travel time to the heliocentre
    Raises
    ------
    None
    """
    
    ltt_bary = time_to_correct.light_travel_time(target)
    ltt_helio = time_to_correct.light_travel_time(target, 'heliocentric')
    return ltt_bary, ltt_helio


def time_conversion(jd, location, target):

    time_inp = Time(jd, format='jd', scale='utc', location=location)

    mjd = time_inp.mjd

    ltt_bary, ltt_helio = getLightTravelTimes(target, time_inp)
    
    hjd = (time_inp + ltt_helio).value
    bjd = (time_inp.tdb + ltt_bary).value

    return mjd, hjd, bjd

def create_image_dir():
    folder = (datetime.utcnow() - timedelta(days=0.5)).strftime("%Y%m%d")
    mypath = f"../images/{folder}"
    if not os.path.isdir(mypath):
        os.makedirs(mypath)
    return folder

def interpolate_dfs(index, *data):
    '''
    Interpolates panda dataframes onto an index, of same index type (e.g. wavelength in microns)
    Parameters
    ----------
    index: 1d array which data is to be interpolated onto
    data:       Pandas dataframes 
    Returns
    -------
    df: Interpolated dataframe
    '''
    df = pd.DataFrame({'tmp': index}, index=index)
    for dat in data:
        dat = dat[~dat.index.duplicated(keep='first')]
        df = pd.concat([df, dat], axis=1)        
    df = df.sort_index()
    df = df.interpolate(method='index', axis=0).reindex(index)
    df = df.drop(labels='tmp', axis=1)

    return df

def hdr_times(hdr, fits_config, location, target):
    dateobs = pd.to_datetime(hdr['DATE-OBS'])# = (dateobs.strftime('%Y-%m-%dT%H:%M:%S.%f'), 'UTC date/time of exposure start')

    dateend = dateobs + timedelta(seconds=hdr['EXPTIME'])
    jd = to_jd(dateobs)
    jdend = to_jd(dateobs)

    mjd, hjd, bjd = time_conversion(jd, location, target)
    mjdend, hjdend, bjdend = time_conversion(jdend, location, target)


    for i, row in fits_config[fits_config['fixed'] is False].iterrows():

        if row['device_type'] == 'astra':
            match row['header']:
                case 'JD':
                    hdr[row['header']] = (jd, row['comment'])
                case 'JD-HELIO':
                    hdr[row['header']] = (hjd, row['comment'])
                case 'JD-OBS':
                    hdr[row['header']] = (jd, row['comment'])
                case 'HJD-OBS':
                    hdr[row['header']] = (hjd, row['comment'])
                case 'BJD-OBS':
                    hdr[row['header']] = (bjd, row['comment']) 
                case 'MJD-OBS':
                    hdr[row['header']] = (mjd, row['comment']) # TODO: Check this
                case 'MJD-END':
                    hdr[row['header']] = (mjdend, row['comment']) 
                case 'DATE-END':
                    hdr[row['header']] = (dateend.strftime('%Y-%m-%dT%H:%M:%S.%f'), row['comment']) 
                case _:
                    if row['header'] not in hdr:
                        # display(row['header'])
                        # print(row['header'], " Yikers. I don't know that one.")
                        print(row['header'])
    
    z = (90 - hdr['ALTITUDE']) * np.pi/180
    hdr['AIRMASS'] = (1.002432*np.cos(z)**2 + 0.148386*np.cos(z) + 0.0096467) /  (np.cos(z)**3 + 0.149864*np.cos(z)**2 + 0.0102963*np.cos(z) + 0.000303978) # https://doi.org/10.1364/AO.33.001108, https://en.wikipedia.org/wiki/Air_mass_(astronomy)



def is_sun_rising(obs_location):
    # sun's position now
    obs_time0 = Time.now()
    sun_position0 = get_sun(obs_time0)
    sun_altaz0 = sun_position0.transform_to(AltAz(obstime=obs_time0, location=obs_location))

    # sun's position in 5 minutes
    obs_time1 = obs_time0 + 5 * u.minute
    sun_position1 = get_sun(obs_time1)
    sun_altaz1 = sun_position1.transform_to(AltAz(obstime=obs_time1, location=obs_location))

    # determine if sun is moving up or down by looking at gradient
    sun_altaz_grad = (sun_altaz1.alt.degree - sun_altaz0.alt.degree) / (obs_time1 - obs_time0).sec

    sun_rising = None
    if sun_altaz_grad > 0:
        sun_rising = True
    else:
        sun_rising = False
    
    flat_ready = False
    if sun_rising:
        if sun_altaz0.alt.deg > -10 and sun_altaz0.alt.deg < -1:
            flat_ready = True
    else:
        if sun_altaz0.alt.deg < -1:
            flat_ready = True

    return sun_rising, flat_ready, sun_altaz0