"""
Script to calibrate the pulseGuide command

This is done by pointing the telescope at the meridian
and 0 deg Declination. The telescope is then pulseGuided
around the sky in a cross pattern, taking an image at each
location. DONUTS is then used to measure the shift and
determine the camera orientation and pulseGuide conversion
factors
"""
import time
from collections import defaultdict
import numpy as np
from donuts import Donuts
from donuts.image import Image
from scipy.ndimage import median_filter

from alpaca.telescope import *
from alpaca.camera import *
from alpaca.telescope import GuideDirections

from alpaca.exceptions import *
import astropy.io.fits as fits
import os

# pylint: disable=invalid-name
# pylint: disable=redefined-outer-name

class CustomImageClass(Image):
    def preconstruct_hook(self):
        clean = median_filter(self.raw_image, size=4, mode='mirror')
        band_corr = np.median(clean, axis=1).reshape(-1, 1)
        band_clean = clean - band_corr
        self.raw_image = band_clean

TELESCOPE_IP = '10.211.55.3:11111'
TELESCOPE_DEVICE_NUMBER = 0

CAMERA_IP = 'localhost:8080'
CAMERA_DEVICE_NUMBER = 0

def connectTelescope():
    """
    A reusable way to connect to ACP telescope
    """
    print("Connecting to telescope...")
    # myScope = win32com.client.Dispatch("ACP.Telescope")
    myScope = Telescope(TELESCOPE_IP, TELESCOPE_DEVICE_NUMBER)
    try:
        myScope.Connected = True
        SCOPE_READY = myScope.Connected
        myScope.Unpark()
        myScope.Tracking = True
        print('Telescope connected')
    except:
        print('WARNING: CANNOT CONNECT TO TELESCOPE')
        SCOPE_READY = False
    return myScope, SCOPE_READY

def connectCamera():
    """
    A reusable way of checking camera connection

    The camera needs treated slightly differently. We
    have to try connecting before we can tell if
    connected or not. Annoying!
    """
    print("Connecting to camera...")
    myCamera = Camera(CAMERA_IP, CAMERA_DEVICE_NUMBER)
    try:
        myCamera.Connected = True
        CAMERA_READY = True
        print('Camera connected')
    except AttributeError:
        print('WARNING: CANNOT CONNECT TO CAMERA')
        CAMERA_READY = False
    return myCamera, CAMERA_READY

def takeImageWithMaxIm(camera_object : Camera, image_path, filter_id=2,
                       exptime=1, t_settle=1):
    """
    Take an image with MaxImDL
    """
    print('Waiting {}s to settle...'.format(t_settle))
    time.sleep(t_settle)

    print('Taking image...')
    dateobs = datetime.utcnow()
    maxadu = camera_object.MaxADU

    camera_object.StartExposure(Duration=exptime, Light=1)

    while not camera_object.ImageReady:
        time.sleep(0.1)
    print('Image ready...')

    hdr = fits.Header()
    hdr['FILTER'] = ('none', 'Filter name')
    hdr['EXPTIME'] = (exptime, 'Exposure time (s)')
    hdr['IMAGETYP'] = ('Light', 'Image type')

    save_image(camera_object, hdr, dateobs, maxadu, image_path)

    print('{} saved...'.format(image_path))

def save_image(device : Camera, hdr : fits.Header, dateobs : datetime, maxadu : int, filename : str) -> str:
    '''
    Save an image to disk.

    This function retrieves an image from an Alpaca device, transforms it, and saves it to disk in FITS format.
    The filename is generated based on device information and the image's characteristics.

    The FITS header is updated with the 'DATE-OBS' and 'DATE' keywords to record the exposure start time
    and the time when the file was written.

    After saving the image, it is logged, and its file path is returned.

    Parameters:
        device (AlpacaDevice): The camera from which to retrieve the image.
        hdr (fits.Header): The FITS header associated with the image.
        dateobs (datetime): The UTC date and time of exposure start.
        t0 (datetime): The starting time of the image acquisition.
        maxadu (int): The maximum analog-to-digital unit value for the image.
        folder (str): The folder where the image will be saved.

    Returns:
        str: The file path to the saved image.

    '''
    if not os.path.exists(os.path.join('..', 'images', 'calibrate_guiding')):
        print('Creating directory: {}'.format(os.path.join('..', 'images', 'calibrate_guiding')))
        os.makedirs(os.path.join('..', 'images', 'calibrate_guiding'))
        print('Directory created')

    arr = device.ImageArray

    img = np.array(arr)
    
    nda = img_transform(device, img, maxadu) ## TODO: make more efficient?

    hdr['DATE-OBS'] = (dateobs.strftime('%Y-%m-%dT%H:%M:%S.%f'), 'UTC date/time of exposure start')  

    date = datetime.utcnow() 
    hdr['DATE'] = (date.strftime('%Y-%m-%dT%H:%M:%S.%f'), 'UTC date/time when this file was written')  

    hdu = fits.PrimaryHDU(nda, header=hdr)

    hdu.writeto(os.path.join('..', 'images', 'calibrate_guiding', os.path.basename(filename)))

def img_transform(device : Camera, img : np.array, maxadu : int) -> np.array:
    '''
    This function takes in a device object, an image object, and a maximum ADU 
    value and returns a numpy array of the correct shape for astropy.io.fits.

    Parameters:
        device (AlpacaDevice): A device object that contains the ImageArrayInfo data.
        img (np.array): An image object that contains the image data.
        maxadu (int): The maximum ADU value.

    Returns:
        nda (np.array): A numpy array of the correct shape for astropy.io.fits.
    '''
    
    imginfo = device.ImageArrayInfo

    # Determine the image data type
    if imginfo.ImageElementType == 0 or imginfo.ImageElementType == 1:
        imgDataType = np.uint16
    elif imginfo.ImageElementType == 2:
        if maxadu <= 65535:
            imgDataType = np.uint16 # Required for BZERO & BSCALE to be written
        else:
            imgDataType = np.int32
    elif imginfo.ImageElementType == 3:
        imgDataType = np.float64
    else:
        raise ValueError(f"Unknown ImageElementType: {imginfo.ImageElementType}")
    

    # Make a numpy array of he correct shape for astropy.io.fits
    if imginfo.Rank == 2:
        nda = np.array(img, dtype=imgDataType).transpose()
    else:
        nda = np.array(img, dtype=imgDataType).transpose(2,1,0)

    return nda

def pulseGuide(scope : Telescope, direction_int, duration):
    """
    Move the telescope along a given direction
    for the specified amount of time
    """
    print('Pulse guiding {} for {}ms'.format(direction_int, duration))

    match direction_int:
        case 0:
            direction = GuideDirections.guideNorth
        case 1:
            direction = GuideDirections.guideSouth
        case 2:
            direction = GuideDirections.guideEast
        case 3:
            direction = GuideDirections.guideWest
        case _:
            print('Invalid direction')

    print('Pulse guiding {} for {}ms'.format(direction, duration))
    

    scope.PulseGuide(direction, duration)
    while scope.IsPulseGuiding == True:
        # print('Pulse guiding...')
        time.sleep(0.1)

    while scope.Slewing == True:
        print('Slewing...')
        time.sleep(0.1)

    ra = (scope.RightAscension/24) * 360
    dec = scope.Declination
    print(ra, dec)

def determineShiftDirectionMagnitude(shft):
    """
    Take a donuts shift object and work out
    the direction of the shift and the distance
    """
    sx = shft.x.value
    sy = shft.y.value
    if abs(sx) > abs(sy):
        if sx > 0:
            direction = '-x'
        else:
            direction = '+x'
        magnitude = abs(sx)
    else:
        if sy > 0:
            direction = '-y'
        else:
            direction = '+y'
        magnitude = abs(sy)
    return direction, magnitude

def newFilename(direction, pulse_time,
                image_id):
    """
    Generate new FITS image name
    """
    filename = "step_{:03d}_d{}_{}ms.fits".format(image_id, direction, pulse_time)
    
    filepath = os.path.join('..', 'images', 'calibrate_guiding', filename)

    image_id += 1
    return filepath, image_id

if __name__ == "__main__":
    pulse_time = 5000
    
    # set up objects to hold calib info
    DIRECTION_STORE = defaultdict(list)
    SCALE_STORE = defaultdict(list)
    image_id = 0

    # connect to hardware
    myScope, SCOPE_READY = connectTelescope()
    myCamera, CAMERA_READY = connectCamera()

    time.sleep(1)
    # start the calibration run
    print("Starting calibration run...")
    ref_image, image_id = newFilename('R', 0, image_id)
    takeImageWithMaxIm(myCamera, ref_image)

    # set up donuts with this reference point. Assume default params for now
    donuts_ref = Donuts(ref_image, normalise=False, subtract_bkg=True, downweight_edges=False, image_class=CustomImageClass)

    # loop over 10 cycles of the U, D, L, R nudging to determine
    # the scale and orientation of the camera
    for i in range(10):
        # loop pver 4 directions, 0 to 3
        for j in range(4):
            # pulse guide the telescope
            pulseGuide(myScope, j, pulse_time)
            
            # take an image
            check, image_id = newFilename(j, pulse_time, image_id)

            takeImageWithMaxIm(myCamera, check)

            # now measure the shift
            shift = donuts_ref.measure_shift(check)
            direction, magnitude = determineShiftDirectionMagnitude(shift)

            print(direction, magnitude)
            DIRECTION_STORE[j].append(direction)
            SCALE_STORE[j].append(magnitude)

            # now update the reference image
            donuts_ref = Donuts(check, normalise=False, subtract_bkg=True, downweight_edges=False, image_class=CustomImageClass)

    # now do some analysis on the run from above
    # check that the directions are the same every time for each orientation
    config = {'PIX2TIME' : {'+x': None, '-x': None, '+y': None, '-y': None},
            'RA_AXIS' : None,
            'DIRECTIONS' : {'+x': None, '-x': None, '+y': None, '-y': None}}
    
    print("Configuration:", end='\n\n')

    for i, dir in enumerate(DIRECTION_STORE):
        assert len(set(DIRECTION_STORE[dir])) == 1

        xy = DIRECTION_STORE[dir][0]
        match dir:
            case 0:
                direction = "North"
            case 1:
                direction = "South"
            case 2:
                direction = "East"
                if xy == '+x' or xy == '-x':
                    config['RA_AXIS'] = 'x'
                else:
                    config['RA_AXIS'] = 'y'
            case 3:
                direction = "West"
            case _:
                direction = "Invalid direction"
                print('Invalid direction')

        config['PIX2TIME'][xy] = pulse_time/np.average(SCALE_STORE[dir])
        config['DIRECTIONS'][xy] = direction

    # print dict as yml
    for key in config:
        if isinstance(config[key], dict):
            print('{}:'.format(key))
            for subkey in config[key]:
                if isinstance(config[key][subkey], str):
                    print('  \'{}\': {}'.format(subkey, config[key][subkey]))
                else:
                    print('  \'{}\': {}'.format(subkey, config[key][subkey]))
        else:
            if isinstance(config[key], str):
                print('{}: \'{}\''.format(key, config[key]))
            else:
                print('{}: {}'.format(key, config[key]))
