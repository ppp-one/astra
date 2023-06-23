import numpy as np
from math import (
    radians,
    sin,
    cos)

import os

from astropy.io import fits
import glob as g

from datetime import datetime

from collections import defaultdict

import time

from shutil import copyfile

from PID import PID
from donuts import Donuts
from donuts.image import Image

from scipy.ndimage import median_filter # todo: check this is the right one

"""
Confiuguration parameters
"""

# header keyword for the current filter
FILTER_KEYWORD = 'FILTER'

# header keyword for the current target/field
FIELD_KEYWORD = 'OBJECT'

# RA axis alignment along x or y?
RA_AXIS = 'x'

# guider log file name
LOGFILE = "guider.log"

# rejection buffer length
GUIDE_BUFFER_LENGTH = 20

# number images allowed during pull in period
IMAGES_TO_STABILISE = 10

# outlier rejection sigma
SIGMA_BUFFER = 10

# pulseGuide conversions
PIX2TIME = {'+x': 61.77,
            '-x': 61.78,
            '+y': 61.87,
            '-y': 61.78}

# guide directions
DIRECTIONS = {'-y': 0, '+y': 1, '+x': 2, '-x': 3}

# max allowed shift to correct
MAX_ERROR_PIXELS = 20

# max alloed shift to correct during stabilisation
MAX_ERROR_STABIL_PIXELS = 40

# PID loop coefficients
PID_COEFFS = {'x': {'p': 0.70, 'i': 0.02, 'd': 0.0},
              'y': {'p': 0.50, 'i': 0.02, 'd': 0.0},
              'set_x': 0.0,
              'set_y': 0.0}


class CustomImageClass(Image):
    def preconstruct_hook(self):
        clean = median_filter(self.raw_image, size=4, mode='mirror')
        band_corr = np.median(clean, axis=1).reshape(-1, 1)
        band_clean = clean - band_corr
        self.raw_image = band_clean

class Guider():
    def __init__(self, camera, telescope, cursor, glob_str):

        # pass in objects from astra
        self.telescope = telescope
        self.camera = camera
        self.cursor = cursor # I think this is the way - how is it done in the astra class?

        # set up the database
        self.create_tables() # this is assuming we're using the same db.  Should we have a separate one for guiding?

        # set up the image glob string
        self.glob_str = glob_str # e.g. './images/20230621/io_trappist_z_10_*.fits'
        self.reference_dir = './images/autoguider_ref/'

        # set up variables
        self.BUFF_X, self.BUFF_Y = [], []
        self.PIDx, self.PIDy = None, None

        self.running = False

    def create_tables(self):
        '''
        Create a database for donuts
        '''
                    
        db_command_0 = """CREATE TABLE IF NOT EXISTS autoguider_ref (
                ref_id mediumint auto_increment primary key,
                field varchar(100) not null,
                telescope varchar(20) not null,
                ref_image varchar(100) not null,
                filter varchar(20) not null,
                valid_from datetime not null,
                valid_until datetime
                );"""

        self.cursor.execute(db_command_0)
        
        db_command_1 = """CREATE TABLE IF NOT EXISTS autoguider_log_new (
                updated timestamp default current_timestamp,
                night date not null,
                reference varchar(150) not null,
                comparison varchar(150) not null,
                stabilised varchar(5) not null,
                shift_x double not null,
                shift_y double not null,
                pre_pid_x double not null,
                pre_pid_y double not null,
                post_pid_x double not null,
                post_pid_y double not null,
                std_buff_x double not null,
                std_buff_y double not null,
                culled_max_shift_x varchar(5) not null,
                culled_max_shift_y varchar(5) not null
                );
                """

        self.cursor.execute(db_command_1)
        
        db_command_2 = """CREATE TABLE IF NOT EXISTS autoguider_info_log (
                message_id INTEGER PRIMARY KEY AUTOINCREMENT,
                updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                telescope varchar(20) NOT NULL,
                message varchar(500) NOT NULL
                );
                """

        self.cursor.execute(db_command_2)

    def logShiftsToDb(self, qry_args):
        """
        Log the autguiding information to the database

        Parameters
        ----------
        qry_args : array like
            Tuple of items to log in the database.
            See itemised list in logShiftsToFile docstring

        Returns
        -------
        None

        Raises
        ------
        None
        """
        qry = """
            INSERT INTO autoguider_log_new
            (night, reference, comparison, stabilised, shift_x, shift_y,
            pre_pid_x, pre_pid_y, post_pid_x, post_pid_y, std_buff_x,
            std_buff_y, culled_max_shift_x, culled_max_shift_y)
            VALUES
            ('%s', '%s', '%s', '%s', '%s', '%s', '%s',
            '%s', '%s', '%s', '%s', '%s', '%s', '%s')
            """
        
        self.cursor.execute(qry%qry_args)

    def logMessageToDb(self, message):
        """
        Log outout messages to the database

        Parameters
        ----------
        telescope : str
            Name of the instrument being autoguided
        message : str
            Output message to log

        Returns
        -------
        None

        Raises
        ------
        None
        """
        qry = """
            INSERT INTO autoguider_info_log
            (telescope, message)
            VALUES
            ('%s', '%s')
            """
        qry_args = (self.camera.device_name, message)
        self.cursor.execute(qry%qry_args)

# TODO: change location of logfile to be in the same directory as the data
    def logShiftsToFile(self, logfile, loglist, header=False):
        """
        Log the guide corrections to disc. This log is
        typically located with the data files for each night

        Parameters
        ----------
        logfile : string
            Path to the logfile
        log_list : array like
            List of items to log, see order of items below:
            night : string
                Date of the night
            ref : string
                Name of the current reference image
            check : string
                Name of the current guide image
            stabilised : string
                Telescope stabilised yet? (y | n)
            shift_x : float
                Raw shift measured in X direction
            shift_y : float
                Raw shift measured in Y direction
            pre_pid_X : float
                X correction sent to the PID loop
            pre_pid_y : float
                Y correction sent to the PID loop
            post_pid_X : float
                X correction sent to the mount, post PID loop
            post_pid_y : float
                Y correction sent to the mount, post PID loop
            std_buff_x : float
                Sttdev of X AG value buffer
            std_buff_y : float
                Sttdev of Y AG value buffer
            culled_max_shift_x : string
                Culled X measurement if > max allowed shift (y | n)
            culled_max_shift_y : string
                Culled Y measurement if > max allowed shift (y | n)
        header : boolean
            Flag to set writing the log file header. This is done
            at the start of the night only

        Returns
        -------
        None

        Raises
        ------
        None
        """
        if header:
            line = "night  ref  check  stable  shift_x  shift_y  pre_pid_x  pre_pid_y  " \
                "post_pid_x  post_pid_y  std_buff_x  std_buff_y  culled_x  culled_y"
        else:
            line = "  ".join(loglist)
        with open(logfile, "a") as outfile:
            outfile.write("{}\n".format(line))

    # todo: add astra code
    def guide(self, x, y, images_to_stabilise, gem=False):
        """
        Generic autoguiding command with built-in PID control loop
        guide() will track recent autoguider corrections and ignore
        abnormally large offsets. It will also handle orientation
        and scale conversions as per the telescope specific config
        file.

        During the initial field stabilisation period the limits are
        relaxed slightly and large pull in errors are not appended
        to the steady state outlier rejection buffer

        Parameters
        ----------
        x : float
            Guide correction to make in X direction
        y : float
            Guide correction to make in Y direction
        images_to_stabilise : int
            Number of images before field is stabilised
            If -ve, field has stabilised
            If +ve allow for bigger shifts and do not append
            ag values to buffers
        gem : boolean
            Are we using a German Equatorial Mount?
            Default = False
            If so, the side of the pier matters for correction
            directions. Ping the mount for pierside before applying
            a correction. If this turns out to be slow, we can do so
            only when in the HA range for a pier flip

        Returns
        -------
        success : boolean
            was the correction applied? Proxy for telescope connected
        pidx : float
            X correction actually sent to the mount, post PID
        pidy : float
            Y correction actually sent to the mount, post PID
        sigma_x : float
            Stddev of X buffer
        sigma_y : float
            Stddev of Y buffer

        Raises
        ------
        None
        """

        #TODO: add scope connection
        connected = self.telescope.get('Connected')['data']
        if connected:
            # get telescope declination to scale RA corrections
            dec = self.telescope.get('Declination')['data']
            dec_rads = radians(dec)
            cos_dec = cos(dec_rads)
            # pop the earliest buffer value if > 30 measurements
            while len(self.BUFF_X) > GUIDE_BUFFER_LENGTH:
                self.BUFF_X.pop(0)
            while len(self.BUFF_Y) > GUIDE_BUFFER_LENGTH:
                self.BUFF_Y.pop(0)
            assert len(self.BUFF_X) == len(self.BUFF_Y)
            if images_to_stabilise < 0:
                CURRENT_MAX_SHIFT = MAX_ERROR_PIXELS
                # kill anything that is > sigma_buffer sigma buffer stats
                if len(self.BUFF_X) < GUIDE_BUFFER_LENGTH and len(self.BUFF_Y) < GUIDE_BUFFER_LENGTH:
                    self.logMessageToDb('Filling AG stats buffer...')
                    sigma_x = 0.0
                    sigma_y = 0.0
                else:
                    sigma_x = np.std(self.BUFF_X)
                    sigma_y = np.std(self.BUFF_Y)
                    if abs(x) > SIGMA_BUFFER * sigma_x or abs(y) > SIGMA_BUFFER * sigma_y:
                        self.logMessageToDb('Guide error > {} sigma * buffer errors, ignoring...'.format(SIGMA_BUFFER))
                        # store the original values in the buffer, even if correction
                        # was too big, this will allow small outliers to be caught
                        self.BUFF_X.append(x)
                        self.BUFF_Y.append(y)
                        return True, 0.0, 0.0, sigma_x, sigma_y
                    else:
                        pass
            else:
                self.logMessageToDb('Ignoring AG buffer during stabilisation')
                CURRENT_MAX_SHIFT = MAX_ERROR_STABIL_PIXELS
                sigma_x = 0.0
                sigma_y = 0.0

            # update the PID controllers, run them in parallel
            pidx = self.PIDx.update(x) * -1
            pidy = self.PIDy.update(y) * -1

            # check if we are stabilising and allow for the max shift
            if images_to_stabilise > 0:
                if pidx >= CURRENT_MAX_SHIFT:
                    pidx = CURRENT_MAX_SHIFT
                elif pidx <= -CURRENT_MAX_SHIFT:
                    pidx = -CURRENT_MAX_SHIFT
                if pidy >= CURRENT_MAX_SHIFT:
                    pidy = CURRENT_MAX_SHIFT
                elif pidy <= -CURRENT_MAX_SHIFT:
                    pidy = -CURRENT_MAX_SHIFT
            self.logMessageToDb("PID: {0:.2f}  {1:.2f}".format(float(pidx), float(pidy)))

            # make another check that the post PID values are not > Max allowed
            # using >= allows for the stabilising runs to get through
            # abs() on -ve duration otherwise throws back an error
            if pidy > 0 and pidy <= CURRENT_MAX_SHIFT:
                guide_time_y = pidy * PIX2TIME['+y']
                if RA_AXIS == 'y':
                    guide_time_y = guide_time_y/cos_dec
                self.telescope.get('PulseGuide')['data'](Direction=DIRECTIONS['+y'], Duration=guide_time_y)
            if pidy < 0 and pidy >= -CURRENT_MAX_SHIFT:
                guide_time_y = abs(pidy * PIX2TIME['-y'])
                if RA_AXIS == 'y':
                    guide_time_y = guide_time_y/cos_dec
                self.telescope.get('PulseGuide')['data'](Direction=DIRECTIONS['-y'], Duration=guide_time_y)
            
            # TODO: add timeout
            while self.telescope.get('IsPulseGuiding')['data']:
                time.sleep(0.01)
                
            if pidx > 0 and pidx <= CURRENT_MAX_SHIFT:
                guide_time_x = pidx * PIX2TIME['+x']
                if RA_AXIS == 'x':
                    guide_time_x = guide_time_x/cos_dec
                self.telescope.get('PulseGuide')['data'](Direction=DIRECTIONS['+x'], Duration=guide_time_x)

            if pidx < 0 and pidx >= -CURRENT_MAX_SHIFT:
                guide_time_x = abs(pidx * PIX2TIME['-x'])
                if RA_AXIS == 'x':
                    guide_time_x = guide_time_x/cos_dec
                self.telescope.get('PulseGuide')['data'](Direction=DIRECTIONS['-x'], Duration=guide_time_x)

            # TODO: add timeout
            while self.telescope.get('IsPulseGuiding')['data']:
                time.sleep(0.01)

            self.logMessageToDb("Guide correction Applied")
            # store the original values in the buffer
            # only if we are not stabilising
            if images_to_stabilise < 0:
                self.BUFF_X.append(x)
                self.BUFF_Y.append(y)
            return True, pidx, pidy, sigma_x, sigma_y
        else:
            self.logMessageToDb("Telescope NOT connected!")
            self.logMessageToDb("Please connect Telescope via ACP!")
            self.logMessageToDb("Ignoring corrections!")
            return False, 0.0, 0.0, 0.0, 0.0

    # where is this used?
    def rotateAxes(self, x, y, theta):
        """
        Take a correction in X and Y and rotate it
        by the known position angle of the camera

        This function accounts for non-orthogonalty
        between a camera's X/Y axes and the RA/Dec
        axes of the sky

        x' = x*cos(theta) + y*sin(theta)
        y' = -x*sin(theta) + y*cos(theta)

        Parameters
        -----------

        Returns
        -------

        Raises
        ------
        """
        x_new = x*cos(radians(theta)) + y*sin(radians(theta))
        y_new = -x*sin(radians(theta)) + y*cos(radians(theta))
        return x_new, y_new

    # where is this used?
    def splitObjectIdIntoPidCoeffs(self, filename):
        """
        Take the special filename and pull out the coeff values

        Name should have the format:
            PXX.xx-IYY.yy-DZZ.zz

        If not, None is returned and this will force the
        PID coeffs back to the configured value

        Parameters
        ----------
        filename : string
            name of the file to extract PID coeffs from

        Returns
        -------
        p : float
            proportional coeff
        i : float
            integral coeff
        d : float
            derivative coeff

        Raises
        ------
        None
        """
        sp = os.path.split(filename)[1].split('-')
        if sp[0].startswith('P') and sp[1].startswith('I') and sp[2].startswith('D'):
            p = sp[0]
            i = sp[1]
            d = sp[2]
            p = round(float(p[1:]), 2)
            i = round(float(i[1:]), 2)
            d = round(float(d[1:]), 2)
        else:
            p, i, d = None, None, None
        return p, i, d

    def getReferenceImage(self, field, filt):
        """
        Look in the database for the current
        field/filter reference image

        Parameters
        ----------
        field : string
            name of the current field
        filt : string
            name of the current filter

        Returns
        -------
        ref_image : string
            path to the reference image
            returns None if no reference image found

        Raises
        ------
        None
        """
        tnow = datetime.utcnow().isoformat().split('.')[0].replace('T', ' ')
        qry = """
            SELECT ref_image
            FROM autoguider_ref
            WHERE field = '%s'
            AND filter = '%s'
            AND valid_from < '%s'
            AND valid_until IS NULL
            """
        qry_args = (field, filt, tnow)

        result = self.cursor.execute(qry%qry_args)
        
        if not result:
            ref_image = None
        else:
            ref_image = "{}/{}".format(self.reference_dir, result[0][0])
        return ref_image
    
    def setReferenceImage(self, field, filt, ref_image, telescope):
        """
        Set a new image as a reference in the database

        Parameters
        ----------
        field : string
            name of the current field
        filt : string
            name of the current filter
        ref_image : string
            name of the image to set as reference
        telescope : string
            name of the telescope

        Returns
        -------

        Raises
        ------
        """
        tnow = datetime.utcnow().isoformat().split('.')[0].replace('T', ' ')
        qry = """
            INSERT INTO autoguider_ref
            (field, telescope, ref_image,
            filter, valid_from)
            VALUES
            ('%s', '%s', '%s', '%s', '%s')
            """
        qry_args = (field, telescope, ref_image, filt, tnow)
        self.cursor.execute(qry%qry_args)

        # copy the file to the autoguider_ref location
        copyfile(ref_image, "{}/{}".format(self.reference_dir, ref_image))

    # todo: save images with telescope+target_name + filter + exp time
    def waitForImage(self, n_images):
        """
        Wait for new images.

        Parameters
        ----------
        n_images : int
            number of images previously acquired


        Returns
        -------
        newest_image : string
            filenname of the newest image
        newest_field : string
            name of the newest field
        newest_filter : string
            name of the newest filter

        Raises
        ------
        None
        """
        while self.running:

            # check for new images
            t = g.glob(self.glob_str)

            if len(t) > n_images:

                # get newest image
                try:
                    newest_image = max(t, key=os.path.getctime)
                except ValueError:
                    # if the intial list is empty, just cycle back and try again
                    continue

                # open the newest image and check the field and filter
                try:
                    with fits.open(newest_image) as fitsfile:
                        newest_filter = fitsfile[0].header[FILTER_KEYWORD]
                        newest_field = fitsfile[0].header[FIELD_KEYWORD]
                except FileNotFoundError:
                    # if the file cannot be accessed (not completely written to disc yet)
                    # cycle back and try again
                    self.logMessageToDb('Problem accessing fits file {}, skipping...'.format(newest_image))
                    continue
                except OSError:
                    # this catches the missing header END card
                    self.logMessageToDb('Problem accessing fits file {}, skipping...'.format(newest_image))
                    continue

                return newest_image, newest_field, newest_filter

                
            # if no new images, wait for a bit
            else:
                time.sleep(0.1)

    def guider_loop(self):

        self.running = True

        # dictionaries to hold reference images for different fields/filters
        ref_track = defaultdict(dict)

        while self.running:
            # initialise the PID controllers for X and Y
            self.PIDx = PID(PID_COEFFS['x']['p'], PID_COEFFS['x']['i'], PID_COEFFS['x']['d'])
            self.PIDy = PID(PID_COEFFS['y']['p'], PID_COEFFS['y']['i'], PID_COEFFS['y']['d'])
            self.PIDx.setPoint(PID_COEFFS['set_x'])
            self.PIDy.setPoint(PID_COEFFS['set_y'])

            # ag correction buffers - used for outlier rejection
            self.BUFF_X, self.BUFF_Y = [], []


            # get a list of the images in the directory
            templist = g.glob(self.glob_str)

            # TODO: change location of logfile
            self.logShiftsToFile(LOGFILE, [], header=True)

            # check for any data in there
            n_images = len(templist)
            print("testing: n_images ", n_images) # todo: remove

            if n_images == 0:
                last_file, _, _ = self.waitForImage(n_images)
            else:
                last_file = max(templist, key=os.path.getctime)

            # check we can access the last file
            try:
                with fits.open(last_file) as ff:
                    # current field and filter?
                    current_filter = ff[0].header[FILTER_KEYWORD]
                    current_field = ff[0].header[FIELD_KEYWORD]
                    # Look for a reference image for this field/filter
                    ref_file = self.getReferenceImage(current_field, current_filter)
                    # if there is no reference image, set this one as it and continue
                    # set the previous reference image
                    if not ref_file:
                        self.setReferenceImage(current_field, current_filter, last_file, self.camera.device_name)
                        ref_file = "{}/{}".format(self.reference_dir, last_file)
            except IOError:
                self.logMessageToDb("Problem opening {}...".format(last_file))
                self.logMessageToDb("Breaking back to check for new day...")
                continue

            # finally, load up the reference file for this field/filter
            self.logMessageToDb("Ref_File: {}".format(ref_file))
            ref_track[current_field][current_filter] = ref_file

            # set up the reference image with donuts
            donuts_ref = Donuts(ref_file, normalise=False, subtract_bkg=True, downweight_edges=False, image_class=CustomImageClass)

            # number of images allowed during initial pull in
            # -ve numbers mean ag should have stabilised
            images_to_stabilise = IMAGES_TO_STABILISE
            stabilised = 'n'

            # Now wait on new images
            while self.running:
                check_file, current_field, current_filter = self.waitForImage(n_images)

                self.logMessageToDb(
                            "REF: {} CHECK: {} [{}]".format(ref_track[current_field][current_filter],
                                                            check_file, current_filter))
                images_to_stabilise -= 1
                # if we are done stabilising, reset the PID loop
                if images_to_stabilise == 0:
                    self.logMessageToDb('Stabilisation complete, reseting PID loop...')
                    self.PIDx = PID(PID_COEFFS['x']['p'], PID_COEFFS['x']['i'], PID_COEFFS['x']['d'])
                    self.PIDy = PID(PID_COEFFS['y']['p'], PID_COEFFS['y']['i'], PID_COEFFS['y']['d'])
                    self.PIDx.setPoint(PID_COEFFS['set_x'])
                    self.PIDy.setPoint(PID_COEFFS['set_y'])
                elif images_to_stabilise > 0:
                    self.logMessageToDb('Stabilising using P=1.0, I=0.0, D=0.0')
                    self.PIDx = PID(1.0, 0.0, 0.0)
                    self.PIDy = PID(1.0, 0.0, 0.0)
                    self.PIDx.setPoint(PID_COEFFS['set_x'])
                    self.PIDy.setPoint(PID_COEFFS['set_y'])

                # test load the comparison image to get the shift
                try:
                    h2 = fits.open(check_file)
                    del h2
                except IOError:
                    self.logMessageToDb("Problem opening CHECK: {}...".format(check_file))
                    self.logMessageToDb("Breaking back to look for new file...")
                    continue

                # reset culled tags
                culled_max_shift_x = 'n'
                culled_max_shift_y = 'n'
                # work out shift here
                shift = donuts_ref.measure_shift(check_file)
                shift_x = shift.x.value
                shift_y = shift.y.value
                self.logMessageToDb("x shift: {:.2f}".format(float(shift_x)))
                self.logMessageToDb("y shift: {:.2f}".format(float(shift_y)))
                # revoke stabilisation early if shift less than 2 pixels
                if abs(shift_x) <= 2.0 and abs(shift_y) < 2.0 and images_to_stabilise > 0:
                    images_to_stabilise = 1

                # Check if shift greater than max allowed error in post pull in state
                if images_to_stabilise < 0:
                    stabilised = 'y'
                    if abs(shift_x) > MAX_ERROR_PIXELS:
                        self.logMessageToDb("X shift > {}, applying no correction".format(MAX_ERROR_PIXELS))
                        culled_max_shift_x = 'y'
                    else:
                        pre_pid_x = shift_x
                    if abs(shift_y) > MAX_ERROR_PIXELS:
                        self.logMessageToDb("Y shift > {}, applying no correction".format(MAX_ERROR_PIXELS))
                        culled_max_shift_y = 'y'
                    else:
                        pre_pid_y = shift_y
                else:
                    self.logMessageToDb('Allowing field to stabilise, imposing new max error clip')

                    stabilised = 'n'
                    if shift_x > MAX_ERROR_STABIL_PIXELS:
                        pre_pid_x = MAX_ERROR_STABIL_PIXELS
                    elif shift_x < -MAX_ERROR_STABIL_PIXELS:
                        pre_pid_x = -MAX_ERROR_STABIL_PIXELS
                    else:
                        pre_pid_x = shift_x

                    if shift_y > MAX_ERROR_STABIL_PIXELS:
                        pre_pid_y = MAX_ERROR_STABIL_PIXELS
                    elif shift_y < -MAX_ERROR_STABIL_PIXELS:
                        pre_pid_y = -MAX_ERROR_STABIL_PIXELS
                    else:
                        pre_pid_y = shift_y
                # if either axis is off by > MAX error then stop everything, no point guiding
                # in 1 axis, need to figure out the source of the problem and run again
                if culled_max_shift_x == 'y' or culled_max_shift_y == 'y':
                    pre_pid_x, pre_pid_y, post_pid_x, post_pid_y, \
                        std_buff_x, std_buff_y = 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
                else:
                    applied, post_pid_x, post_pid_y, \
                        std_buff_x, std_buff_y = self.guide(pre_pid_x, pre_pid_y,
                                                    images_to_stabilise, self.PIDx, self.PIDy)
                    # !applied means no telescope, break to tomorrow
                    if not applied:
                        self.logMessageToDb('SHIFT NOT APPLIED, TELESCOPE *NOT* CONNECTED, EXITING')
                        self.running = False

                log_list = [self.glob_str.split('/')[-2],
                            os.path.split(ref_file)[1],
                            check_file,
                            stabilised,
                            str(round(shift_x, 3)),
                            str(round(shift_y, 3)),
                            str(round(pre_pid_x, 3)),
                            str(round(pre_pid_y, 3)),
                            str(round(post_pid_x, 3)),
                            str(round(post_pid_y, 3)),
                            str(round(std_buff_x, 3)),
                            str(round(std_buff_y, 3)),
                            culled_max_shift_x,
                            culled_max_shift_y]

                # log info to file
                self.logShiftsToFile(LOGFILE, log_list)
                # log info to database - enable when DB is running
                self.logShiftsToDb(tuple(log_list))
                # reset the comparison templist so the nested while(1) loop
                # can find new images
                templist = g.glob(self.glob_str)
                n_images = len(templist)
