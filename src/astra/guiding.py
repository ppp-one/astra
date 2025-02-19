import glob as g
import logging
import os
import time
from datetime import datetime
from math import cos, radians
from shutil import copyfile

import numpy as np
from alpaca.telescope import GuideDirections
from astropy.io import fits
from astropy.stats import SigmaClip
from donuts import Donuts
from donuts.image import Image
from photutils.background import Background2D, MedianBackground
from scipy import ndimage

from astra import Config

CONFIG = Config()

"""
Configuration parameters
"""

# header keyword for the current filter
FILTER_KEYWORD = "FILTER"

# header keyword for the current target/field
FIELD_KEYWORD = "OBJECT"

# header keyword for the current exposure time
EXPTIME_KEYWORD = "EXPTIME"

# rejection buffer length
GUIDE_BUFFER_LENGTH = 20

# number images allowed during pull in period
IMAGES_TO_STABILISE = 10

# outlier rejection sigma
SIGMA_BUFFER = 10

# max allowed shift to correct
MAX_ERROR_PIXELS = 20

# max alloed shift to correct during stabilisation
MAX_ERROR_STABIL_PIXELS = 40

# TODO: add logger handler here?


class CustomImageClass(Image):
    def preconstruct_hook(self):
        sigma_clip = SigmaClip(sigma=3.0)
        bkg_estimator = MedianBackground()

        self.raw_image = self.raw_image.astype(np.int16)

        bkg = Background2D(
            self.raw_image,
            (32, 32),
            filter_size=(3, 3),
            sigma_clip=sigma_clip,
            bkg_estimator=bkg_estimator,
        )
        bkg_clean = self.raw_image - bkg.background

        med_clean = ndimage.median_filter(bkg_clean, size=5, mode="mirror")
        band_corr = np.median(med_clean, axis=1).reshape(-1, 1)
        image_clean = med_clean - band_corr

        self.raw_image = image_clean


class Guider:
    def __init__(
        self, telescope: object, cursor: object, logger: logging.Logger, params: dict
    ):
        # TODO: camera angle?

        # pass in objects from astra
        self.telescope = telescope
        self.cursor = cursor
        self.logger = logger

        # set up the database
        self.create_tables()  # this is assuming we're using the same db.  Should we have a separate one for guiding?

        # set up the image glob string
        # create reference directory if not exists
        self.reference_dir = CONFIG.paths.images / "autoguider_ref"
        self.reference_dir.mkdir(parents=True, exist_ok=True)

        # pulseGuide conversions
        self.PIX2TIME = params["PIX2TIME"]

        # guide directions
        self.DIRECTIONS = {}
        for direction in params["DIRECTIONS"]:
            if params["DIRECTIONS"][direction] == "North":
                self.DIRECTIONS[direction] = GuideDirections.guideNorth
            elif params["DIRECTIONS"][direction] == "South":
                self.DIRECTIONS[direction] = GuideDirections.guideSouth
            elif params["DIRECTIONS"][direction] == "East":
                self.DIRECTIONS[direction] = GuideDirections.guideEast
            elif params["DIRECTIONS"][direction] == "West":
                self.DIRECTIONS[direction] = GuideDirections.guideWest
            else:
                self.logger.error(
                    f"Invalid guide direction {self.DIRECTIONS[direction]}"
                )

        # RA axis alignment along x or y? TODO: can be inferred from telescope direction
        self.RA_AXIS = params["RA_AXIS"]

        # PID loop coefficients
        self.PID_COEFFS = params["PID_COEFFS"]

        # set up variables
        # initialise the PID controllers for X and Y
        self.PIDx = PID(
            self.PID_COEFFS["x"]["p"],
            self.PID_COEFFS["x"]["i"],
            self.PID_COEFFS["x"]["d"],
        )
        self.PIDy = PID(
            self.PID_COEFFS["y"]["p"],
            self.PID_COEFFS["y"]["i"],
            self.PID_COEFFS["y"]["d"],
        )
        self.PIDx.setPoint(self.PID_COEFFS["set_x"])
        self.PIDy.setPoint(self.PID_COEFFS["set_y"])

        # ag correction buffers - used for outlier rejection
        self.BUFF_X, self.BUFF_Y = [], []

        self.running = False

    def create_tables(self):
        """
        Create a database for donuts
        """

        db_command_0 = """CREATE TABLE IF NOT EXISTS autoguider_ref (
                ref_id mediumint auto_increment primary key,
                field varchar(100) not null,
                telescope varchar(20) not null,
                ref_image varchar(100) not null,
                filter varchar(20) not null,
                exptime varchar(20) not null,
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

        self.cursor.execute(qry % qry_args)

    def logMessageToDb(self, camera_name, message):
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
        qry_args = (camera_name, message)
        self.cursor.execute(qry % qry_args)

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
            line = (
                "night  ref  check  stable  shift_x  shift_y  pre_pid_x  pre_pid_y  "
                "post_pid_x  post_pid_y  std_buff_x  std_buff_y  culled_x  culled_y"
            )
        else:
            line = "  ".join(loglist)
        with open(logfile, "a") as outfile:
            outfile.write("{}\n".format(line))

    def guide(self, x, y, images_to_stabilise, camera_name, gem=False):
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

        connected = self.telescope.get("Connected")
        if connected:
            # get telescope declination to scale RA corrections
            dec = self.telescope.get("Declination")
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
                if (
                    len(self.BUFF_X) < GUIDE_BUFFER_LENGTH
                    and len(self.BUFF_Y) < GUIDE_BUFFER_LENGTH
                ):
                    self.logMessageToDb(camera_name, "Filling AG stats buffer...")
                    sigma_x = 0.0
                    sigma_y = 0.0
                else:
                    sigma_x = np.std(self.BUFF_X)
                    sigma_y = np.std(self.BUFF_Y)
                    if (
                        abs(x) > SIGMA_BUFFER * sigma_x
                        or abs(y) > SIGMA_BUFFER * sigma_y
                    ):
                        self.logMessageToDb(
                            camera_name,
                            "Guide error > {} sigma * buffer errors, ignoring...".format(
                                SIGMA_BUFFER
                            ),
                        )
                        # store the original values in the buffer, even if correction
                        # was too big, this will allow small outliers to be caught
                        self.BUFF_X.append(x)
                        self.BUFF_Y.append(y)
                        return True, 0.0, 0.0, sigma_x, sigma_y
                    else:
                        pass
            else:
                self.logMessageToDb(
                    camera_name, "Ignoring AG buffer during stabilisation"
                )
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
            self.logMessageToDb(
                camera_name, "PID: {0:.2f}  {1:.2f}".format(float(pidx), float(pidy))
            )

            # make another check that the post PID values are not > Max allowed
            # using >= allows for the stabilising runs to get through
            # abs() on -ve duration otherwise throws back an error
            if pidy > 0 and pidy <= CURRENT_MAX_SHIFT:
                guide_time_y = pidy * self.PIX2TIME["+y"]
                if self.RA_AXIS == "y":
                    guide_time_y = guide_time_y / cos_dec
                self.telescope.get("PulseGuide")(
                    Direction=self.DIRECTIONS["+y"], Duration=int(guide_time_y)
                )
            if pidy < 0 and pidy >= -CURRENT_MAX_SHIFT:
                guide_time_y = abs(pidy * self.PIX2TIME["-y"])
                if self.RA_AXIS == "y":
                    guide_time_y = guide_time_y / cos_dec
                self.telescope.get("PulseGuide")(
                    Direction=self.DIRECTIONS["-y"], Duration=int(guide_time_y)
                )

            # TODO: add timeout
            while self.telescope.get("IsPulseGuiding"):
                time.sleep(0.01)

            if pidx > 0 and pidx <= CURRENT_MAX_SHIFT:
                guide_time_x = pidx * self.PIX2TIME["+x"]
                if self.RA_AXIS == "x":
                    guide_time_x = guide_time_x / cos_dec
                self.telescope.get("PulseGuide")(
                    Direction=self.DIRECTIONS["+x"], Duration=int(guide_time_x)
                )

            if pidx < 0 and pidx >= -CURRENT_MAX_SHIFT:
                guide_time_x = abs(pidx * self.PIX2TIME["-x"])
                if self.RA_AXIS == "x":
                    guide_time_x = guide_time_x / cos_dec
                self.telescope.get("PulseGuide")(
                    Direction=self.DIRECTIONS["-x"], Duration=int(guide_time_x)
                )

            # TODO: add timeout
            while self.telescope.get("IsPulseGuiding"):
                time.sleep(0.01)

            self.logMessageToDb(camera_name, "Guide correction Applied")
            # store the original values in the buffer
            # only if we are not stabilising
            if images_to_stabilise < 0:
                self.BUFF_X.append(x)
                self.BUFF_Y.append(y)
            return True, pidx, pidy, sigma_x, sigma_y
        else:
            self.logMessageToDb(camera_name, "Telescope NOT connected!")
            self.logMessageToDb(camera_name, "Ignoring corrections!")
            return False, 0.0, 0.0, 0.0, 0.0

    def getReferenceImage(self, field, filt, exptime):
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
        tnow = datetime.utcnow().isoformat().split(".")[0].replace("T", " ")
        qry = """
            SELECT ref_image
            FROM autoguider_ref
            WHERE field = '%s'
            AND filter = '%s'
            AND exptime = '%s'
            AND valid_from < '%s'
            AND valid_until IS NULL
            """
        qry_args = (field, filt, exptime, tnow)

        result = self.cursor.execute(qry % qry_args)

        if not result:
            ref_image = None
        else:
            ref_image = os.path.join(self.reference_dir, result[0][0])
        return ref_image

    def setReferenceImage(self, field, filt, exptime, ref_image, telescope):
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
        tnow = datetime.utcnow().isoformat().split(".")[0].replace("T", " ")
        qry = """
            INSERT INTO autoguider_ref
            (field, telescope, ref_image,
            filter, exptime, valid_from)
            VALUES
            ('%s', '%s', '%s', '%s', '%s', '%s')
            """
        qry_args = (field, telescope, os.path.split(ref_image)[-1], filt, exptime, tnow)
        self.cursor.execute(qry % qry_args)

        # copy the file to the autoguider_ref location
        print(ref_image, os.path.join(self.reference_dir, os.path.split(ref_image)[-1]))
        copyfile(
            ref_image, os.path.join(self.reference_dir, os.path.split(ref_image)[-1])
        )

    def waitForImage(self, n_images, camera_name, glob_str, wait_time=10):
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
        if self.running is True:
            while self.running:
                # check for new images
                t = g.glob(glob_str)

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
                            newest_filter = (
                                fitsfile[0].header[FILTER_KEYWORD].replace("'", "")
                            )
                            newest_field = fitsfile[0].header[FIELD_KEYWORD]
                            newest_exptime = fitsfile[0].header[EXPTIME_KEYWORD]
                    except FileNotFoundError:
                        # if the file cannot be accessed (not completely written to disc yet)
                        # cycle back and try again
                        self.logMessageToDb(
                            camera_name,
                            "Problem accessing fits file {}, skipping...".format(
                                newest_image
                            ),
                        )
                        continue
                    except OSError:
                        # this catches the missing header END card
                        self.logMessageToDb(
                            camera_name,
                            "Problem accessing fits file {}, skipping...".format(
                                newest_image
                            ),
                        )
                        continue

                    return newest_image, newest_field, newest_filter, newest_exptime

                # if no new images, wait for a bit
                else:
                    time.sleep(max(wait_time, 30))

        # return None values if self.running is False
        return None, None, None, None

    def guider_loop(self, camera_name, glob_str, wait_time=10):
        self.running = True

        self.logger.info(f"Starting guider loop for: {glob_str} images")

        try:
            while self.running:
                # get a list of the images in the directory
                templist = g.glob(glob_str)

                # take directory of glob_str and add logfile name
                LOGFILE = os.path.join(os.path.dirname(glob_str), "guider.log")

                # TODO: change location of logfile and detect if it already exists
                self.logShiftsToFile(LOGFILE, [], header=True)

                # check for any data in there
                n_images = len(templist)

                if n_images == 0:
                    last_file, _, _, _ = self.waitForImage(
                        n_images, camera_name, glob_str, wait_time
                    )
                else:
                    last_file = max(templist, key=os.path.getctime)

                # check we can access the last file
                try:
                    with fits.open(last_file) as ff:
                        # current field and filter?
                        current_filter = ff[0].header[FILTER_KEYWORD].replace("'", "")
                        current_field = ff[0].header[FIELD_KEYWORD]
                        current_exptime = ff[0].header[EXPTIME_KEYWORD]
                        # Look for a reference image for this field/filter
                        ref_file = self.getReferenceImage(
                            current_field, current_filter, current_exptime
                        )
                        # if there is no reference image, set this one as it and continue
                        # set the previous reference image
                        if not ref_file:
                            self.setReferenceImage(
                                current_field,
                                current_filter,
                                current_exptime,
                                last_file,
                                camera_name,
                            )
                            ref_file = os.path.join(
                                self.reference_dir, os.path.basename(last_file)
                            )
                except IOError:
                    self.logMessageToDb(
                        camera_name, "Problem opening {}...".format(last_file)
                    )
                    continue

                # finally, load up the reference file for this field/filter
                self.logMessageToDb(camera_name, "Ref_File: {}".format(ref_file))

                # set up the reference image with donuts
                donuts_ref = Donuts(
                    ref_file,
                    normalise=False,
                    subtract_bkg=True,
                    downweight_edges=False,
                    image_class=CustomImageClass,
                )

                # number of images allowed during initial pull in
                # -ve numbers mean ag should have stabilised
                images_to_stabilise = IMAGES_TO_STABILISE
                stabilised = "n"

                # Now wait on new images
                while self.running:
                    (
                        check_file,
                        current_field,
                        current_filter,
                        current_exptime,
                    ) = self.waitForImage(n_images, camera_name, glob_str)

                    # to insure file is fully written to disc
                    time.sleep(1)

                    if self.running is True:
                        self.logMessageToDb(
                            camera_name,
                            "REF: {} CHECK: {} [{}]".format(
                                ref_file, check_file, current_filter
                            ),
                        )
                        images_to_stabilise -= 1
                        # if we are done stabilising, reset the PID loop
                        if images_to_stabilise == 0:
                            self.logMessageToDb(
                                camera_name,
                                "Stabilisation complete, reseting PID loop...",
                            )
                            self.PIDx = PID(
                                self.PID_COEFFS["x"]["p"],
                                self.PID_COEFFS["x"]["i"],
                                self.PID_COEFFS["x"]["d"],
                            )
                            self.PIDy = PID(
                                self.PID_COEFFS["y"]["p"],
                                self.PID_COEFFS["y"]["i"],
                                self.PID_COEFFS["y"]["d"],
                            )
                            self.PIDx.setPoint(self.PID_COEFFS["set_x"])
                            self.PIDy.setPoint(self.PID_COEFFS["set_y"])
                        elif images_to_stabilise > 0:
                            self.logMessageToDb(
                                camera_name, "Stabilising using P=1.0, I=0.0, D=0.0"
                            )
                            self.PIDx = PID(1.0, 0.0, 0.0)
                            self.PIDy = PID(1.0, 0.0, 0.0)
                            self.PIDx.setPoint(self.PID_COEFFS["set_x"])
                            self.PIDy.setPoint(self.PID_COEFFS["set_y"])

                        # test load the comparison image to get the shift
                        try:
                            h2 = fits.open(check_file)
                            del h2
                        except IOError:
                            self.logMessageToDb(
                                camera_name,
                                "Problem opening CHECK: {}...".format(check_file),
                            )
                            self.logMessageToDb(
                                camera_name, "Breaking back to look for new file..."
                            )
                            continue

                        # reset culled tags
                        culled_max_shift_x = "n"
                        culled_max_shift_y = "n"
                        # work out shift here
                        shift = donuts_ref.measure_shift(check_file)
                        shift_x = shift.x.value
                        shift_y = shift.y.value
                        self.logMessageToDb(
                            camera_name, "x shift: {:.2f}".format(float(shift_x))
                        )
                        self.logMessageToDb(
                            camera_name, "y shift: {:.2f}".format(float(shift_y))
                        )
                        # revoke stabilisation early if shift less than 2 pixels
                        if (
                            abs(shift_x) <= 2.0
                            and abs(shift_y) < 2.0
                            and images_to_stabilise > 0
                        ):
                            images_to_stabilise = 1

                        # Check if shift greater than max allowed error in post pull in state
                        if images_to_stabilise < 0:
                            stabilised = "y"
                            if abs(shift_x) > MAX_ERROR_PIXELS:
                                self.logMessageToDb(
                                    camera_name,
                                    "X shift > {}, applying no correction".format(
                                        MAX_ERROR_PIXELS
                                    ),
                                )
                                culled_max_shift_x = "y"
                            else:
                                pre_pid_x = shift_x
                            if abs(shift_y) > MAX_ERROR_PIXELS:
                                self.logMessageToDb(
                                    camera_name,
                                    "Y shift > {}, applying no correction".format(
                                        MAX_ERROR_PIXELS
                                    ),
                                )
                                culled_max_shift_y = "y"
                            else:
                                pre_pid_y = shift_y
                        else:
                            self.logMessageToDb(
                                camera_name,
                                "Allowing field to stabilise, imposing new max error clip",
                            )

                            stabilised = "n"
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
                        if culled_max_shift_x == "y" or culled_max_shift_y == "y":
                            (
                                pre_pid_x,
                                pre_pid_y,
                                post_pid_x,
                                post_pid_y,
                                std_buff_x,
                                std_buff_y,
                            ) = (0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
                        else:
                            (
                                applied,
                                post_pid_x,
                                post_pid_y,
                                std_buff_x,
                                std_buff_y,
                            ) = self.guide(
                                pre_pid_x, pre_pid_y, images_to_stabilise, camera_name
                            )
                            # !applied means no telescope, break to tomorrow
                            if not applied:
                                self.logMessageToDb(
                                    camera_name,
                                    "SHIFT NOT APPLIED, TELESCOPE *NOT* CONNECTED, EXITING",
                                )
                                self.running = False

                        log_list = [
                            os.path.split(glob_str)[-2],
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
                            culled_max_shift_y,
                        ]

                        # log info to file
                        self.logShiftsToFile(LOGFILE, log_list)
                        # log info to database - enable when DB is running
                        self.logShiftsToDb(tuple(log_list))
                        # reset the comparison templist so the nested while(1) loop
                        # can find new images
                        templist = g.glob(glob_str)
                        n_images = len(templist)
        except Exception as e:
            self.running = False
            self.logger.error(f"Error in guide loop: {str(e)}")

        self.logger.info(f"Stopping guider loop for: {glob_str} images")


"""
PID loop controller
"""

# : disable=invalid-name
# pylint: disable=too-many-arguments
# pylint: disable=too-many-instance-attributes


class PID:
    """
    Discrete PID control

    http://code.activestate.com/recipes/577231-discrete-pid-controller/
    """

    def __init__(
        self,
        P=0.5,
        I=0.25,
        D=0.0,
        Derivator=0,
        Integrator=0,
        Integrator_max=500,
        Integrator_min=-500,
    ):
        self.Kp = P
        self.Ki = I
        self.Kd = D
        self.Derivator = Derivator
        self.Integrator = Integrator
        self.Integrator_max = Integrator_max
        self.Integrator_min = Integrator_min
        self.set_point = 0.0
        self.error = 0.0
        self.P_value = 0.0  # included as pylint complained - jmcc
        self.D_value = 0.0  # included as pylint complained - jmcc
        self.I_value = 0.0  # included as pylint complained - jmcc

    def update(self, current_value):
        """
        Calculate PID output value for given reference input and feedback
        """
        self.error = self.set_point - current_value
        self.P_value = self.Kp * self.error
        self.D_value = self.Kd * (self.error - self.Derivator)
        self.Derivator = self.error
        self.Integrator = self.Integrator + self.error
        if self.Integrator > self.Integrator_max:
            self.Integrator = self.Integrator_max
        elif self.Integrator < self.Integrator_min:
            self.Integrator = self.Integrator_min
        self.I_value = self.Integrator * self.Ki
        pid = self.P_value + self.I_value + self.D_value
        return pid

    def setPoint(self, set_point):
        """
        Initilize the setpoint of PID
        """
        self.set_point = set_point
        self.Integrator = 0
        self.Derivator = 0

    def setIntegrator(self, Integrator):
        """
        Set Integrator
        """
        self.Integrator = Integrator

    def setDerivator(self, Derivator):
        """
        Set Derivator
        """
        self.Derivator = Derivator

    def setKp(self, P):
        """
        Set Kp
        """
        self.Kp = P

    def setKi(self, I):
        """
        Set Ki
        """
        self.Ki = I

    def setKd(self, D):
        """
        Set Kd
        """
        self.Kd = D

    def getPoint(self):
        """
        Get point
        """
        return self.set_point

    def getError(self):
        """
        Get Error
        """
        return self.error

    def getIntegrator(self):
        """
        Get Integrator
        """
        return self.Integrator

    def getDerivator(self):
        """
        Get Derivator
        """
        return self.Derivator
