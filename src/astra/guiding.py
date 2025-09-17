"""
Astronomical telescope autoguiding system with PID control.

This module provides automated guiding functionality for astronomical telescopes
using image-based tracking with PID control loops. It implements the complete
guiding workflow from image acquisition to telescope correction commands.

Key Features:
- Real-time star tracking using the Donuts image registration library
- PID control loops for precise telescope corrections
- Database logging of guiding performance and corrections
- Support for German Equatorial Mount (GEM) pier side changes
- Outlier rejection and statistical analysis of guiding errors
- Automatic reference image management per field/filter combination
- Background subtraction and image cleaning for robust star detection

The system continuously monitors incoming images, compares them to reference
images, calculates pointing errors, and applies corrective pulse guide commands
to keep the telescope accurately tracking celestial objects.

Typical Usage:
    # Initialize guider with telescope and configuration
    guider = Guider(telescope, astra_instance, guider_params)

    # Start the main guiding loop
    guider.guider_loop(
        camera_name="main_camera",
        glob_str="/path/to/images/*.fits",
        wait_time=10,
        binning=1
    )

Components:
    CustomImageClass: Image preprocessing for robust star detection
    Guider: Main autoguiding class with PID control
    PID: Discrete PID controller implementation
"""

import glob as g
import os
import time
from datetime import UTC, datetime
from math import cos, radians
from shutil import copyfile
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from alpaca.telescope import AlignmentModes, GuideDirections, PierSide
from astropy.io import fits
from astropy.stats import SigmaClip
from donuts import Donuts
from donuts.image import Image
from photutils.background import Background2D, MedianBackground
from scipy import ndimage

import astra
from astra import Config

# header keyword for the current filter
FILTER_KEYWORD = "FILTER"

# header keyword for the current target/field
FIELD_KEYWORD = "OBJECT"

# header keyword for the current exposure time
EXPTIME_KEYWORD = "EXPTIME"

# header keyword for the current PIERSIDE
PIERSIDE_KEYWORD = "PIERSIDE"

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

# IsPulseGuiding timeout
IS_PULSE_GUIDING_TIMEOUT = 120  # seconds


class CustomImageClass(Image):
    """
    Custom image preprocessing class for robust autoguiding star detection.

    Extends the Donuts Image class to apply background subtraction, median filtering,
    and horizontal banding correction before star detection. This preprocessing
    improves the reliability of star tracking in noisy or non-uniform images.

    The preprocessing pipeline:
    1. Background subtraction using 2D background estimation
    2. Median filtering to reduce noise
    3. Horizontal band correction to remove systematic gradients
    4. Clipping to ensure positive pixel values
    """

    def preconstruct_hook(self) -> None:
        """
        Apply image preprocessing before Donuts star detection.

        Performs background subtraction, noise reduction, and systematic
        correction to improve star detection reliability.
        """
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

        self.raw_image = np.clip(image_clean, 1, None)


class Guider:
    """
    Automated telescope guiding system with PID control and statistical analysis.

    Implements a complete autoguiding solution that continuously monitors telescope
    pointing accuracy and applies corrective pulse guide commands. Features include
    PID control loops, outlier rejection, database logging, and support for German
    Equatorial Mounts with pier side changes.

    The guider maintains statistical buffers for error analysis, handles field
    stabilization periods, and manages reference images per field/filter combination.

    Attributes:
        telescope: Alpaca telescope device for pulse guiding commands
        observatory: Astra observatory instance for logging and database access
        PIX2TIME: Pixel-to-millisecond conversion factors for guide pulses
        DIRECTIONS: Mapping of guide directions to Alpaca constants
        RA_AXIS: Which axis (x/y) corresponds to Right Ascension
        PID_COEFFS: PID controller coefficients for x and y axes
        running: Flag to control guiding loop execution

    Example:
        >>> guider = Guider(telescope, astra_instance, {
        ...     "PIX2TIME": {"+x": 100, "-x": 100, "+y": 100, "-y": 100},
        ...     "DIRECTIONS": {"+x": "East", "-x": "West", "+y": "North", "-y": "South"},
        ...     "RA_AXIS": "x",
        ...     "PID_COEFFS": {"x": {"p": 0.8, "i": 0.1, "d": 0.1}, ...}
        ... })
        >>> guider.guider_loop("camera1", "/data/*.fits")
    """

    def __init__(
        self,
        telescope: Any,
        observatory: "astra.observatory.Observatory",
        params: Dict[str, Any],
    ) -> None:
        """
        Initialize the autoguider with telescope, logging, and PID parameters.

        Parameters:
            telescope: Alpaca telescope device for sending pulse guide commands.
            observatory: Astra observatory instance for logging and database access.
            params (dict): Configuration dictionary containing:
                - PIX2TIME: Pixel to millisecond conversion factors
                - DIRECTIONS: Guide direction mappings
                - RA_AXIS: Which axis corresponds to RA ("x" or "y")
                - PID_COEFFS: PID controller coefficients for both axes
        """
        # TODO: camera angle?

        self.telescope = telescope
        self.observatory = observatory

        # set up the database
        self.create_tables()  # this is assuming we're using the same db.  Should we have a separate one for guiding?

        # set up the image glob string
        # create reference directory if not exists
        self.reference_dir = Config().paths.images / "autoguider_ref"
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
                self.observatory.report_device_issue(
                    device_type="Guider",
                    device_name=self.telescope.device_name,
                    message=f"Invalid guide direction {params['DIRECTIONS'][direction]} for {self.telescope.device_name} config",
                )

        # RA axis alignment along x or y? TODO: can be inferred from telescope direction
        self.RA_AXIS = params["RA_AXIS"]

        # PID loop coefficients
        self.PID_COEFFS = params["PID_COEFFS"]

        # set up variables
        # initialise the PID controllers for X and Y
        self.PIDx: PID = PID.from_config_dict(self.PID_COEFFS["x"])
        self.PIDy: PID = PID.from_config_dict(self.PID_COEFFS["y"])
        self.PIDx.initialize_set_point(self.PID_COEFFS["set_x"])
        self.PIDy.initialize_set_point(self.PID_COEFFS["set_y"])

        # ag correction buffers - used for outlier rejection
        self.BUFF_X: List[float] = []
        self.BUFF_Y: List[float] = []

        self.running: bool = False

    def create_tables(self) -> None:
        """
        Create database tables for autoguider reference images and logging.

        Creates three tables:
        - autoguider_ref: Reference image metadata and validity periods
        - autoguider_log: Detailed guiding corrections and statistics
        - autoguider_info_log: General status and info messages
        """

        db_command_0 = """CREATE TABLE IF NOT EXISTS autoguider_ref (
                ref_id mediumint auto_increment primary key,
                field varchar(100) not null,
                camera varchar(20) not null,
                ref_image varchar(100) not null,
                filter varchar(20) not null,
                exptime varchar(20) not null,
                pierside int not null,
                valid_from datetime not null,
                valid_until datetime
                );"""

        self.observatory.cursor.execute(db_command_0)

        db_command_1 = """CREATE TABLE IF NOT EXISTS autoguider_log (
                datetime timestamp default current_timestamp,
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

        self.observatory.cursor.execute(db_command_1)

        db_command_2 = """CREATE TABLE IF NOT EXISTS autoguider_info_log (
                message_id INTEGER PRIMARY KEY AUTOINCREMENT,
                datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                camera varchar(20) NOT NULL,
                message varchar(500) NOT NULL
                );
                """

        self.observatory.cursor.execute(db_command_2)

    def logShiftsToDb(self, qry_args: Tuple[str, ...]) -> None:
        """
        Log autoguiding corrections and statistics to the database.

        Parameters:
            qry_args (tuple): Tuple containing guiding data in order:
                night, reference, comparison, stabilised, shift_x, shift_y,
                pre_pid_x, pre_pid_y, post_pid_x, post_pid_y, std_buff_x,
                std_buff_y, culled_max_shift_x, culled_max_shift_y
        """
        qry = """
            INSERT INTO autoguider_log
            (night, reference, comparison, stabilised, shift_x, shift_y,
            pre_pid_x, pre_pid_y, post_pid_x, post_pid_y, std_buff_x,
            std_buff_y, culled_max_shift_x, culled_max_shift_y)
            VALUES
            ('%s', '%s', '%s', '%s', '%s', '%s', '%s',
            '%s', '%s', '%s', '%s', '%s', '%s', '%s')
            """

        self.observatory.cursor.execute(qry % qry_args)

    def logMessageToDb(self, camera_name: str, message: str) -> None:
        """
        Log status messages to the database.

        Parameters:
            camera_name (str): Name of the camera being autoguided.
            message (str): Status or info message to log.
        """
        qry = """
            INSERT INTO autoguider_info_log
            (camera, message)
            VALUES
            ('%s', '%s')
            """
        qry_args = (camera_name, message)
        self.observatory.cursor.execute(qry % qry_args)

    def logShiftsToFile(
        self, logfile: str, loglist: List[str], header: bool = False
    ) -> None:
        """
        Log guiding corrections to a text file alongside image data.

        Parameters:
            logfile (str): Path to the log file.
            loglist (list): List of values to log (see logShiftsToDb for order).
            header (bool, optional): Whether to write column headers. Defaults to False.
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

    def guide(
        self,
        x: float,
        y: float,
        images_to_stabilise: int,
        camera_name: str,
        binning: int = 1,
        gem: bool = False,
    ) -> Tuple[bool, float, float, float, float]:
        """
        Apply telescope guiding corrections using PID control with outlier rejection.

        Processes measured pointing errors through PID controllers, applies outlier
        rejection during stable operation, and sends pulse guide commands to the telescope.
        Handles declination scaling for RA corrections and German Equatorial Mount
        pier side changes.

        Parameters:
            x (float): Guide correction needed in X direction (pixels).
            y (float): Guide correction needed in Y direction (pixels).
            images_to_stabilise (int): Images remaining in stabilization period.
                Negative values indicate stable operation.
            camera_name (str): Name of the camera for logging.
            binning (int, optional): Image binning factor. Defaults to 1.
            gem (bool, optional): Whether telescope is German Equatorial Mount. Defaults to False.

        Returns:
            tuple: (success, pidx, pidy, sigma_x, sigma_y) where:
                - success (bool): Whether correction was applied
                - pidx, pidy (float): Actual corrections sent to mount
                - sigma_x, sigma_y (float): Buffer standard deviations
        """

        if gem:
            current_pierside = self.telescope.get("SideOfPier")

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
                if abs(x) > SIGMA_BUFFER * sigma_x or abs(y) > SIGMA_BUFFER * sigma_y:
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
            self.logMessageToDb(camera_name, "Ignoring AG buffer during stabilisation")
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
        if pidy > 0 and pidy <= CURRENT_MAX_SHIFT and self.running:
            guide_time_y = pidy * self.PIX2TIME["+y"] / binning

            y_p_dir = self.DIRECTIONS["+y"]
            if self.RA_AXIS == "y":
                guide_time_y = guide_time_y / cos_dec

                if gem is False:
                    pass  # keep as is
                elif current_pierside == PierSide.pierEast:
                    pass  # keep as is
                else:
                    if self.DIRECTIONS["+y"] == GuideDirections.guideWest:
                        y_p_dir = GuideDirections.guideEast
                    else:
                        y_p_dir = GuideDirections.guideWest

            self.telescope.get("PulseGuide")(
                Direction=y_p_dir, Duration=int(guide_time_y)
            )

        if pidy < 0 and pidy >= -CURRENT_MAX_SHIFT and self.running:
            guide_time_y = abs(pidy * self.PIX2TIME["-y"] / binning)

            y_n_dir = self.DIRECTIONS["-y"]
            if self.RA_AXIS == "y":
                guide_time_y = guide_time_y / cos_dec

                if gem is False:
                    pass  # keep as is
                elif current_pierside == PierSide.pierEast:
                    pass  # keep as is
                else:
                    if self.DIRECTIONS["-y"] == GuideDirections.guideWest:
                        y_n_dir = GuideDirections.guideEast
                    else:
                        y_n_dir = GuideDirections.guideWest

            self.telescope.get("PulseGuide")(
                Direction=y_n_dir, Duration=int(guide_time_y)
            )

        start_time = time.time()
        while self.telescope.get("IsPulseGuiding") and self.running:
            if time.time() - start_time > IS_PULSE_GUIDING_TIMEOUT:
                self.observatory.logger.warning(
                    f"Pulse guiding timed out after {IS_PULSE_GUIDING_TIMEOUT} seconds."
                )
                break
            time.sleep(0.01)

        if pidx > 0 and pidx <= CURRENT_MAX_SHIFT and self.running:
            guide_time_x = pidx * self.PIX2TIME["+x"] / binning

            x_p_dir = self.DIRECTIONS["+x"]
            if self.RA_AXIS == "x":
                guide_time_x = guide_time_x / cos_dec

                if gem is False:
                    pass
                elif current_pierside == PierSide.pierEast:
                    pass  # keep as is
                else:
                    if self.DIRECTIONS["+x"] == GuideDirections.guideWest:
                        x_p_dir = GuideDirections.guideEast
                    else:
                        x_p_dir = GuideDirections.guideWest

            self.telescope.get("PulseGuide")(
                Direction=x_p_dir, Duration=int(guide_time_x)
            )

        if pidx < 0 and pidx >= -CURRENT_MAX_SHIFT and self.running:
            guide_time_x = abs(pidx * self.PIX2TIME["-x"] / binning)

            x_n_dir = self.DIRECTIONS["-x"]
            if self.RA_AXIS == "x":
                guide_time_x = guide_time_x / cos_dec

                if gem is False:
                    pass
                elif current_pierside == PierSide.pierEast:
                    pass  # keep as is
                else:
                    if self.DIRECTIONS["-x"] == GuideDirections.guideWest:
                        x_n_dir = GuideDirections.guideEast
                    else:
                        x_n_dir = GuideDirections.guideWest

            self.telescope.get("PulseGuide")(
                Direction=x_n_dir, Duration=int(guide_time_x)
            )

        start_time = time.time()
        while self.telescope.get("IsPulseGuiding") and self.running:
            if time.time() - start_time > IS_PULSE_GUIDING_TIMEOUT:
                self.observatory.logger.warning(
                    f"Pulse guiding timed out after {IS_PULSE_GUIDING_TIMEOUT} seconds."
                )
                break
            time.sleep(0.01)

        if self.running:
            self.logMessageToDb(camera_name, "Guide correction Applied")
        else:
            self.logMessageToDb(
                camera_name,
                "Guide correction NOT Applied due to self.running=False",
            )

        # store the original values in the buffer
        # only if we are not stabilising
        if images_to_stabilise < 0:
            self.BUFF_X.append(x)
            self.BUFF_Y.append(y)
        return True, pidx, pidy, sigma_x, sigma_y

    def getReferenceImage(
        self, field: str, filt: str, exptime: str, camera: str, pierside: int
    ) -> Optional[str]:
        """
        Retrieve the current reference image path for given observation parameters.

        Parameters:
            field (str): Target field name.
            filt (str): Filter name.
            exptime (str): Exposure time.
            camera (str): Camera name.
            pierside (int): Telescope pier side (1=West, 0=East, -1=Unknown).

        Returns:
            str | None: Path to reference image, or None if not found.
        """
        tnow = datetime.now(UTC).isoformat().split(".")[0].replace("T", " ")
        qry = """
            SELECT ref_image
            FROM autoguider_ref
            WHERE field = '%s'
            AND filter = '%s'
            AND exptime = '%s'
            AND valid_from < '%s'
            AND camera = '%s'
            AND pierside = %d
            AND valid_until IS NULL
            """
        qry_args = (field, filt, exptime, tnow, camera, pierside)

        result = self.observatory.cursor.execute(qry % qry_args)

        if not result:
            ref_image = None
        else:
            ref_image = os.path.join(self.reference_dir, result[0][0])
        return ref_image

    def setReferenceImage(
        self,
        field: str,
        filt: str,
        exptime: str,
        ref_image: str,
        camera: str,
        pierside: int,
    ) -> None:
        """
        Set a new reference image in the database and copy to reference directory.

        Parameters:
            field (str): Target field name.
            filt (str): Filter name.
            exptime (str): Exposure time.
            ref_image (str): Path to image file to use as reference.
            camera (str): Camera name.
            pierside (int): Telescope pier side (1=West, 0=East, -1=Unknown).
        """
        tnow = datetime.now(UTC).isoformat().split(".")[0].replace("T", " ")
        qry = """
            INSERT INTO autoguider_ref
            (field, camera, ref_image,
            filter, exptime, valid_from, pierside)
            VALUES
            ('%s', '%s', '%s', '%s', '%s', '%s', %d)
            """
        qry_args = (
            field,
            camera,
            os.path.split(ref_image)[-1],
            filt,
            exptime,
            tnow,
            pierside,
        )
        self.observatory.cursor.execute(qry % qry_args)

        # copy the file to the autoguider_ref location
        print(ref_image, os.path.join(self.reference_dir, os.path.split(ref_image)[-1]))
        copyfile(
            ref_image, os.path.join(self.reference_dir, os.path.split(ref_image)[-1])
        )

    def waitForImage(
        self, n_images: int, camera_name: str, glob_str: str, wait_time: int = 10
    ) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
        """
        Wait for new images to appear in the monitoring directory.

        Parameters:
            n_images (int): Number of images previously seen.
            camera_name (str): Camera name for logging.
            glob_str (str): Glob pattern to match image files.
            wait_time (int, optional): Base wait time in seconds. Defaults to 10.

        Returns:
            tuple: (newest_image, newest_field, newest_filter, newest_exptime)
                Returns (None, None, None, None) if self.running becomes False.
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
                    total_wait_time = max(wait_time, 30)
                    elapsed_time = 0
                    while elapsed_time < total_wait_time and self.running:
                        time.sleep(0.1)
                        elapsed_time += 0.1

        # return None values if self.running is False
        return None, None, None, None

    def guider_loop(
        self, camera_name: str, glob_str: str, wait_time: int = 10, binning: int = 1
    ) -> None:
        """
        Main autoguiding loop that continuously monitors and corrects telescope pointing.

        Monitors a directory for new images, compares them to reference images,
        calculates pointing corrections, and applies telescope guide pulses.
        Handles field changes, pier side changes, and maintains guiding statistics.

        Parameters:
            camera_name (str): Name of the camera for logging and database records.
            glob_str (str): Glob pattern to match incoming image files.
            wait_time (int, optional): Time to wait for new images. Defaults to 10.
            binning (int, optional): Image binning factor for scaling corrections. Defaults to 1.

        Note:
            Sets self.running = True and continues until set to False.
            Automatically handles reference image selection and field stabilization.
        """
        self.running = True

        self.observatory.logger.info(f"Starting guider loop for: {glob_str} images")

        try:
            while self.running:
                # check telescope alignment mode
                gem = (
                    self.telescope.get("AlignmentMode") == AlignmentModes.algGermanPolar
                )

                if gem:
                    self.observatory.logger.info(
                        "Telescope is in German equatorial mode"
                    )

                telescope_pierside = self.telescope.get("SideOfPier")

                # get a list of the images in the directory
                templist = g.glob(glob_str)

                # take directory of glob_str and add logfile name
                LOGFILE = os.path.join(os.path.dirname(glob_str), "guider.log")

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
                            current_field,
                            current_filter,
                            current_exptime,
                            camera_name,
                            telescope_pierside,
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
                                telescope_pierside,
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
                    subtract_bkg=False,
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
                    ) = self.waitForImage(n_images, camera_name, glob_str, wait_time)

                    # to insure file is fully written to disc
                    time.sleep(1)

                    # check still same pierside, else reset loop?
                    if gem:
                        current_pierside = self.telescope.get("SideOfPier")
                        if current_pierside != telescope_pierside:
                            self.logMessageToDb(
                                camera_name,
                                "Pierside changed from {} to {}, resetting guider loop...".format(
                                    telescope_pierside, current_pierside
                                ),
                            )
                            self.observatory.logger.info(
                                f"Pierside changed from {telescope_pierside} to {current_pierside}, resetting guider loop..."
                            )
                            break

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
                            self.PIDx = PID.from_config_dict(self.PID_COEFFS["x"])
                            self.PIDy = PID.from_config_dict(self.PID_COEFFS["y"])
                            self.PIDx.initialize_set_point(self.PID_COEFFS["set_x"])
                            self.PIDy.initialize_set_point(self.PID_COEFFS["set_y"])
                        elif images_to_stabilise > 0:
                            self.logMessageToDb(
                                camera_name, "Stabilising using P=1.0, I=0.0, D=0.0"
                            )
                            self.PIDx = PID(1.0, 0.0, 0.0)
                            self.PIDy = PID(1.0, 0.0, 0.0)
                            self.PIDx.initialize_set_point(self.PID_COEFFS["set_x"])
                            self.PIDy.initialize_set_point(self.PID_COEFFS["set_y"])

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
                            if self.running:
                                (
                                    applied,
                                    post_pid_x,
                                    post_pid_y,
                                    std_buff_x,
                                    std_buff_y,
                                ) = self.guide(
                                    pre_pid_x,
                                    pre_pid_y,
                                    images_to_stabilise,
                                    camera_name,
                                    binning,
                                    gem,
                                )
                            else:
                                break

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

                        # log the shifts to the logger
                        self.observatory.logger.info(
                            "Guider post_pid_x shift: {:.2f}".format(post_pid_x)
                        )
                        self.observatory.logger.info(
                            "Guider post_pid_y shift: {:.2f}".format(post_pid_y)
                        )

                        # reset the comparison templist so the nested while(1) loop
                        # can find new images
                        templist = g.glob(glob_str)
                        n_images = len(templist)
        except Exception as e:
            self.running = False
            self.observatory.report_device_issue(
                device_type="Guider",
                device_name=self.telescope.device_name,
                message="Error in guide loop",
                exception=e,
            )

        self.observatory.logger.info(f"Stopping guider loop for: {glob_str} images")


"""
PID loop controller
"""


class PID:
    """
    Discrete PID controller for autoguiding corrections.

    Implements a digital PID control loop with configurable proportional, integral,
    and derivative gains. Includes integrator clamping to prevent windup.

    Based on: http://code.activestate.com/recipes/577231-discrete-pid-controller/

    Parameters:
        kp (float, optional): Proportional gain. Defaults to 0.5.
        ki (float, optional): Integral gain. Defaults to 0.25.
        kd (float, optional): Derivative gain. Defaults to 0.0.
        derivator (float, optional): Initial derivative term. Defaults to 0.
        integrator (float, optional): Initial integrator value. Defaults to 0.
        integrator_max (float, optional): Maximum integrator value. Defaults to 500.
        integrator_min (float, optional): Minimum integrator value. Defaults to -500.
    """

    def __init__(
        self,
        kp: float = 0.5,
        ki: float = 0.25,
        kd: float = 0.0,
        derivator: float = 0,
        integrator: float = 0,
        integrator_max: float = 500,
        integrator_min: float = -500,
    ) -> None:
        self.kp: float = kp
        self.ki: float = ki
        self.kd: float = kd
        self.derivator: float = derivator
        self.integrator: float = integrator
        self.integrator_max: float = integrator_max
        self.integrator_min: float = integrator_min
        self.set_point: float = 0.0
        self.error: float = 0.0
        self.p_value: float = 0.0
        self.d_value: float = 0.0
        self.i_value: float = 0.0

    def update(self, current_value: float) -> float:
        """
        Calculate PID output for given input and feedback.

        Parameters:
            current_value (float): Current process value (feedback).

        Returns:
            float: PID controller output.
        """
        self.error = self.set_point - current_value
        self.p_value = self.kp * self.error
        self.d_value = self.kd * (self.error - self.derivator)
        self.derivator = self.error
        self.integrator = self.integrator + self.error
        if self.integrator > self.integrator_max:
            self.integrator = self.integrator_max
        elif self.integrator < self.integrator_min:
            self.integrator = self.integrator_min
        self.i_value = self.integrator * self.ki
        pid = self.p_value + self.i_value + self.d_value
        return pid

    def initialize_set_point(self, set_point: float) -> None:
        """
        Initialize the PID setpoint and reset integrator/derivator.

        Parameters:
            set_point (float): Desired target value.
        """
        self.set_point = set_point
        self.integrator = 0
        self.derivator = 0

    @classmethod
    def from_config_dict(cls, config: Dict[str, float]) -> "PID":
        """
        Create a PID instance from a configuration dictionary.

        Parameters:
            config (dict): Configuration with keys 'p', 'i', 'd'
        """
        return cls(
            kp=config["p"],
            ki=config["i"],
            kd=config["d"],
        )
