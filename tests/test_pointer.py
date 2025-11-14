import datetime
from unittest.mock import MagicMock

import cabaret
import numpy as np
import pytest
from astropy.coordinates import SkyCoord

from astra.pointer import (
    ImageStarMapping,
    PointingCorrection,
    calculate_pointing_correction_from_image,
)


class TestPointingCorrection:
    def setup_method(self):
        """Setup method to create a PointingCorrection instance for testing."""
        self.pc = PointingCorrection(
            target_ra=10.685,
            target_dec=41.269,
            plating_ra=10.68471,
            plating_dec=41.26917,
        )

    def test_offset_ra(self):
        assert self.pc.offset_ra == pytest.approx(-0.00029, rel=1e-5)

    def test_offset_dec(self):
        assert self.pc.offset_dec == pytest.approx(0.00017, rel=1e-5)

    def test_angular_separation(self):
        expected_separation = (
            SkyCoord(10.685, 41.269, unit="deg")
            .separation(SkyCoord(10.68471, 41.26917, unit="deg"))
            .deg
        )
        assert self.pc.angular_separation == pytest.approx(
            expected_separation, rel=1e-5
        )

    def test_proxy_ra(self):
        assert self.pc.proxy_ra == pytest.approx(10.685 - self.pc.offset_ra, rel=1e-5)

    def test_proxy_dec(self):
        assert self.pc.proxy_dec == pytest.approx(41.269 - self.pc.offset_dec, rel=1e-5)
        assert self.pc.proxy_dec == pytest.approx(41.269 - self.pc.offset_dec, rel=1e-5)


class TestImageStarMapping:
    def setup_method(self):
        """Setup method to create an ImageStarMapping instance for testing."""
        # Sample data for stars in image
        self.stars_in_image = np.array(
            [
                [468.88477963, 544.42659689],
                [300.85801364, 625.8030695],
                [1003.4834597, 150.39217117],
                [750.29660867, 791.5817885],
                [587.57745046, 855.28773308],
                [41.39460527, 810.50233526],
                [990.99531148, 233.73375796],
                [466.27547694, 359.25843622],
                [905.01111519, 828.09614696],
                [229.108765, 493.07829969],
                [720.73893833, 292.91002],
                [552.70391959, 610.32916217],
            ]
        )

        # Gaia star coordinates
        gaia_star_coordinates = np.array(
            [
                [100.01491039, 33.97676867],
                [99.99400567, 34.03520055],
                [99.98173261, 34.05121314],
                [100.03449937, 33.96889903],
                [99.95255415, 34.01485141],
                [99.98210632, 33.95287099],
                [100.00509785, 34.03067191],
                [100.00107344, 33.94671449],
                [100.06474074, 33.95102993],
                [99.95401636, 34.0067917],
                [99.96407522, 33.94933723],
                [99.96916866, 34.03919279],
                [100.04286216, 33.98172489],
                [100.01315545, 34.03071217],
                [99.98554204, 34.0010843],
                [100.01527488, 33.99459381],
                [100.00513555, 33.970395],
                [99.97992162, 33.95675571],
                [99.96538514, 33.96875204],
                [99.93579984, 34.00152493],
                [99.98735502, 34.01575219],
                [99.95599639, 34.01474556],
                [100.01188129, 33.98281021],
                [100.02898152, 33.96938376],
            ]
        )

        self.mapping = ImageStarMapping.from_gaia_coordinates(
            self.stars_in_image, gaia_star_coordinates
        )

    def test_get_plating_center(self):
        plating_center = self.mapping.get_plating_center((400, 400))
        assert isinstance(plating_center, tuple)
        assert len(plating_center) == 2

    def test_skycoord_to_pixels(self):
        ra, dec = 100.0, 34.0
        pixel_coords = self.mapping.skycoord_to_pixels(ra, dec)
        assert isinstance(pixel_coords, tuple)
        assert len(pixel_coords) == 2

    def test_find_gaia_match(self):
        matched_gaia_stars, distances = self.mapping.find_gaia_match()
        assert matched_gaia_stars.shape == (self.stars_in_image.shape[0], 2)
        assert distances.shape == (self.stars_in_image.shape[0],)

    def test_number_of_matched_stars(self):
        # Mock the find_gaia_match method to return a fixed distance array
        self.mapping.find_gaia_match = MagicMock(
            return_value=(
                self.mapping.gaia_stars_in_image,
                np.array([5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5]),
            )
        )

        matched_count = self.mapping.number_of_matched_stars(pixel_threshold=10)
        assert matched_count == 12

        matched_count = self.mapping.number_of_matched_stars(pixel_threshold=10)
        assert matched_count == 12


class TestPointingCorrectionFromImage:
    @pytest.fixture(autouse=True)
    def setup_pointing(self, temp_config):
        # if the file at Config().gaia_db is empty skip the test
        if not temp_config.gaia_db or temp_config.gaia_db.stat().st_size == 0:
            pytest.skip("Skipping as gaia_db is not set up")

        self.observatory = cabaret.Observatory(
            name="MyObservatory",
            camera=cabaret.Camera(
                height=1024,
                width=1024,
            ),
        )
        self.ra = 100
        self.dec = 34
        self.real_ra, self.real_dec = self.ra + 0.01, self.dec - 0.02
        self.dateobs = datetime.datetime(
            2025, 3, 1, 21, 1, 35, 86730, tzinfo=datetime.timezone.utc
        )
        self.image = self.observatory.generate_image(
            ra=self.real_ra,
            dec=self.real_dec,
            exp_time=30,
            dateobs=self.dateobs,
        )
        (
            self.pointing_correction,
            self.image_star_mapping,
            self.num_stars_used,
        ) = calculate_pointing_correction_from_image(
            self.image,
            target_ra=self.ra,
            target_dec=self.dec,
            dateobs=self.dateobs,
            plate_scale=self.observatory.camera.plate_scale / 3600,
        )

    def test_pointing_correction(self):
        """Test that the pointing correction is correctly initialized."""
        assert isinstance(self.pointing_correction, PointingCorrection)
        assert isinstance(self.image_star_mapping, ImageStarMapping)
        assert isinstance(self.num_stars_used, int)
        assert self.num_stars_used > 0

    def test_pointing_correction_values(self):
        """Test the values of the pointing correction."""
        pc = self.pointing_correction
        assert pc.target_ra == self.ra
        assert pc.target_dec == self.dec
        assert pc.plating_ra != pc.target_ra  # Ensure some correction is applied
        assert pc.plating_dec != pc.target_dec  # Ensure some correction is applied

    def test_image_star_mapping(self):
        """Test the image star mapping."""
        ism = self.image_star_mapping
        assert isinstance(ism, ImageStarMapping)
        assert ism.stars_in_image.shape[0] > 0  # Ensure some stars were detected
        assert ism.gaia_stars_in_image.shape[0] > 0  # Ensure some Gaia stars were found
        assert ism.stars_in_image.shape[0] > 0  # Ensure some stars were detected
        assert ism.gaia_stars_in_image.shape[0] > 0  # Ensure some Gaia stars were found
