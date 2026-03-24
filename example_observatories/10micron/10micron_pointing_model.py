"""
10Micron GM-HPS Mount – Astra Observatory Subclass
===================================================

Builds a pointing model using the mount's native newalig/newalpt/endalig
pipeline, communicating directly over TCP to the mount — the same approach
used by MountWizzard4.

Why direct TCP instead of ASCOM CommandString:
  The 10Micron ASCOM driver's internal caching polls the mount over a shared
  TCP connection. CommandString responses get interleaved with cached data,
  returning stale sidereal time values. MW4 avoids this entirely by opening
  its own independent TCP connection to port 3490. The mount supports up to
  10 simultaneous connections (firmware ≥2.9.10, protocol doc p. 3).

Flow:
  0. User sets MOUNT_IP below.
  1. Open direct TCP connection to mount
  2. :newalig# — open alignment buffer (existing model stays active)
  3. Astra's native loop — slew/expose/solve, we capture data pairs via
     ASCOM properties (safe with polling) in pointing_correction override
  4. :newalpt × N — feed all points over our TCP connection
  5. :endalig# — compute model atomically
  6. :modelsv0NAME# — save to flash
  7. Close TCP connection

Coordinate conventions (confirmed by MW4 source + Hans/10Micron devs):
  - MRA/MDEC: mount-reported JNow (read via ASCOM, converted from J2000)
  - PRA/PDEC: plate-solved J2000 → JNow (precession+nutation+aberration, NO refraction)
  - Dec format: sDD*MM:SS.S (asterisk separator, matching MW4)
  - Sidereal time: HH:MM:SS.SS (two decimals, matching MW4)

ASCOM driver prerequisites:
  - "Use J2000.0 ICRS Coordinates" should be CHECKED (our code reads J2000
    via ASCOM properties and converts to JNow for :newalpt)
  - "Enable Sync" does NOT need to be checked — we never sync the mount,
    the model is built entirely via :newalpt/:endalig over direct TCP
"""

from __future__ import annotations

import socket
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from astropy.wcs import WCS

from astra.observatory import Observatory
from astra.paired_devices import PairedDevices

MOUNT_IP = "192.168.1.11"  # Change if your mount is at a different IP address


class AlignmentPoint:
    """One data pair for :newalpt — all values in JNow protocol format."""

    __slots__ = (
        "mount_ra",
        "mount_dec",
        "pier_side",
        "solved_ra",
        "solved_dec",
        "sidereal_time",
    )

    def __init__(
        self,
        mount_ra: str,
        mount_dec: str,
        pier_side: str,
        solved_ra: str,
        solved_dec: str,
        sidereal_time: str,
    ):
        self.mount_ra = mount_ra
        self.mount_dec = mount_dec
        self.pier_side = pier_side
        self.solved_ra = solved_ra
        self.solved_dec = solved_dec
        self.sidereal_time = sidereal_time


class MountTCP:
    """Direct TCP connection to a 10Micron mount, like MW4's Connection class."""

    def __init__(self, host: str, port: int = 3490, timeout: float = 10.0, logger=None):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.logger = logger
        self.sock: Optional[socket.socket] = None

    def connect(self) -> bool:
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.settimeout(self.timeout)
            self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
            self.sock.connect((self.host, self.port))
            if self.logger:
                self.logger.info(f"10Micron TCP: Connected to {self.host}:{self.port}")
            return True
        except Exception as e:
            if self.logger:
                self.logger.error(f"10Micron TCP: Connect failed: {e}")
            self.sock = None
            return False

    def close(self) -> None:
        if self.sock:
            try:
                self.sock.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None
            if self.logger:
                self.logger.info("10Micron TCP: Connection closed.")

    def send_blind(self, cmd: str) -> None:
        """Send a command with no response expected."""
        if not self.sock:
            return
        full = f":{cmd}#"
        if self.logger:
            self.logger.info(f"10Micron TCP TX (blind): {full}")
        self.sock.sendall(full.encode())

    def send_cmd(self, cmd: str) -> str:
        """Send a command and read a '#'-terminated response."""
        if not self.sock:
            return ""
        full = f":{cmd}#"
        if self.logger:
            self.logger.info(f"10Micron TCP TX: {full}")
        self.sock.sendall(full.encode())

        # Read until '#'
        response = b""
        try:
            while True:
                chunk = self.sock.recv(1024)
                if not chunk:
                    break
                response += chunk
                if b"#" in response:
                    break
        except socket.timeout:
            if self.logger:
                self.logger.warning(f"10Micron TCP RX: timeout for {full}")
            return ""

        decoded = response.decode("ascii", errors="replace").strip("#")
        if self.logger:
            self.logger.info(f"10Micron TCP RX: '{decoded}'")
        return decoded


class TenMicron(Observatory):
    OBSERVATORY_ALIASES: List[str] = [
        "10micron",
        "gm1000",
        "gm2000",
        "gm3000",
        "gm4000",
    ]

    # ---- read mount position via ASCOM properties (safe with polling) ---

    def _read_mount_position(self, telescope) -> dict:
        """Read mount position using standard ASCOM properties."""
        ra_hours = telescope.get("RightAscension")  # hours (J2000 if driver configured)
        dec_deg = telescope.get("Declination")  # degrees
        sid_hours = telescope.get("SiderealTime")  # hours (local apparent)
        side_of_pier = telescope.get("SideOfPier")  # 0=pierEast, 1=pierWest

        pier_side = "E" if side_of_pier == 0 else "W"

        self.logger.info(
            f"10Micron: Mount pos RA={ra_hours:.6f}h Dec={dec_deg:.4f}° "
            f"LST={sid_hours:.6f}h Pier={pier_side}"
        )
        return {
            "ra_hours": ra_hours,
            "dec_deg": dec_deg,
            "sid_hours": sid_hours,
            "pier_side": pier_side,
        }

    # ---- formatting helpers (matching MW4) ------------------------------

    @staticmethod
    def _hours_to_hms(hours: float) -> str:
        """Decimal hours → HH:MM:SS.S"""
        h = hours % 24.0
        hh = int(h)
        rem = (h - hh) * 60.0
        mm = int(rem)
        ss = (rem - mm) * 60.0
        return f"{hh:02d}:{mm:02d}:{ss:04.1f}"

    @staticmethod
    def _deg_to_dms(deg: float) -> str:
        """Decimal degrees → sDD*MM:SS.S (asterisk separator, matching MW4)."""
        sign = "+" if deg >= 0 else "-"
        d = abs(deg)
        dd = int(d)
        rem = (d - dd) * 60.0
        mm = int(rem)
        ss = (rem - mm) * 60.0
        return f"{sign}{dd:02d}*{mm:02d}:{ss:04.1f}"

    @staticmethod
    def _hours_to_hms_2dec(hours: float) -> str:
        """Decimal hours → HH:MM:SS.SS (sidereal time, matching MW4)."""
        h = hours % 24.0
        hh = int(h)
        rem = (h - hh) * 60.0
        mm = int(rem)
        ss = (rem - mm) * 60.0
        return f"{hh:02d}:{mm:02d}:{ss:05.2f}"

    # ---- J2000 → JNow ---------------------------------------------------

    def _j2000_to_jnow(self, ra_hours: float, dec_deg: float) -> tuple[float, float]:
        """
        J2000 ICRS → JNow apparent topocentric.
        Precession + nutation + aberration, NO refraction.
        """
        try:
            import astropy.units as u
            from astropy.coordinates import SkyCoord

            coord = SkyCoord(
                ra=ra_hours * u.hourangle, dec=dec_deg * u.deg, frame="icrs"
            )
            apparent = coord.transform_to("cirs")
            return apparent.ra.hour, apparent.dec.deg
        except ImportError:
            self.logger.warning("astropy not available — using J2000 as JNow!")
            return ra_hours, dec_deg

    # ---- alignment info parsing and display -----------------------------

    def _log_alignment_info(self, info: str) -> None:
        """
        Parse and log :getain# response in human-readable format.

        Format: ZZZ.ZZZZ,+AA.AAAA,EE.EEEE,PPP.PP,+OO.OOOO,+aa.aa,+bb.bb,NN,RRRRR.R
        Fields can be 'E' if not calculated (e.g. <4 stars, QCI, altaz, lat>80°).
        """
        if not info or info == "E":
            self.logger.info("10Micron: Alignment info: not available (< 3 stars)")
            return

        parts = info.split(",")
        if len(parts) != 9:
            self.logger.warning(f"10Micron: Unexpected alignment info format: {info}")
            return

        def safe_float(val: str) -> Optional[float]:
            if val.strip() == "E":
                return None
            try:
                return float(val)
            except (ValueError, TypeError):
                return None

        az_axis = safe_float(parts[0])
        alt_axis = safe_float(parts[1])
        polar_err = safe_float(parts[2])
        pos_angle = safe_float(parts[3])
        ortho_err = safe_float(parts[4])
        az_turns = safe_float(parts[5])
        alt_turns = safe_float(parts[6])
        terms = safe_float(parts[7])
        rms = safe_float(parts[8])

        self.logger.info("=" * 60)
        self.logger.info("10Micron Alignment Model Summary")
        self.logger.info("=" * 60)

        # RA axis direction
        if az_axis is not None and alt_axis is not None:
            self.logger.info(
                f"  RA axis direction:     Az = {az_axis:.4f}°, Alt = {alt_axis:.4f}°"
            )

        # Polar alignment error
        if polar_err is not None:
            polar_arcmin = polar_err * 60.0
            self.logger.info(
                f"  Polar alignment error: {polar_err:.4f}° ({polar_arcmin:.1f} arcmin)"
            )
        else:
            self.logger.info("  Polar alignment error: N/A (altaz mount)")

        # Position angle of polar error
        if pos_angle is not None:
            self.logger.info(
                f"  Polar error direction: {pos_angle:.2f}° (position angle)"
            )

        # Orthogonality error
        if ortho_err is not None:
            ortho_arcmin = ortho_err * 60.0
            self.logger.info(
                f"  Orthogonality error:   {ortho_err:.4f}° ({ortho_arcmin:.1f} arcmin)"
            )
        else:
            self.logger.info("  Orthogonality error:   N/A (< 3 stars)")

        # Knob adjustment recommendations
        if az_turns is not None:
            direction = "LEFT" if az_turns > 0 else "RIGHT" if az_turns < 0 else "none"
            self.logger.info(
                f"  Azimuth knob:          {abs(az_turns):.2f} turns {direction}"
            )
        if alt_turns is not None:
            direction = "DOWN" if alt_turns > 0 else "UP" if alt_turns < 0 else "none"
            self.logger.info(
                f"  Altitude knob:         {abs(alt_turns):.2f} turns {direction}"
            )

        # Model terms and RMS
        if terms is not None:
            self.logger.info(f"  Model terms:           {int(terms)}")
        else:
            self.logger.info("  Model terms:           N/A (< 4 stars)")

        if rms is not None:
            self.logger.info(f"  Expected RMS error:    {rms:.1f} arcsec")
        else:
            self.logger.info("  Expected RMS error:    N/A (< 4 stars)")

        self.logger.info("=" * 60)

    # ---- resolve mount IP -----------------------------------------------

    def _get_mount_ip(self, telescope) -> str:
        """
        Try to get the mount IP. Users can override by setting
        mount_ip in the device config. Falls back to the default.
        """
        try:
            config = self.devices_config.get("Telescope", {})
            if "mount_ip" in config:
                return config["mount_ip"]
        except Exception:
            pass
        return MOUNT_IP

    # ---- main sequence -------------------------------------------------

    def pointing_model_sequence(self, action, paired_devices: PairedDevices) -> None:
        """
        Build a 10Micron pointing model using direct TCP (like MW4).

        1. Open TCP to mount → :newalig# (existing model stays active)
        2. Astra's native loop captures data pairs (via ASCOM properties)
        3. Feed all points via :newalpt over TCP → :endalig#
        4. Save model → close TCP
        """
        telescope = paired_devices.get_device("Telescope")
        self._alignment_points = []

        mount_ip = self._get_mount_ip(telescope)
        tcp = MountTCP(mount_ip, port=3490, logger=self.logger)

        if not tcp.connect():
            raise RuntimeError(
                f"10Micron: Cannot connect to mount at {mount_ip}:3490. "
                "Check network and mount_ip in device config."
            )

        try:
            # --- PRE: open alignment buffer ---
            resp = tcp.send_cmd("newalig")
            if resp != "V":
                raise RuntimeError(f"10Micron: :newalig# failed (got '{resp}')")

            n_existing = tcp.send_cmd("getalst")
            self.logger.info(
                f"10Micron: Alignment buffer open. "
                f"Existing model ({n_existing} stars) active for slewing."
            )

            # --- RUN: Astra's native loop ---
            super().pointing_model_sequence(action, paired_devices)

            # --- POST: feed points and compute model ---
            n_pts = len(self._alignment_points)
            self.logger.info(f"10Micron: Feeding {n_pts} points to mount.")

            if n_pts < 3:
                self.logger.warning(
                    f"10Micron: Only {n_pts} points — need ≥3. Existing model retained."
                )
                return

            accepted = 0
            for i, pt in enumerate(self._alignment_points, 1):
                cmd = (
                    f"newalpt{pt.mount_ra},{pt.mount_dec},{pt.pier_side},"
                    f"{pt.solved_ra},{pt.solved_dec},{pt.sidereal_time}"
                )
                resp = tcp.send_cmd(cmd)
                if resp == "E":
                    self.logger.warning(f"  Point {i}/{n_pts} rejected.")
                else:
                    accepted += 1
                    self.logger.info(f"  Point {i}/{n_pts} accepted (buffer: {resp}).")

            self.logger.info(f"10Micron: {accepted}/{n_pts} points accepted.")

            # Compute model
            result = tcp.send_cmd("endalig")
            if result == "V":
                n_stars = tcp.send_cmd("getalst")
                self.logger.info(f"10Micron: New model active with {n_stars} stars.")

                # Save (MW4 pattern: delete then save)
                name = f"A_{datetime.now().strftime('%y%m%d_%H%M')}"
                tcp.send_cmd(f"modeldel0{name}")
                save_resp = tcp.send_cmd(f"modelsv0{name}")
                if save_resp == "1":
                    self.logger.info(f"10Micron: Model saved as '{name}'.")
                else:
                    self.logger.warning(
                        f"10Micron: Model active but save returned '{save_resp}'."
                    )

                # Log alignment quality
                info = tcp.send_cmd("getain")
                self._log_alignment_info(info)
            else:
                self.logger.warning(
                    f"10Micron: :endalig# returned '{result}' — "
                    "existing model retained."
                )

        finally:
            tcp.close()

    # ---- Override: capture data pairs ------------------------------------

    def pointing_correction(
        self,
        action,
        filepath: str | Path | None,
        paired_devices: PairedDevices,
        dark_frame: str | Path | None = None,
        sync: bool = False,
        slew: bool = True,
    ) -> tuple[bool, WCS | None]:
        """
        Override for 10Micron model building.

        When sync=True (called from pointing_model_sequence): capture the
        mount/solved data pair for :newalpt, then plate-solve WITHOUT syncing.
        The model is built entirely via :newalpt/:endalig over direct TCP.

        When sync=False (called from other Astra workflows): pass through
        to the parent unchanged.
        """
        if sync:
            telescope = paired_devices.get_device("Telescope")

            # Snapshot mount position BEFORE plate-solve
            mount_pos = self._read_mount_position(telescope)

            # Convert mount-reported J2000 → JNow for :newalpt
            mount_ra_jnow, mount_dec_jnow = self._j2000_to_jnow(
                mount_pos["ra_hours"], mount_pos["dec_deg"]
            )

            # Let Astra plate-solve only — override sync to False
            pointing_complete, wcs_solve = super().pointing_correction(
                action,
                filepath,
                paired_devices,
                dark_frame=dark_frame,
                sync=False,
                slew=slew,
            )

            # If solve succeeded, store the data pair
            if wcs_solve is not None:
                action_value = action.action_value
                solved_ra_j2000_deg = action_value["ra"]
                solved_dec_j2000_deg = action_value["dec"]

                solved_ra_jnow, solved_dec_jnow = self._j2000_to_jnow(
                    solved_ra_j2000_deg / 15.0,
                    solved_dec_j2000_deg,
                )

                point = AlignmentPoint(
                    mount_ra=self._hours_to_hms(mount_ra_jnow),
                    mount_dec=self._deg_to_dms(mount_dec_jnow),
                    pier_side=mount_pos["pier_side"],
                    solved_ra=self._hours_to_hms(solved_ra_jnow),
                    solved_dec=self._deg_to_dms(solved_dec_jnow),
                    sidereal_time=self._hours_to_hms_2dec(mount_pos["sid_hours"]),
                )
                self._alignment_points.append(point)
                self.logger.info(
                    f"10Micron: Captured point #{len(self._alignment_points)} — "
                    f"mount=({point.mount_ra}, {point.mount_dec}) "
                    f"solved=({point.solved_ra}, {point.solved_dec}) "
                    f"LST={point.sidereal_time} pier={point.pier_side}"
                )

            return pointing_complete, wcs_solve
        else:
            # Non-model-building calls — pass through unchanged
            return super().pointing_correction(
                action,
                filepath,
                paired_devices,
                dark_frame=dark_frame,
                sync=sync,
                slew=slew,
            )
