# SPDX-License-Identifier: EUPL-1.2
# Contact: felixd@gfz.de, besnard@gfz.de, urbazaev@gfz.de and amelia.holcomb@gmail.com.
# SPDX-FileCopyrightText: 2026 Felix Dombrowski
# SPDX-FileCopyrightText: 2026 Mikhail Urbazaev
# SPDX-FileCopyrightText: 2026 Simon Besnard
# SPDX-FileCopyrightText: 2026 Amelia Holcomb
# SPDX-FileCopyrightText: 2026 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences

from typing import Dict

from icesat2db.beam.Beam import beam_handler
from icesat2db.beam.atl03_beam import ATL03Beam
from icesat2db.granule.Granule import granule_handler


class ATL03Granule(granule_handler):
    """
    Represents a IceSat2 ATL03 granule, providing access to its beams and related data.

    This class extends the base Granule class and initializes with a specific file path,
    a field mapping that maps product variables to the corresponding data fields in the
    granule, and the signal-confidence filter settings used by ATL03Beam.

    Attributes:
        field_mapping (Dict[str, str]): A dictionary mapping product variables to HDF5 field names.
    """

    def __init__(
        self,
        file_path: str,
        field_mapping: Dict[str, str],
        confidence_column: int = 0,
        confidence_threshold: int = 3,
    ):
        """
        Initialize an ATL03Granule object.

        Parameters:
            file_path (str): Path to the IceSat-2 ATL03 granule file (HDF5 format).
            field_mapping (Dict[str, str]): Dictionary containing the mapping of product variables to data fields.
            confidence_column (int): Surface-type column into ``heights/signal_conf_ph`` to filter on.
            confidence_threshold (int): Minimum signal confidence to keep.
        """
        self.field_mapping = (
            field_mapping  # Initialize early to avoid missing attributes
        )
        self.confidence_column = confidence_column
        self.confidence_threshold = confidence_threshold

        try:
            super().__init__(file_path)  # Call parent constructor
            if not self._is_open:
                raise RuntimeError(
                    f"Failed to initialize ATL03Granule: {file_path} could not be opened."
                )
        except Exception as e:
            print(f"Error initializing ATL03Granule for {file_path}: {e}")
            self.file_path = (
                file_path  # Ensure this is always set to avoid AttributeError
            )
            self._is_open = False  # Mark as not open

    def validate_beam_name(self, beam: str) -> None:
        """
        Validate that the provided beam name exists in the granule.

        Parameters:
            beam (str): The name of the beam to validate.

        Raises:
            ValueError: If the specified beam name is not found in the granule.
        """
        if not self._is_open:
            raise RuntimeError(
                f"Cannot validate beams; granule file '{self.file_path}' is not open."
            )

        if beam not in self.beam_names:
            raise ValueError(
                f"Invalid beam name '{beam}' in file '{self.file_path}'. "
                f"Valid beam names: {self.beam_names}. Ensure the beam exists in the granule."
            )

    def _beam_from_name(self, beam: str) -> beam_handler:
        """
        Retrieve a specific beam from the granule by name.

        Parameters:
            beam (str): The name of the beam to retrieve (e.g., "gt1l").

        Returns:
            ATL03Beam: The corresponding ATL03Beam object for the given beam name.
        """
        self.validate_beam_name(beam)
        return ATL03Beam(
            self,
            beam,
            self.field_mapping,
            confidence_column=self.confidence_column,
            confidence_threshold=self.confidence_threshold,
        )
