# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felixd@gfz.de and urbazaev@gfz.de
# SPDX-FileCopyrightText: 2026 Felix Dombrowski
# SPDX-FileCopyrightText: 2026 Mikhail Urbazaev
# SPDX-FileCopyrightText: 2026 Simon Besnard
# SPDX-FileCopyrightText: 2026 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences


from typing import Dict

from icesat2db.beam.Beam import beam_handler
from icesat2db.beam.atl08_beam import ATL08Beam
from icesat2db.granule.Granule import granule_handler


class ATL08Granule(granule_handler):
    """
    Represents a IceSat2 ATL08 granule, providing access to its beams and related data.

    This class extends the base Granule class and initializes with a specific file path and
    a field mapping that maps product variables to the corresponding data fields in the granule.

    Attributes:
        field_mapping (Dict[str, str]): A dictionary mapping product variables to HDF5 field names.
    """

    def __init__(self, file_path: str, field_mapping: Dict[str, str]):
        """
        Initialize an ATL08Granule object.

        Parameters:
            file_path (str): Path to the IceSat-2 ATL08 granule file (HDF5 format).
            field_mapping (Dict[str, str]): Dictionary containing the mapping of product variables to data fields.
        """
        self.field_mapping = (
            field_mapping  # Initialize early to avoid missing attributes
        )

        try:
            super().__init__(file_path)  # Call parent constructor
            if not self._is_open:
                raise RuntimeError(
                    f"Failed to initialize ATL08Granule: {file_path} could not be opened."
                )
        except Exception as e:
            print(f"Error initializing ATL08Granule for {file_path}: {e}")
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
            ATL08Beam: The corresponding ATL08Beam object for the given beam name.
        """
        self.validate_beam_name(beam)
        return ATL08Beam(self, beam, self.field_mapping)
