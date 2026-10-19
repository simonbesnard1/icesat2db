
.. for doctest:
    >>> import icesat2db as isdb

.. _fundamentals-filters:

#################
Quality Filtering
#################

This guide details the default quality filtering applied to ICESat-2 ATL08 data in the icesat2DB package during data ingestion.

Overview
--------

icesat2DB applies a **minimal filter at ingestion** and leaves every other selection to query time. A 100 m land segment is stored whenever it has a valid terrain height, so the database holds terrain-only (non-vegetated), water and urban segments as well as vegetated ones. Choosing what counts as a usable segment for a given analysis (vegetated only, no water, no urban areas, night-time only, ...) is done on the fly with :py:class:`icesat2db.IceSat2Provider` without re-ingesting the data.

ATL08 Ingestion Filter
----------------------

Defined in :py:class:`icesat2db.ATL08Beam`, a single condition drops a segment:

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Variable
     - Condition and rationale
   * - ``h_te_best_fit``
     - Retained when below the HDF5 fill value (3.4028235×10³⁸) and above −999. A segment without a terrain height has no usable land surface.

Invalid Values Become NaN
-------------------------

Every other float field keeps the segment and stores invalid values as ``NaN``: values at or above the fill value, and additionally values at or below −999 for ``h_te_median``, ``h_te_uncertainty``, ``h_te_best_fit_20m`` and ``h_canopy_20m``. A terrain-only segment therefore has valid terrain fields and ``NaN`` canopy fields.

Query-time Filters
------------------

Filters are passed as keyword arguments to :py:meth:`icesat2db.IceSat2Provider.get_data`. ``NaN`` fails every ordering comparison (``>=``, ``<``, ...), so a range condition on a canopy field also excludes segments without canopy; ``!=`` keeps ``NaN`` values. The equivalent of the previous ingestion defaults is:

.. code-block:: python

   provider.get_data(
       variables, geometry=area,
       h_canopy=">= 0",              # vegetated segments only
       h_canopy_uncertainty=">= 0",
       h_te_uncertainty=">= 0",
       urban_flag="== 0",            # non-urban
       segment_watermask="== 0",     # non-water
   )

Other quality variables (e.g. ``night_flag``, ``layer_flag``, ``segment_landcover``, ``n_ca_photons``, ``n_te_photons``) can be filtered the same way. See :ref:`fundamentals-provider` for details.
