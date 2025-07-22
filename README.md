# CMIP6-Atlas Backend

This repository contains the backend code for the web viewer (to be created) of the [CMIP6-Atlas project](https://github.com/jackiryan/cmip6-atlas). All the global regions that the climate data is aggregated on are loaded into a regions database table, and the associated JSON files for each climate metric are loaded into a climate_data table. A FastAPI service provides running averages over specified temporal windows on the data. Commonly accessed averages are cached in the database.

Tiles are served via maplibre martin.