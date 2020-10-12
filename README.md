# tc-risk-forecast

Code needed to set up a [CLIMADA](https://github.com/CLIMADA-project/climada_python) based tropical cyclone risk forecasting system.

In the course of my master's thesis at [ETHZ WCR](https://wcr.ethz.ch/) and [Red Cross/Red Crescent Climate Centre](https://www.climatecentre.org/), I aim to effectively visualize tropical cyclone risk forecasts. This tool focuses on:

- Wind based risk, following Greg Holland's 2008 [_Revised Hurricane Pressure–Wind Model_](https://doi.org/10.1175/2008MWR2395.1) in conjunction with ECMWF [forecast tracks](https://www.ecmwf.int/en/forecasts/datasets/wmo-essential#Essential_Tropical). Omitting storm surges and precipitation for the time being.
- The spatial coincidence with human settlements/people, as represented by the [High Resolution Settlement Layer](https://arxiv.org/abs/1712.05839) (available on the [Humanitarian Data Exchange](https://data.humdata.org/search?res_format=zipped%20geotiff&organization=facebook&q=hrsl&ext_page_size=190&sort=title_case_insensitive%20asc#dataset-filter-start)).
- While there are many uncertainties that could be visually adressed, this tool consciously focuses on the uncertainty that the ensemble forecast members describe.

## Toolchain

_Graph coming soon_
