# gcp-scripts
## Create Materialized View and Views
This script will create all the views and materialized views.
Currently supports: JAG
By default it will create the Materilized View and Views considered to be BHIs.

It can also take a custom .json file for a custom set of MV/Vs.

### Use
node index.js -p `[PROJECT]` -d `[DATASET]` -t `[CFR_PRODUCT]`

#### Example
node index.js -p loganalysis-278801 -d Wawa -t jag