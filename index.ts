import { Command } from 'commander';
const program = new Command();

import * as bqViews from './bqSetupViews';

//options.requiredOption('-v, --views <views>', 'Views', 'views');
program
  .option('-p, --project <project>', 'GCP Project')
  .option('-d, --dataset <dataset>', 'BQ Dataset')
  .option('-t, --logType <logType>', 'CFR Log Type') //jag/optic/eps/rfs
  .option('-i, --input_file <input_file>', 'Input JSON File')
  .option('--mvName <mvName>','Custom Materialized View Name','BHIs');

program.parse(process.argv);
const options = program.opts();

const PROJECT = options.project;
const DATASET = options.dataset;
const LOG_TYPE = options.logType;
const MV_NAME = options.mvName;
let JSON_FILE = options.input_file;
const supportedLogTypes = ['jag', 'optic', 'eps', 'rfs'];

console.log(program.opts());
`
TODO:
Add command line options for different scripts
Add a check to see that the logs table is not empty before adding a MV.
Validate input JSON file for formatting and completeness
Give stats about jobs and list all MV/Vs created
`

main();

async function main() {
    //check for options.
    if(PROJECT == null) {
        console.log(`Project not defined. Use -p flag to specify target project.`);
    }
    else if (DATASET == null)
    {
        console.log(`Dataset not defined. Use -d flag to specify dataset.`);
    }
    else if (LOG_TYPE == null || !supportedLogTypes.includes(LOG_TYPE))
    {
        if (LOG_TYPE == null) {
            console.log(`Log Type not defined. Use -d flag to specify dataset.`);
        } else {
            console.log(`Unsupported log type option '-t' used: ${LOG_TYPE}. Supported options: 'jag', 'optic', 'eps', 'rfs'.`)
        }
        
    }
    else
    {
        //Set the input log file to a BHIs one unless another one was specified.
        if (JSON_FILE == null) {
            JSON_FILE = `${LOG_TYPE}_BHIs`;
        }
        console.log(`Materialized View Name: ${MV_NAME}. INPUT JSON: ${JSON_FILE}`)
        const mvId = `${LOG_TYPE}_mv_${MV_NAME}`;
        const vId = `${LOG_TYPE}_v_`
        
        await bqViews.createMaterializedViewJag(PROJECT, DATASET, mvId, JSON_FILE);
        await bqViews.createAllViewsJag(PROJECT, DATASET,mvId, vId, JSON_FILE);
    }
}