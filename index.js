"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
const commander_1 = require("commander");
const program = new commander_1.Command();
const bqViews = require("./bqSetupViews");
//options.requiredOption('-v, --views <views>', 'Views', 'views');
program
    .option('-p, --project <project>', 'GCP Project')
    .option('-d, --dataset <dataset>', 'BQ Dataset')
    .option('-t, --logType <logType>', 'CFR Log Type') //jag/optic/eps/rfs
    .option('-i, --input_file <input_file>', 'Input JSON File')
    .option('--mvName <mvName>', 'Custom Materialized View Name', 'BHIs');
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
`;
main();
function main() {
    return __awaiter(this, void 0, void 0, function* () {
        //check for options.
        if (PROJECT == null) {
            console.log(`Project not defined. Use -p flag to specify target project.`);
        }
        else if (DATASET == null) {
            console.log(`Dataset not defined. Use -d flag to specify dataset.`);
        }
        else if (LOG_TYPE == null || !supportedLogTypes.includes(LOG_TYPE)) {
            if (LOG_TYPE == null) {
                console.log(`Log Type not defined. Use -d flag to specify dataset.`);
            }
            else {
                console.log(`Unsupported log type option '-t' used: ${LOG_TYPE}. Supported options: 'jag', 'optic', 'eps', 'rfs'.`);
            }
        }
        else {
            //Set the input log file to a BHIs one unless another one was specified.
            if (JSON_FILE == null) {
                JSON_FILE = `BHIs_${LOG_TYPE}`;
            }
            console.log(`Materialized View Name: ${MV_NAME}. INPUT JSON: ${JSON_FILE}`);
            const mvId = `${LOG_TYPE}_mv_${MV_NAME}`;
            const vId = `${LOG_TYPE}_v_`;
            yield bqViews.createMaterializedViewJag(PROJECT, DATASET, mvId);
            yield bqViews.createAllViewsJag(PROJECT, DATASET, mvId, vId, JSON_FILE);
        }
    });
}
//# sourceMappingURL=index.js.map