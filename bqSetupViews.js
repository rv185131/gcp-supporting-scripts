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
exports.createAllViewsJag = exports.createMaterializedViewJag = void 0;
const BigQuery = require("@google-cloud/bigquery");
const fs = require("fs");
let bq = new BigQuery.BigQuery();
function createMaterializedViewJag(projectId, datasetId, materializedViewId) {
    return __awaiter(this, void 0, void 0, function* () {
        console.log(`Creating Materialized View for ${projectId}.${datasetId}.${materializedViewId}`);
        const query = `CREATE MATERIALIZED VIEW \`${projectId}.${datasetId}.${materializedViewId}\` AS
        SELECT customer, store , date, DATE(date) as day, pumpNumber, message
        FROM \`${projectId}.${datasetId}.jag_logs\`
        WHERE (message like '%icr offline event%') OR --ICR Offline
        (message like '%EpsilonAuthInfo POSTran#:%') OR --EPS Info
        (message like '%Unable to Send Heartbeat Msg%') OR --Heartbeat not sent
        (message like '%DC has not received%') OR --Heartbeat not received
        (message like '%Icarus idle time is at%') OR -- High CPU Usage
        (message like '%ICR down%') OR --ICR Down
        (message like '%Failed to Load Icarus%') OR --Failed Icarus Load
        (message like '%Started program%') OR --Starting Executables (Icarus/DevMan/PCM)
        (message like '%Start WaitForSPOTOnlineEvent%') -- Connected to ICR, no Online signal
        GROUP BY  customer, date, store, day, pumpNumber, message`;
        const options = {
            query: query,
            // Location must match that of the dataset(s) referenced in the query.
            location: 'US',
        };
        try {
            const [job] = yield bq.createQueryJob(options);
            const [results] = yield job.getQueryResults();
            console.log(query);
            console.log(`Result: ${results}`);
            console.log(job.metadata.statistics);
        }
        catch (err) {
            console.log(`Could not create ${projectId}.${datasetId}.${materializedViewId}`);
            console.log(err);
        }
    });
}
exports.createMaterializedViewJag = createMaterializedViewJag;
function createAllViewsJag(projectId, datasetId, materializedViewId, viewId, JSON_FILE) {
    return __awaiter(this, void 0, void 0, function* () {
        //get data from json
        const queryDataRaw = fs.readFileSync(`./${JSON_FILE}.json`, 'utf-8');
        const queryData = JSON.parse(queryDataRaw);
        console.log(queryData);
        var queries = Object.keys(queryData);
        for (var i = 0; i < queries.length; i++) {
            let viewName = queries[i];
            let queryString = queryData[queries[i]];
            const query = `CREATE VIEW \`${projectId}.${datasetId}.${viewId}${viewName}\` AS
        SELECT customer, store, DATE(date) as day, pumpNumber, sum(1) as ${viewName}
        FROM \`${projectId}.${datasetId}.${materializedViewId}\`
        WHERE message like '%${queryString}%'
        GROUP BY customer, store, day, pumpNumber
        ORDER BY customer, store, day, pumpNumber`;
            const options = {
                query: query,
                // Location must match that of the dataset(s) referenced in the query.
                location: 'US',
            };
            try {
                const [job] = yield bq.createQueryJob(options);
                //const [results] = await job.getQueryResults();
                console.log(query);
                //console.log(`Result: ${results}`);
                console.log(job.metadata.statistics);
            }
            catch (err) {
                console.log(`Could not create ${projectId}.${datasetId}.${viewId}${viewName}`);
                console.log(err);
            }
        }
    });
}
exports.createAllViewsJag = createAllViewsJag;
//# sourceMappingURL=bqSetupViews.js.map