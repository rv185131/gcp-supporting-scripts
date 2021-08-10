import * as BigQuery from "@google-cloud/bigquery";
import * as fs from 'fs';

let bq = new BigQuery.BigQuery();

export async function createMaterializedViewJag(projectId: any, datasetId: any, materializedViewId: any) {

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
        //dryRun: true, //delete this to actually run query
    };
    try {
        const [job] = await bq.createQueryJob(options);
        const [results] = await job.getQueryResults();
        console.log(query);
        console.log(`Result: ${results}`);
        console.log(job.metadata.statistics);
    }
    catch(err)
    {
        console.log(`Could not create ${projectId}.${datasetId}.${materializedViewId}`);
        console.log(err);
    }

}

export async function createAllViewsJag(projectId: string, datasetId: string, materializedViewId: string, viewId: string, JSON_FILE: string) {
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
            //dryRun: true, //delete this to actually run query
        };
        try {
            const [job] = await bq.createQueryJob(options);
            //const [results] = await job.getQueryResults();
            console.log(query);
            //console.log(`Result: ${results}`);
            console.log(job.metadata.statistics);
        }
        catch (err)
        {
            console.log(`Could not create ${projectId}.${datasetId}.${viewId}${viewName}`);
            console.log(err);
        }

    }
}
