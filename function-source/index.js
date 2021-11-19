/**
 * Triggered from a message on a Cloud Pub/Sub topic.
 *
 * @param {!Object} event Event payload.
 * @param {!Object} context Metadata for the event.
 */

// Imports the Google Cloud client library
// const {PubSub} = require('@google-cloud/pubsub');
const {BigQuery} = require('@google-cloud/bigquery');

// Creates a client; cache this for further use
// const pubSubClient = new PubSub();
const bigquery = new BigQuery();

//BigQuery variables
const DATASET = '<YOUR BIGQUERY DATASET>'
const TABLE = '<YOUR BIGQUERY TABLE>';
const PROJECT = '<YOUR GCP PROJECT>';
const DATASET_LOCATION = '<YOUR REGION>';

// Webhook
const fetch = require('node-fetch');

exports.process = async function process(event, context) {
    
    pubsubMessage = event;
    pubsubData = JSON.parse(Buffer.from(pubsubMessage.data, 'base64').toString());
    const formatter = new Intl.NumberFormat('en-US', {style: 'currency', currency: 'USD', minimumFractionDigits: 2});
    billingAccountId = pubsubMessage.attributes.billingAccountId;
    budgetName = pubsubData.budgetDisplayName;

    let threshold = (pubsubData.alertThresholdExceeded*100).toFixed(0);
    
    if (!isFinite(threshold)){
        threshold = 0;
    }

    costAmount = formatter.format(pubsubData.costAmount);
    budgetAmount = formatter.format(pubsubData.budgetAmount);
    budgetAmountType = pubsubData.budgetAmountType;
    currencyCode = pubsubData.currencyCode;
    createdAt = new Date().toISOString();
  
    // Write data to BigQuery dataset
    const rows = [{billingAccountId: billingAccountId,
                    budgetName: budgetName,
                    threshold: threshold,
                    costAmount: pubsubData.costAmount, 
                    budgetAmount: pubsubData.budgetAmount,
                    budgetAmountType: budgetAmountType,
                    currencyCode: currencyCode,
                    createdAt: createdAt}]

    // Confirm that the Event data has been parsed from PubSub
    console.log(rows);

    // Write the event data to BigQuery
    await bigquery.dataset(DATASET).table(TABLE).insert(rows);
    
    console.log(budgetName,' written to BQ!');

    // For each event, determine the count per threshold and billing account. This is done to ensure only the first 
    // event per threshold and billing account will be emitted via webhook to Google Chat
    const query = `SELECT count(*) cnt
                    FROM \`${PROJECT}.${DATASET}.${TABLE}\`
                    WHERE createdAt >  TIMESTAMP( DATE(EXTRACT(YEAR FROM CURRENT_DATE()) , EXTRACT(MONTH FROM CURRENT_DATE()), 1))
                    AND threshold = ${threshold} and billingAccountId = '${billingAccountId}'
                    `;
    
    const options = {
        query: query,
        location: DATASET_LOCATION,
    };

    const [job] = await bigquery.createQueryJob(options);
    
    // Wait for the query to finish
    const [results] = await job.getQueryResults();
    console.log('Count is: ',results[0].cnt);
 
    // query2 compares cost for each row vs the previous row and then calculates the average of all diffs, 
    // ignoring 0 diff entries
    const query2 = `SELECT 
                        CAST(AVG(diff) AS BIGNUMERIC) as AverageSpend
                    FROM
                        (SELECT 
                            *,
                            costAmount - prev_costAmount as diff
                        FROM
                            (SELECT budgetName,
                                    costAmount, 
                                    createdAt,
                                    LAG(costAmount, 1) OVER (PARTITION BY budgetName ORDER BY createdAt) AS prev_costAmount
                            FROM \`${PROJECT}.${DATASET}.${TABLE}\`
                            WHERE createdAt >  TIMESTAMP( DATE(EXTRACT(YEAR FROM CURRENT_DATE()) , EXTRACT(MONTH FROM CURRENT_DATE()), 1))
                            AND threshold = ${threshold} 
                            AND billingAccountId = '${billingAccountId}'
                            ORDER BY 1,3))
                    WHERE diff > 0`;

    const options2 = {
        query: query2,
        location: DATASET_LOCATION,
    };
    
    const [rowdata] = await bigquery.query(options2);

    console.log('Average cost differential this month: ',rowdata[0].AverageSpend);

    // rowdata.forEach((row) => {        
    //     var avgspend = `${row.AverageSpend}`;
    //     console.log('Average cost differential this month: ',avgspend);
    // }); 

    // query3 calculates the percentile of the latest diff entry to determine if it should be flagged as a potential spike
    const query3 =  `SELECT 
                        budgetName,    
                        createdAt,
                        CAST(diff AS BIGNUMERIC) AS diff,
                        ROUND(PERCENTILE_CONT(diff, 0.99) OVER(),2) AS percentile99
                    FROM
                        (SELECT 
                            *,
                            costAmount - prev_costAmount as diff
                        FROM
                            (SELECT budgetName,
                                    costAmount, 
                                    createdAt,
                                    LAG(costAmount, 1) OVER (PARTITION BY budgetName ORDER BY createdAt) AS prev_costAmount
                            FROM \`${PROJECT}.${DATASET}.${TABLE}\`
                            WHERE createdAt >  DATE_SUB(TIMESTAMP(DATE(EXTRACT(YEAR FROM CURRENT_DATE()) , EXTRACT(MONTH FROM CURRENT_DATE()), 1)),INTERVAL 10 DAY)
                            AND billingAccountId = '${billingAccountId}'
                            ORDER BY 1,3 DESC))
                    WHERE diff > 0
                    ORDER BY createdAt DESC
                    LIMIT 1`;

    const options3 = {
        query: query3,
        location: DATASET_LOCATION,
    };
    
    const [diffdata] = await bigquery.query(options3);

    // Diff query only, no percentile
    // diffdata.forEach(row => console.log(`${row.budgetName}: ${row.diff}`));
    console.log('Cost Differential: ',diffdata[0].diff);
    console.log('Percentile Value: ',diffdata[0].percentile99);

    var diff = diffdata[0].diff;
    var perc = diffdata[0].percentile99;

    // If there is no cost differential between the last and current datapoints, do not post a notification to Slack
    const diffquery = `SELECT 
                            budgetName,
                            CAST(diff as BIGNUMERIC) as diff
                        FROM
                            (SELECT 
                                *,
                                costAmount - prev_costAmount as diff
                            FROM
                                (SELECT budgetName,
                                        costAmount, 
                                        createdAt,
                                        LAG(costAmount, 1) OVER (PARTITION BY budgetName ORDER BY createdAt) AS prev_costAmount
                                        FROM \`${PROJECT}.${DATASET}.${TABLE}\`
                                WHERE createdAt >  TIMESTAMP( DATE(EXTRACT(YEAR FROM CURRENT_DATE()) , EXTRACT(MONTH FROM CURRENT_DATE()), 1))
                                AND threshold = ${threshold}
                                AND billingAccountId = '${billingAccountId}'
                                ORDER BY 1,3 DESC))
                        LIMIT 1`
    
    const diffoptions = {
        query: diffquery,
        location: DATASET_LOCATION,
    };
    
    const [zerodiff] = await bigquery.query(diffoptions);
    console.log('Zero diff check value: ',zerodiff[0].diff);

    if (zerodiff[0].diff > 0) {
    // diffdata.forEach((row) => {        
        // var diff = formatter.format(`${row.diff}`);
        // var perc = formatter.format(`${row.percentile95}`);

        // console.log('Cost Differential:',diff);
        // console.log('Percentile Value: ',perc);
                
        // NOTIFICATION
        // ============
        if (diff > perc) {
            console.log('99th Percentile exceeded!');

            // const slackURL = 'https://hooks.slack.com/services/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';
            // const emoticon = threshold >= 90 ? ':fire:' : ':ghost:'; 
            // const link = `<https://console.cloud.google.com/billing/${billingAccountId}|Go to Console>`;
            // notification = `:fire: ${link} *${budgetName}* :fire: \nYour billing account has exceeded the 99th percentile spend differential within the last hour.\n>The billing account has accrued ${costAmount} in costs so far for the month.\n>The last cost differential was ${formatter.format(diff)} which was higher than the percentile threshold of ${formatter.format(perc)}`;
            
            // const slackdata = JSON.stringify({
            //     'text': notification,
            // });

            // // Post to Slack channel as per configured webhook
            // fetch(slackURL, {
            //     method: 'POST',
            //     headers: {
            //     'Content-Type': 'application/json; charset=UTF-8',
            //     },
            //     body: slackdata,
            // });
            // // Slack END

            // Google Chat Notification
            const webhookURL = 'https://chat.googleapis.com/v1/spaces/AAAAQCspC8M/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=wvIfnFUAkylKr7XSJSZyaUqTZvcHiS37aFKA4ctQrK8%3D';
    
            // Construct a card notification
            const data = JSON.stringify({
                        "cards": [
                            {
                            "header": {
                                "title": `${budgetName}`,
                                "subtitle": "ruhan@sada.com",
                                "imageUrl": "https://storage.googleapis.com/logos-ruhanbudget-alerts/SADA_logo_shorthand_rev_rgb_blk.png"
                            },
                            "sections": [
                                {
                                "widgets": [
                                    {
                                        "keyValue": {
                                        "topLabel": "<b>Run rate this month</b>",
                                        "content": `${costAmount} | Threshold: ${threshold}%`
                                        }
                                    },
                                    {
                                        "keyValue": {
                                        "topLabel": "<b>Spend Status</b>",
                                        "content": `The last cost differential was <font color=\"#ff0000\">${formatter.format(diff)}</font> <br> which was higher than the percentile threshold of <br> ${formatter.format(perc)}`
                                        }
                                    }
                                ]
                                },
                                {
                                "widgets": [
                                    {
                                        "buttons": [
                                            {
                                            "textButton": {
                                                "text": "OPEN BILLING CONSOLE",
                                                "onClick": {
                                                "openLink": {
                                                    "url": `https://console.cloud.google.com/billing/${billingAccountId}`
                                                }
                                                }
                                            }
                                            }
                                        ]
                                    }
                                ]
                                }
                            ]
                            }
                        ]
                        });
            
            // Notification END

            // Post to Google Chat as per configured webhook
            fetch(webhookURL, {
                method: 'POST',
                headers: {
                'Content-Type': 'application/json; charset=UTF-8',
                },
                body: data,
            }).then((response) => {
                console.log(response);
            });

        } else console.log('Below the percentile threshold');
            
    // }); 
    } else {
        console.log('Last differential was zero');
    }
    
    // For threshold related webhook messages, as mentioned earlier we do not want to be notified more than once
    // per threshold and billing account within a month
    console.log("Testing results");
    console.log(results.length);
    console.log(results[0].cnt);

    if (results.length > 0 && results[0].cnt > 1 ){
        return;
    }  

}