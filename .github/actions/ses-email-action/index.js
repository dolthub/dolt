const core = require('@actions/core');
const aws = require('aws-sdk');
const fs = require('fs');

const region = core.getInput('region');
const version = core.getInput('version');
const Template = core.getInput('template');
const dataFilePath = core.getInput('dataFile');
const CcAddresses = JSON.parse(core.getInput('ccAddresses'));
const ToAddresses = JSON.parse(core.getInput('toAddresses'));
const ReplyToAddresses = JSON.parse(core.getInput('replyToAddresses'));
const workflowURL = core.getInput('workflowURL');

const data = dataFilePath ? fs.readFileSync(dataFilePath, { encoding: 'utf-8' }) : "";

const templated = {
    version,
    results: data,
    workflowURL,
};

// Set the region
aws.config.update({ region });

// Create sendEmail params
const params = {
    Destination: { /* required */
        CcAddresses,
        ToAddresses,
    },
    Source: 'github-actions-bot@corp.ld-corp.com', /* required */
    Template,
    TemplateData: JSON.stringify(templated),
    ReplyToAddresses,
};

// Create the promise and SES service object
// const sendPromise = new aws.SES({apiVersion: '2010-12-01'}).sendEmail(params).promise();
const sendPromise = new aws.SES({apiVersion: '2010-12-01'}).sendTemplatedEmail(params).promise();

// Handle promise's fulfilled/rejected states
sendPromise
    .then((data) => console.log("Successfully sent email:", data.MessageId))
    .catch((err) => console.error(err, err.stack));
