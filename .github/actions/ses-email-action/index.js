const core = require('@actions/core');
const aws = require('aws-sdk');
const fs = require('fs');

const region = core.getInput('region');
const dataFilePath = core.getInput('dataFile');
const CcAddresses = JSON.parse(core.getInput('ccAddresses'));
const ToAddresses = JSON.parse(core.getInput('toAddresses'));
const ReplyToAddresses = JSON.parse(core.getInput('replyToAddresses'));

const data = fs.readFileSync(dataFilePath, { encoding: 'utf-8' });

// Set the region
aws.config.update({ region });

// Create sendEmail params
const params = {
    Destination: { /* required */
        CcAddresses,
        ToAddresses,
    },
    Message: { /* required */
        Body: { /* required */
            Html: {
                Charset: "UTF-8",
                Data: `This is the first part of the email.\n ${data}`
            },
            // Text: {
            //     Charset: "UTF-8",
            //     Data: "TEXT_FORMAT_BODY"
            // }
        },
        Subject: {
            Charset: 'UTF-8',
            Data: 'Test email'
        }
    },
    Source: 'dustin@dolthub.com', /* required */
    ReplyToAddresses,
};

// Create the promise and SES service object
const sendPromise = new aws.SES({apiVersion: '2010-12-01'}).sendEmail(params).promise();

// Handle promise's fulfilled/rejected states
sendPromise
    .then((data) => console.log("Successfully sent email:", data.MessageId))
    .catch((err) => console.error(err, err.stack));
